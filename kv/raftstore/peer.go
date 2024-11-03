package raftstore

import (
	"fmt"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

// NotifyStaleReq 通知请求过时，返回相应的错误。
func NotifyStaleReq(term uint64, cb *message.Callback) {
	cb.Done(ErrRespStaleCommand(term))
}

// NotifyReqRegionRemoved 通知请求区域已被移除。
func NotifyReqRegionRemoved(regionId uint64, cb *message.Callback) {
	regionNotFound := &util.ErrRegionNotFound{RegionId: regionId}
	resp := ErrResp(regionNotFound)
	cb.Done(resp)
}

// If we create the peer actively, like bootstrap/split/merge region, we should
// use this function to create the peer. The region must contain the peer info
// for this store.
// 如果我们主动创建该节点，例如引导/拆分/合并区域，我们应该使用此函数来创建节点。
// 区域必须包含该存储的节点信息。
func createPeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, region *metapb.Region) (*peer, error) {
	metaPeer := util.FindPeer(region, storeID)
	if metaPeer == nil {
		return nil, errors.Errorf("find no peer for store %d in region %v", storeID, region)
	}
	log.Infof("region %v create peer with ID %d", region, metaPeer.Id)
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

// The peer can be created from another node with raft membership changes, and we only
// know the region_id and peer_id when creating this replicated peer, the region info
// will be retrieved later after applying snapshot.
// 节点可以通过其他节点的 Raft 成员变更来创建，当创建这个复制的节点时，我们只知道 region_id 和 peer_id，
// 区域信息将在应用快照后检索。
func replicatePeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, regionID uint64, metaPeer *metapb.Peer) (*peer, error) {
	// We will remove tombstone key when apply snapshot
	// 在应用快照时，我们将移除墓碑键
	log.Infof("[region %v] replicates peer with ID %d", regionID, metaPeer.GetId())
	region := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{},
	}
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

// proposal 表示一个提案，包含索引、任期和回调。
type proposal struct {
	// index + term for unique identification
	// 索引 + 任期用于唯一识别
	index uint64
	term  uint64
	// 提案的回调
	cb *message.Callback
}

// peer 表示一个节点。
type peer struct {
	// The ticker of the peer, used to trigger
	// * raft tick
	// * raft log gc
	// * region heartbeat
	// * split check
	// 节点的计时器，用于触发
	// * Raft 滴答
	// * Raft 日志垃圾回收
	// * 区域心跳
	// * 拆分检查
	ticker *ticker
	// Instance of the Raft module
	// Raft 模块的实例
	RaftGroup *raft.RawNode
	// The peer storage for the Raft module
	// Raft 模块的节点存储
	peerStorage *PeerStorage

	// Record the meta information of the peer
	// 记录节点的元信息
	Meta     *metapb.Peer
	regionId uint64
	// Tag which is useful for printing log
	// 用于打印日志的标签
	Tag string

	// Record the callback of the proposals
	// (Used in 2B)
	// 记录提案的回调
	// (在 2B 中使用)
	proposals []*proposal

	// Index of last scheduled compacted raft log.
	// (Used in 2C)
	// 最后调度的压缩 Raft 日志的索引。
	// (在 2C 中使用)
	LastCompactedIdx uint64

	// Cache the peers information from other stores
	// when sending raft messages to other peers, it's used to get the store id of target peer
	// (Used in 3B conf change)
	// 缓存来自其他存储的节点信息
	// 在向其他节点发送 Raft 消息时，用于获取目标节点的存储 ID
	// (在 3B 配置变更中使用)
	peerCache map[uint64]*metapb.Peer
	// Record the instants of peers being added into the configuration.
	// Remove them after they are not pending any more.
	// (Used in 3B conf change)
	// 记录节点被添加到配置中的时刻。
	// 在它们不再待处理时将其移除。
	// (在 3B 配置变更中使用)
	PeersStartPendingTime map[uint64]time.Time
	// Mark the peer as stopped, set when peer is destroyed
	// (Used in 3B conf change)
	// 标记节点为停止状态，在节点被销毁时设置。
	// (在 3B 配置变更中使用)
	stopped bool

	// An inaccurate difference in region size since last reset.
	// split checker is triggered when it exceeds the threshold, it makes split checker not scan the data very often
	// (Used in 3B split)
	// 自上次重置以来区域大小的一个不准确差异。
	// 当差异超过阈值时触发分裂检查器，这使得分裂检查器不会频繁扫描数据。
	// (在 3B 拆分中使用)
	SizeDiffHint uint64
	// Approximate size of the region.
	// It's updated everytime the split checker scan the data
	// (Used in 3B split)
	// 区域的近似大小。
	// 每次分裂检查器扫描数据时都会更新。
	// (在 3B 拆分中使用)
	ApproximateSize *uint64
}

// NewPeer 创建一个新的节点实例。
func NewPeer(storeId uint64, cfg *config.Config, engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task,
	meta *metapb.Peer) (*peer, error) {
	if meta.GetId() == util.InvalidID {
		return nil, fmt.Errorf("invalid peer id")
	}
	tag := fmt.Sprintf("[region %v] %v", region.GetId(), meta.GetId())

	ps, err := NewPeerStorage(engines, region, regionSched, tag)
	if err != nil {
		return nil, err
	}

	appliedIndex := ps.AppliedIndex()

	raftCfg := &raft.Config{
		ID:            meta.GetId(),
		ElectionTick:  cfg.RaftElectionTimeoutTicks,
		HeartbeatTick: cfg.RaftHeartbeatTicks,
		Applied:       appliedIndex,
		Storage:       ps,
	}

	raftGroup, err := raft.NewRawNode(raftCfg)
	if err != nil {
		return nil, err
	}
	p := &peer{
		Meta:                  meta,
		regionId:              region.GetId(),
		RaftGroup:             raftGroup,
		peerStorage:           ps,
		peerCache:             make(map[uint64]*metapb.Peer),
		PeersStartPendingTime: make(map[uint64]time.Time),
		Tag:                   tag,
		ticker:                newTicker(region.GetId(), cfg),
	}

	// If this region has only one peer and I am the one, campaign directly.
	// 如果该区域仅有一个节点且我就是那一个，则直接发起竞选。
	if len(region.GetPeers()) == 1 && region.GetPeers()[0].GetStoreId() == storeId {
		err = p.RaftGroup.Campaign()
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

// insertPeerCache 将一个节点插入到缓存中。
func (p *peer) insertPeerCache(peer *metapb.Peer) {
	p.peerCache[peer.GetId()] = peer
}

// removePeerCache 从缓存中移除一个节点。
func (p *peer) removePeerCache(peerID uint64) {
	delete(p.peerCache, peerID)
}

// getPeerFromCache 从缓存中获取一个节点。
func (p *peer) getPeerFromCache(peerID uint64) *metapb.Peer {
	if peer, ok := p.peerCache[peerID]; ok {
		return peer
	}
	for _, peer := range p.peerStorage.Region().GetPeers() {
		if peer.GetId() == peerID {
			p.insertPeerCache(peer)
			return peer
		}
	}
	return nil
}

// nextProposalIndex 返回下一个提议索引。
func (p *peer) nextProposalIndex() uint64 {
	return p.RaftGroup.Raft.RaftLog.LastIndex() + 1
}

// Tries to destroy itself. Returns a job (if needed) to do more cleaning tasks.
// 尝试自我销毁。如果需要返回一个任务以执行更多清理任务。
func (p *peer) MaybeDestroy() bool {
	if p.stopped {
		log.Infof("%v is being destroyed, skip", p.Tag)
		return false
	}
	return true
}

// Does the real destroy worker.Task which includes:
// 1. Set the region to tombstone;
// 2. Clear data;
// 3. Notify all pending requests.
// 执行实际的销毁工作任务。其包括：
// 1. 将区域设置为墓碑（tombstone）；
// 2. 清除数据；
// 3. 通知所有挂起的请求。
func (p *peer) Destroy(engine *engine_util.Engines, keepData bool) error {
	start := time.Now()
	region := p.Region()
	log.Infof("%v begin to destroy", p.Tag)

	// Set Tombstone state explicitly
	// 明确设置墓碑状态
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	if err := p.peerStorage.clearMeta(kvWB, raftWB); err != nil {
		return err
	}
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Tombstone)
	// write kv badgerDB first in case of restart happen between two write
	// 在两次写入之间发生重启的情况下，首先写入 kv badgerDB
	if err := kvWB.WriteToDB(engine.Kv); err != nil {
		return err
	}
	if err := raftWB.WriteToDB(engine.Raft); err != nil {
		return err
	}

	if p.peerStorage.isInitialized() && !keepData {
		// If we meet panic when deleting data and raft log, the dirty data
		// will be cleared by a newer snapshot applying or restart.
		// 如果在删除数据和 Raft 日志时遇到 panic，脏数据将通过应用更新的快照或重启来清除。
		p.peerStorage.ClearData()
	}

	for _, proposal := range p.proposals {
		NotifyReqRegionRemoved(region.Id, proposal.cb)
	}
	p.proposals = nil

	log.Infof("%v destroy itself, takes %v", p.Tag, time.Now().Sub(start))
	return nil
}

// isInitialized 检测该节点的存储是否已初始化。
func (p *peer) isInitialized() bool {
	return p.peerStorage.isInitialized()
}

// storeID 返回该节点对应的存储 ID。
func (p *peer) storeID() uint64 {
	return p.Meta.StoreId
}

// Region 返回当前节点的区域信息。
func (p *peer) Region() *metapb.Region {
	return p.peerStorage.Region()
}

// Set the region of a peer.
//
// This will update the region of the peer, caller must ensure the region
// has been preserved in a durable device.
// 设置一个节点的区域。这将更新节点的区域，调用者必须确保区域数据已经保存在一个持久化设备中。
func (p *peer) SetRegion(region *metapb.Region) {
	p.peerStorage.SetRegion(region)
}

// PeerId 返回该节点的 ID。
func (p *peer) PeerId() uint64 {
	return p.Meta.GetId()
}

// LeaderId 返回当前领导者的 ID。
func (p *peer) LeaderId() uint64 {
	return p.RaftGroup.Raft.Lead
}

// IsLeader 检测该节点是否为领导者。
func (p *peer) IsLeader() bool {
	return p.RaftGroup.Raft.State == raft.StateLeader
}

// Send 发送 Raft 消息到目标节点。
func (p *peer) Send(trans Transport, msgs []eraftpb.Message) {
	for _, msg := range msgs {
		err := p.sendRaftMessage(msg, trans)
		if err != nil {
			log.Debugf("%v send message err: %v", p.Tag, err)
		}
	}
}

// Collects all pending peers and update `peers_start_pending_time`.
// 收集所有待处理的节点，并更新 `peers_start_pending_time`。
func (p *peer) CollectPendingPeers() []*metapb.Peer {
	pendingPeers := make([]*metapb.Peer, 0, len(p.Region().GetPeers()))
	truncatedIdx := p.peerStorage.truncatedIndex()
	for id, progress := range p.RaftGroup.GetProgress() {
		if id == p.Meta.GetId() {
			continue
		}
		if progress.Match < truncatedIdx {
			if peer := p.getPeerFromCache(id); peer != nil {
				pendingPeers = append(pendingPeers, peer)
				if _, ok := p.PeersStartPendingTime[id]; !ok {
					now := time.Now()
					p.PeersStartPendingTime[id] = now
					log.Debugf("%v peer %v start pending at %v", p.Tag, id, now)
				}
			}
		}
	}
	return pendingPeers
}

// clearPeersStartPendingTime 清除待处理时间的记录。
func (p *peer) clearPeersStartPendingTime() {
	for id := range p.PeersStartPendingTime {
		delete(p.PeersStartPendingTime, id)
	}
}

// Returns `true` if any new peer catches up with the leader in replicating logs.
// And updates `PeersStartPendingTime` if needed.
// 如果任何新的节点在日志复制上追赶上领导者，则返回 `true`。
// 如有必要，则更新 `PeersStartPendingTime`。
func (p *peer) AnyNewPeerCatchUp(peerId uint64) bool {
	if len(p.PeersStartPendingTime) == 0 {
		return false
	}
	if !p.IsLeader() {
		p.clearPeersStartPendingTime()
		return false
	}
	if startPendingTime, ok := p.PeersStartPendingTime[peerId]; ok {
		truncatedIdx := p.peerStorage.truncatedIndex()
		progress, ok := p.RaftGroup.Raft.Prs[peerId]
		if ok {
			if progress.Match >= truncatedIdx {
				delete(p.PeersStartPendingTime, peerId)
				elapsed := time.Since(startPendingTime)
				log.Debugf("%v peer %v has caught up logs, elapsed: %v", p.Tag, peerId, elapsed)
				return true
			}
		}
	}
	return false
}

// MaybeCampaign 尝试竞选领导者。
func (p *peer) MaybeCampaign(parentIsLeader bool) bool {
	// The peer campaigned when it was created, no need to do it again.
	// 节点在创建时已经进行了竞选，不需要再进行一次。
	if len(p.Region().GetPeers()) <= 1 || !parentIsLeader {
		return false
	}

	// If last peer is the leader of the region before split, it's intuitional for
	// it to become the leader of new split region.
	// 如果最后一个节点在分裂之前是该区域的领导者，那么它成为新分裂区域的领导者是直观的。
	p.RaftGroup.Campaign()
	return true
}

// Term 返回当前节点的任期。
func (p *peer) Term() uint64 {
	return p.RaftGroup.Raft.Term
}

// HeartbeatScheduler 将心跳任务添加到调度通道。
func (p *peer) HeartbeatScheduler(ch chan<- worker.Task) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(p.Region(), clonedRegion)
	if err != nil {
		return
	}
	ch <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            p.Meta,
		PendingPeers:    p.CollectPendingPeers(),
		ApproximateSize: p.ApproximateSize,
	}
}

// sendRaftMessage 发送 Raft 消息到目标节点。
func (p *peer) sendRaftMessage(msg eraftpb.Message, trans Transport) error {
	sendMsg := new(rspb.RaftMessage)
	sendMsg.RegionId = p.regionId
	// set current epoch
	// 设置当前时期
	sendMsg.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: p.Region().RegionEpoch.ConfVer,
		Version: p.Region().RegionEpoch.Version,
	}

	fromPeer := *p.Meta
	toPeer := p.getPeerFromCache(msg.To)
	if toPeer == nil {
		return fmt.Errorf("failed to lookup recipient peer %v in region %v", msg.To, p.regionId)
	}
	log.Debugf("%v, send raft msg %v from %v to %v", p.Tag, msg.MsgType, fromPeer, toPeer)

	sendMsg.FromPeer = &fromPeer
	sendMsg.ToPeer = toPeer

	// There could be two cases:
	// 1. Target peer already exists but has not established communication with leader yet
	// 2. Target peer is added newly due to member change or region split, but it's not
	//    created yet
	// For both cases the region start key and end key are attached in RequestVote and
	// Heartbeat message for the store of that peer to check whether to create a new peer
	// when receiving these messages, or just to wait for a pending region split to perform
	// later.
	// 有两种情况可能发生：
	// 1. 目标节点已经存在，但尚未与领导者建立通信
	// 2. 由于成员变更或区域分裂，目标节点是新添加的，但尚未创建
	// 对于这两种情况，区域的起始键和结束键会附加在 RequestVote 和 Heartbeat 消息中，
	// 供该节点的存储检查在接收到这些消息时是否创建新节点，或者只是等待一个待处理的区域分裂以便稍后执行。
	if p.peerStorage.isInitialized() && util.IsInitialMsg(&msg) {
		sendMsg.StartKey = append([]byte{}, p.Region().StartKey...)
		sendMsg.EndKey = append([]byte{}, p.Region().EndKey...)
	}
	sendMsg.Message = &msg
	return trans.Send(sendMsg)
}
