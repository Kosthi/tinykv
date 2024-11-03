package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// HandleRaftReady 处理 Raft 状态准备就绪的信号。
// 如果 Raft 节点已经停止，则此方法不会执行任何操作。
// 它会检查 Raft 状态，持久化当前状态，发送消息，并应用已提交的日志条目。
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		log.Warnf("%s is stopped, skipping HandleRaftReady", d.Tag)
		return
	}

	// Your Code Here (2B).
	rawNode := d.RaftGroup
	// 是否有待处理的 ready
	if rawNode.HasReady() {
		// 获取当前状态
		rd := rawNode.Ready()

		// 持久化当前状态
		if _, err := d.peerStorage.SaveReadyState(&rd); err != nil {
			log.Panicf("%s failed to save ready state: %v", d.Tag, err)
		}

		// 发送消息
		d.Send(d.ctx.trans, rd.Messages)

		// 应用已经提交的日志条目
		if err := d.applyCommittedEntries(&rd.CommittedEntries); err != nil {
			log.Panicf("%s failed to apply committed entries: %v", d.Tag, err)
		}

		// 推进节点状态
		rawNode.Advance(rd)
		log.Infof("%s advances state after processing ready", d.Tag)
	}
}

// dropStaleProposal 处理过时提案将其丢弃。
func (d *peerMsgHandler) dropStaleProposal(entry *eraftpb.Entry) {
	// 如果不是领导者，不需要处理提议，直接把提议清空
	if !d.IsLeader() {
		d.proposals = d.proposals[:0]
		return
	}

	i := 0
	length := len(d.proposals)

	// 遍历提议列表
	for ; i < length; i++ {
		proposal := d.proposals[i]

		// 未应用的过期日志，需要应用，不能丢弃，直接跳出循环
		if entry.Index < proposal.index {
			break
		}

		// 由于领导者更改导致一些日志未提交，并且已经被新领导者的日志强制覆盖。
		// 当前提议过期了，调用回调函数告诉客户端，返回 ErrStaleCommand 错误
		if entry.Index > proposal.index {
			log.Warnf("%s Stale proposal detected: Index=%d, Term=%d; notifying callback", d.Tag, proposal.index, proposal.term)
			NotifyStaleReq(proposal.term, proposal.cb)
			continue
		}

		// 到这里索引相同，再判断任期
		// 当前提议过期了，调用回调函数告诉客户端，返回 ErrStaleCommand 错误
		if entry.Term > proposal.term {
			log.Warnf("%s Stale proposal detected: Index=%d, Term=%d; notifying callback", d.Tag, proposal.index, proposal.term)
			NotifyStaleReq(proposal.term, proposal.cb)
			continue
		}

		// 到这里，索引相同且 entry.Term <= proposal.term，是不能丢弃的日志条目，可以直接跳出循环了
		break
	}

	// 如果所有提案都被认为是过期的，则清空提案列表
	if i == length {
		log.Debugf("%s All proposals are stale; resetting the proposals", d.Tag)
		d.proposals = d.proposals[:0]
	} else {
		// 否则，保留有效提议，从第 i 个开始
		log.Debugf("%s Keeping valid proposals, starting from index %d", d.Tag, i)
		d.proposals = d.proposals[i:]
	}
}

// applyCommittedEntries 应用已提交的日志条目
func (d *peerMsgHandler) applyCommittedEntries(entries *[]eraftpb.Entry) error {
	kvWB := new(engine_util.WriteBatch)
	for _, entry := range *entries {
		if err := d.applyEntry(&entry, kvWB); err != nil {
			return err
		}
	}
	return nil
}

// 单个日志条目的应用
func (d *peerMsgHandler) applyEntry(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) error {
	kvWB.Reset()
	if entry.Data != nil {
		if err := d.applyEntryData(entry, kvWB); err != nil {
			return err
		}
	}
	// 更新已应用的索引状态
	d.peerStorage.applyState.AppliedIndex = entry.Index
	// 写入 kv batch
	return d.writeApplyState(kvWB)
}

// processEntryData 处理日志条目的数据
func (d *peerMsgHandler) applyEntryData(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) error {
	msg := raft_cmdpb.RaftCmdRequest{}
	if err := msg.Unmarshal(entry.Data); err != nil {
		log.Errorf("%s failed to unmarshal entry data: %v", d.Tag, err)
		return err
	}

	req := msg.Requests[0]
	d.dropStaleProposal(entry)

	switch req.CmdType {
	case raft_cmdpb.CmdType_Put:
		return d.applyPut(req.Put, entry, kvWB)
	case raft_cmdpb.CmdType_Get:
		return d.applyGet(req.Get, entry)
	case raft_cmdpb.CmdType_Delete:
		return d.applyDelete(req.Delete, entry, kvWB)
	case raft_cmdpb.CmdType_Snap:
		return d.applySnap(entry)
	default:
		log.Warnf("%s unknown command type: %v", d.Tag, req.CmdType)
		return nil
	}
}

// applyPut 应用 Put 请求
func (d *peerMsgHandler) applyPut(req *raft_cmdpb.PutRequest, entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) error {
	kvWB.SetCF(req.Cf, req.Key, req.Value)
	log.Infof("%s applied Put: key=%s, value=%s", d.Tag, req.Key, req.Value)

	return d.respondToProposal([]*raft_cmdpb.Response{{
		CmdType: raft_cmdpb.CmdType_Put,
		Put:     &raft_cmdpb.PutResponse{},
	}}, entry)
}

// applyGet 应用 Get 请求
func (d *peerMsgHandler) applyGet(req *raft_cmdpb.GetRequest, entry *eraftpb.Entry) error {
	val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Cf, req.Key)
	if err != nil {
		log.Errorf("%s failed to get value for key=%s: %v", d.Tag, req.Key, err)
		return err
	}

	log.Infof("%s applied Get: key=%s, value=%s", d.Tag, req.Key, val)

	return d.respondToProposal([]*raft_cmdpb.Response{{
		CmdType: raft_cmdpb.CmdType_Get,
		Get: &raft_cmdpb.GetResponse{
			Value: val,
		},
	}}, entry)
}

// applyDelete 应用 Delete 请求
func (d *peerMsgHandler) applyDelete(req *raft_cmdpb.DeleteRequest, entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) error {
	kvWB.DeleteCF(req.Cf, req.Key)
	log.Infof("%s applied Delete: key=%s", d.Tag, req.Key)

	return d.respondToProposal([]*raft_cmdpb.Response{{
		CmdType: raft_cmdpb.CmdType_Delete,
		Delete:  &raft_cmdpb.DeleteResponse{},
	}}, entry)
}

// applySnap 应用 Snap 请求
func (d *peerMsgHandler) applySnap(entry *eraftpb.Entry) error {
	log.Infof("%s applied Snap", d.Tag)

	return d.respondToProposal([]*raft_cmdpb.Response{{
		CmdType: raft_cmdpb.CmdType_Snap,
		Snap: &raft_cmdpb.SnapResponse{
			Region: d.Region(),
		},
	}}, entry)
}

// respondToProposal 向提议回调返回响应
func (d *peerMsgHandler) respondToProposal(resp []*raft_cmdpb.Response, entry *eraftpb.Entry) error {
	if len(d.proposals) == 0 {
		return nil
	}

	proposal := d.proposals[0]

	// 当成为新领导者的节点，因为没有存储原来客户端的提议，过期的日志条目可能还没应用，但是已经被旧的领导者响应了，这里只需要应用即可
	// 既不需要响应也找不到原来的提议响应了
	if entry.Index < proposal.index {

	} else if proposal.term != entry.Term {
		NotifyStaleReq(entry.Term, proposal.cb)
		d.proposals = d.proposals[1:]
	} else {
		cmdResp := &raft_cmdpb.RaftCmdResponse{
			Header:    &raft_cmdpb.RaftResponseHeader{},
			Responses: resp,
		}
		// 调用回调函数发送响应
		if resp[0].CmdType == raft_cmdpb.CmdType_Snap {
			// 显式开启一个事务
			proposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
		proposal.cb.Done(cmdResp)
		d.proposals = d.proposals[1:]
	}

	return nil
}

// writeApplyState 写入应用状态
func (d *peerMsgHandler) writeApplyState(kvWB *engine_util.WriteBatch) error {
	if err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
		log.Errorf("%s failed to update apply state: %v", d.Tag, err)
		return err
	}
	// 写入数据库
	return d.peerStorage.Engines.WriteKV(kvWB)
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	// 检查 store_id，确保消息被分发到正确的位置。
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	// 检查该存储是否有合适的 peer 来处理请求。
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	// peer_id 必须与 peer 的一致。
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	// 检查选举周期是否过时。
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		// 附加可能从当前区域分裂出来的区域。但如果该区域不是从当前区域分裂出来的，这并不重要。
		// 如果 TiKV 驱动接收到的区域元数据比驱动中缓存的元数据更新，则会更新元数据。
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

// proposeRaftCommand 处理来自客户端的 RaftCmdRequest，将其转换为 Raft 提议并提交到 Raft 集群中。
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// 在提交之前进行预处理，如检查请求的合法性等
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		log.Errorf("%s proposal pre-check failed: %v", d.Tag, err)
		cb.Done(ErrResp(err))
		return
	}

	// Your Code Here (2B).
	for _, req := range msg.Requests {
		var key []byte
		// 根据请求类型提取相应的 key
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			key = req.Put.Key
		case raft_cmdpb.CmdType_Get:
			key = req.Get.Key
		case raft_cmdpb.CmdType_Delete:
			key = req.Delete.Key
		}

		// 检查该 key 是否在当前 Region 中有效
		err = util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			log.Warnf("%s key %s is not valid in current region: %v", d.Tag, key, err)
			// 如果 key 无效，调用回调并返回错误响应
			cb.Done(ErrResp(err))
			continue
		}

		// 将请求序列化为字节数组，以便向 Raft 集群提议
		data, err := msg.Marshal()
		if err != nil {
			log.Errorf("%s failed to marshal RaftCmdRequest: %v", d.Tag, err)
			// 序列化失败，调用回调并返回错误响应
			cb.Done(ErrResp(err))
			continue
		}

		// 提议数据到 Raft 集群，触发 Raft 的日志复制过程
		err = d.RaftGroup.Propose(data)
		if err != nil {
			log.Errorf("%s proposal failed: %v", d.Tag, err)
			// 提议失败，调用回调并返回错误响应
			cb.Done(ErrResp(err))
			continue
		}

		// 创建一个新的提议，将其添加到提议队列中
		d.proposals = append(d.proposals, &proposal{
			index: d.nextProposalIndex() - 1, // 当前提议的索引
			term:  d.Term(),                  // 当前任期
			cb:    cb,                        // 当前回调函数
		})

		log.Infof("%s successfully proposed command for key: %s", d.Tag, key)
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message from %d to %d", regionID, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
