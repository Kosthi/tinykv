package util

import (
	"bytes"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap/errors"
)

const RaftInvalidIndex uint64 = 0
const InvalidID uint64 = 0

// `is_initial_msg` checks whether the `msg` can be used to initialize a new peer or not.
// There could be two cases:
//  1. Target peer already exists but has not established communication with leader yet
//  2. Target peer is added newly due to member change or region split, but it's not
//     created yet
//
// For both cases the region start key and end key are attached in RequestVote and
// Heartbeat message for the store of that peer to check whether to create a new peer
// when receiving these messages, or just to wait for a pending region split to perform
// later.
// IsInitialMsg 检查消息是否可以用来初始化一个新的节点。
// 可能存在两种情况：
//  1. 目标节点已存在但尚未与领导者建立通信。
//  2. 目标节点由于成员变化或区域拆分而新添加，但尚未创建。
//
// 对于这两种情况，区域的开始键和结束键会附加在 RequestVote 和 Heartbeat 消息中，
// 使得该节点的存储可以在接收到这些消息时检查是否需要创建新的节点，
// 或者只是等待待处理的区域拆分操作。
func IsInitialMsg(msg *eraftpb.Message) bool {
	return msg.MsgType == eraftpb.MessageType_MsgRequestVote ||
		// the peer has not been known to this leader, it may exist or not.
		// 节点尚未被该领导者知晓，可能存在或不存在。
		(msg.MsgType == eraftpb.MessageType_MsgHeartbeat && msg.Commit == RaftInvalidIndex)
}

// Check if key in region range [`start_key`, `end_key`).
// CheckKeyInRegion 检查给定的键是否在指定区域范围内 [`start_key`, `end_key`) 中。
func CheckKeyInRegion(key []byte, region *metapb.Region) error {
	if bytes.Compare(key, region.StartKey) >= 0 && (len(region.EndKey) == 0 || bytes.Compare(key, region.EndKey) < 0) {
		return nil
	} else {
		return &ErrKeyNotInRegion{Key: key, Region: region}
	}
}

// Check if key in region range (`start_key`, `end_key`).
// CheckKeyInRegionExclusive 检查给定的键是否在指定区域范围内 (`start_key`, `end_key`) 中。
func CheckKeyInRegionExclusive(key []byte, region *metapb.Region) error {
	if bytes.Compare(region.StartKey, key) < 0 && (len(region.EndKey) == 0 || bytes.Compare(key, region.EndKey) < 0) {
		return nil
	} else {
		return &ErrKeyNotInRegion{Key: key, Region: region}
	}
}

// Check if key in region range [`start_key`, `end_key`].
// CheckKeyInRegionInclusive 检查给定的键是否在指定区域范围内 [`start_key`, `end_key`] 中。
func CheckKeyInRegionInclusive(key []byte, region *metapb.Region) error {
	if bytes.Compare(key, region.StartKey) >= 0 && (len(region.EndKey) == 0 || bytes.Compare(key, region.EndKey) <= 0) {
		return nil
	} else {
		return &ErrKeyNotInRegion{Key: key, Region: region}
	}
}

// check whether epoch is staler than check_epoch.
// IsEpochStale 检查一个时期是否比检查的时期过期。
func IsEpochStale(epoch *metapb.RegionEpoch, checkEpoch *metapb.RegionEpoch) bool {
	return epoch.Version < checkEpoch.Version || epoch.ConfVer < checkEpoch.ConfVer
}

// IsVoteMessage 检查消息是否为投票消息。
func IsVoteMessage(msg *eraftpb.Message) bool {
	tp := msg.GetMsgType()
	return tp == eraftpb.MessageType_MsgRequestVote
}

// `is_first_vote_msg` checks `msg` is the first vote message or not. It's used for
// when the message is received but there is no such region in `Store::region_peers` and the
// region overlaps with others. In this case we should put `msg` into `pending_votes` instead of
// create the peer.
// IsFirstVoteMessage 检查消息是否为第一次投票消息。
// 当接收到该消息时，如果在 `Store::region_peers` 中没有找到该区域，
// 并且该区域与其他区域重叠，则应该将该消息放入 `pending_votes` 中，而不是创建该节点。
func IsFirstVoteMessage(msg *eraftpb.Message) bool {
	return IsVoteMessage(msg) && msg.Term == meta.RaftInitLogTerm+1
}

// CheckRegionEpoch 检查请求的区域时期是否与当前区域匹配。
func CheckRegionEpoch(req *raft_cmdpb.RaftCmdRequest, region *metapb.Region, includeRegion bool) error {
	checkVer, checkConfVer := false, false
	if req.AdminRequest == nil {
		checkVer = true
	} else {
		switch req.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog, raft_cmdpb.AdminCmdType_InvalidAdmin:
		case raft_cmdpb.AdminCmdType_ChangePeer:
			checkConfVer = true
		case raft_cmdpb.AdminCmdType_Split, raft_cmdpb.AdminCmdType_TransferLeader:
			checkVer = true
			checkConfVer = true
		}
	}

	if !checkVer && !checkConfVer {
		return nil
	}

	if req.Header == nil {
		return fmt.Errorf("missing header!")
	}

	if req.Header.RegionEpoch == nil {
		return fmt.Errorf("missing epoch!")
	}

	fromEpoch := req.Header.RegionEpoch
	currentEpoch := region.RegionEpoch

	// We must check epochs strictly to avoid key not in region error.
	//
	// A 3 nodes TiKV cluster with merge enabled, after commit merge, TiKV A
	// tells TiDB with an epoch not match error contains the latest target Region
	// info, TiDB updates its region cache and sends requests to TiKV B,
	// and TiKV B has not applied commit merge yet, since the region epoch in
	// request is higher than TiKV B, the request must be denied due to epoch
	// not match, so it does not read on a stale snapshot, thus avoid the
	// KeyNotInRegion error.
	// 我们必须严格检查 epoch 以避免键不在区域的错误。
	// 例如，在一个 3 节点的 TiKV 集群中，如果在启用合并的情况下提交合并，
	// TiKV A 将会用包含最新目标区域信息的不匹配 epoch 错误通知 TiDB，
	// TiDB 会更新其区域缓存并向 TiKV B 发送请求，
	// 而 TiKV B 尚未应用提交的合并，
	// 由于请求中的区域 epoch 高于 TiKV B 的当前 epoch，
	// 此时请求必须因 epoch 不匹配而被拒绝，避免在过期快照上读取，
	// 从而避免出现 KeyNotInRegion 错误。
	if (checkConfVer && fromEpoch.ConfVer != currentEpoch.ConfVer) ||
		(checkVer && fromEpoch.Version != currentEpoch.Version) {
		log.Debugf("epoch not match, region id %v, from epoch %v, current epoch %v",
			region.Id, fromEpoch, currentEpoch)

		regions := []*metapb.Region{}
		if includeRegion {
			regions = []*metapb.Region{region}
		}
		return &ErrEpochNotMatch{Message: fmt.Sprintf("current epoch of region %v is %v, but you sent %v",
			region.Id, currentEpoch, fromEpoch), Regions: regions}
	}

	return nil
}

// FindPeer 查找指定 storeID 的节点在区域中的实例。
func FindPeer(region *metapb.Region, storeID uint64) *metapb.Peer {
	for _, peer := range region.Peers {
		if peer.StoreId == storeID {
			return peer
		}
	}
	return nil
}

// RemovePeer 从区域的节点列表中移除指定 storeID 的节点，并返回移除的节点。
func RemovePeer(region *metapb.Region, storeID uint64) *metapb.Peer {
	for i, peer := range region.Peers {
		if peer.StoreId == storeID {
			region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
			return peer
		}
	}
	return nil
}

// ConfStateFromRegion 根据区域生成配置状态。
func ConfStateFromRegion(region *metapb.Region) (confState eraftpb.ConfState) {
	for _, p := range region.Peers {
		confState.Nodes = append(confState.Nodes, p.GetId())
	}
	return
}

// CheckStoreID 检查请求的节点是否与指定的存储 ID 匹配。
func CheckStoreID(req *raft_cmdpb.RaftCmdRequest, storeID uint64) error {
	peer := req.Header.Peer
	if peer.StoreId == storeID {
		return nil
	}
	return errors.Errorf("store not match %d %d", peer.StoreId, storeID)
}

// CheckTerm 检查请求的任期与当前任期的关系。
func CheckTerm(req *raft_cmdpb.RaftCmdRequest, term uint64) error {
	header := req.Header
	if header.Term == 0 || term <= header.Term+1 {
		return nil
	}
	// If header's term is 2 verions behind current term,
	// leadership may have been changed away.
	// 如果请求头的任期比当前任期落后 2 个版本，领导权可能已经发生变化。
	return &ErrStaleCommand{}
}

// CheckPeerID 检查请求中的节点 ID 是否与给定的节点 ID 匹配。
func CheckPeerID(req *raft_cmdpb.RaftCmdRequest, peerID uint64) error {
	peer := req.Header.Peer
	if peer.Id == peerID {
		return nil
	}
	return errors.Errorf("mismatch peer id %d != %d", peer.Id, peerID)
}

// CloneMsg 克隆一个 proto 消息。
func CloneMsg(origin, cloned proto.Message) error {
	data, err := proto.Marshal(origin)
	if err != nil {
		return err
	}
	return proto.Unmarshal(data, cloned)
}

// SafeCopy 安全地复制字节切片。
func SafeCopy(b []byte) []byte {
	return append([]byte{}, b...)
}

// PeerEqual 检查两个 peer 是否相等。
func PeerEqual(l, r *metapb.Peer) bool {
	return l.Id == r.Id && l.StoreId == r.StoreId
}

// RegionEqual 检查两个区域是否相等。
func RegionEqual(l, r *metapb.Region) bool {
	if l == nil || r == nil {
		return false
	}
	return l.Id == r.Id && l.RegionEpoch.Version == r.RegionEpoch.Version && l.RegionEpoch.ConfVer == r.RegionEpoch.ConfVer
}
