package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
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

type ApplySnapResult struct {
	// PrevRegion is the region before snapshot applied
	// PrevRegion 是快照应用之前的区域。
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	// current region information of the peer
	// 节点的当前区域信息
	region *metapb.Region
	// current raft state of the peer
	// 节点的当前 Raft 状态
	raftState *rspb.RaftLocalState
	// current apply state of the peer
	// 节点的当前应用状态
	applyState *rspb.RaftApplyState

	// current snapshot state
	// 当前快照状态
	snapState snap.SnapState
	// regionSched used to schedule task to region worker
	// regionSched 用于将任务调度到区域工作者
	regionSched chan<- worker.Task
	// generate snapshot tried count
	// 生成快照尝试计数
	snapTriedCnt int
	// Engine include two badger instance: Raft and Kv
	// 引擎包含两个 Badger 实例：Raft 和 Kv
	Engines *engine_util.Engines
	// Tag used for logging
	// 用于日志记录的标签
	Tag string
}

// NewPeerStorage get the persist raftState from engines and return a peer storage
// NewPeerStorage 从引擎中获取持久化的 raftState 并返回一个节点存储。
func NewPeerStorage(engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task, tag string) (*PeerStorage, error) {
	log.Debugf("%s creating storage for %s", tag, region.String())
	raftState, err := meta.InitRaftLocalState(engines.Raft, region)
	if err != nil {
		return nil, err
	}
	applyState, err := meta.InitApplyState(engines.Kv, region)
	if err != nil {
		return nil, err
	}
	if raftState.LastIndex < applyState.AppliedIndex {
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.LastIndex, applyState.AppliedIndex))
	}
	return &PeerStorage{
		Engines:     engines,
		region:      region,
		Tag:         tag,
		raftState:   raftState,
		applyState:  applyState,
		regionSched: regionSched,
	}, nil
}

func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raft.IsEmptyHardState(*raftState.HardState) {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	if err := ps.checkRange(low, high); err != nil || low == high {
		log.Errorf("%s Error: invalid range - low: %d, high: %d, err: %v", ps.Tag, low, high, err)
		return nil, err
	}
	buf := make([]eraftpb.Entry, 0, high-low)
	nextIndex := low
	txn := ps.Engines.Raft.NewTransaction(false)
	defer txn.Discard()
	startKey := meta.RaftLogKey(ps.region.Id, low)
	endKey := meta.RaftLogKey(ps.region.Id, high)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		var entry eraftpb.Entry
		if err = entry.Unmarshal(val); err != nil {
			return nil, err
		}
		// May meet gap or has been compacted.
		// 可能遇到间隙或已被压缩。
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	// 如果获取到正确数量的条目，则返回。
	if len(buf) == int(high-low) {
		return buf, nil
	}
	// Here means we don't fetch enough entries.
	// 这里表示我们没有获取到足够的条目。
	log.Debugf("%s Not enough entries fetched. Expected: %d, Got: %d", ps.Tag, high-low, len(buf))
	return nil, raft.ErrUnavailable
}

func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	if err := ps.checkRange(idx, idx+1); err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.raftState.LastTerm || idx == ps.raftState.LastIndex {
		return ps.raftState.LastTerm, nil
	}
	var entry eraftpb.Entry
	if err := engine_util.GetMeta(ps.Engines.Raft, meta.RaftLogKey(ps.region.Id, idx), &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
}

func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	var snapshot eraftpb.Snapshot
	if ps.snapState.StateType == snap.SnapState_Generating {
		select {
		case s := <-ps.snapState.Receiver:
			if s != nil {
				snapshot = *s
			}
		default:
			return snapshot, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = snap.SnapState_Relax
		if snapshot.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snapshot) {
				return snapshot, nil
			}
		} else {
			log.Warnf("%s failed to try generating snapshot, times: %d", ps.Tag, ps.snapTriedCnt)
		}
	}

	if ps.snapTriedCnt >= 5 {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snapshot, err
	}

	log.Infof("%s requesting snapshot", ps.Tag)
	ps.snapTriedCnt++
	ch := make(chan *eraftpb.Snapshot, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Generating,
		Receiver:  ch,
	}
	// schedule snapshot generate task
	ps.regionSched <- &runner.RegionTaskGen{
		RegionId: ps.region.GetId(),
		Notifier: ch,
	}
	return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() {
		return raft.ErrCompacted
	} else if high > ps.raftState.LastIndex+1 {
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.LastIndex)
	}
	return nil
}

func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState.TruncatedState.Index
}

func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState.TruncatedState.Term
}

func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState.AppliedIndex
}

func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.Infof("%s snapshot is stale, generate again, snapIndex: %d, truncatedIndex: %d", ps.Tag, idx, ps.truncatedIndex())
		return false
	}
	var snapData rspb.RaftSnapshotData
	if err := proto.UnmarshalMerge(snap.GetData(), &snapData); err != nil {
		log.Errorf("%s failed to decode snapshot, it may be corrupted, err: %v", ps.Tag, err)
		return false
	}
	snapEpoch := snapData.GetRegion().GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.Infof("%s snapshot epoch is stale, snapEpoch: %s, latestEpoch: %s", ps.Tag, snapEpoch, latestEpoch)
		return false
	}
	return true
}

func (ps *PeerStorage) clearMeta(kvWB, raftWB *engine_util.WriteBatch) error {
	return ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, ps.raftState.LastIndex)
}

// Delete all data that is not covered by `new_region`.
func (ps *PeerStorage) clearExtraData(newRegion *metapb.Region) {
	oldStartKey, oldEndKey := ps.region.GetStartKey(), ps.region.GetEndKey()
	newStartKey, newEndKey := newRegion.GetStartKey(), newRegion.GetEndKey()
	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		ps.clearRange(newRegion.Id, oldStartKey, newStartKey)
	}
	if bytes.Compare(newEndKey, oldEndKey) < 0 || (len(oldEndKey) == 0 && len(newEndKey) != 0) {
		ps.clearRange(newRegion.Id, newEndKey, oldEndKey)
	}
}

// ClearMeta delete stale metadata like raftState, applyState, regionState and raft log entries
func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()
	kvWB.DeleteMeta(meta.RegionStateKey(regionID))
	kvWB.DeleteMeta(meta.ApplyStateKey(regionID))

	firstIndex := lastIndex + 1
	beginLogKey := meta.RaftLogKey(regionID, 0)
	endLogKey := meta.RaftLogKey(regionID, firstIndex)
	err := engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := meta.RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(regionID, i))
	}
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))
	log.Infof(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	)
	return nil
}

// Append the given entries to the raft log and update ps.raftState also delete log entries that will
// never be committed
// Append 将给定条目附加到 Raft 日志并更新 ps.raftState，同时删除永远不会被提交的日志条目。
func (ps *PeerStorage) Append(entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
	// Your Code Here (2B).
	if len(entries) > 0 {
		psFirstIndex, err := ps.FirstIndex()
		if err != nil {
			return err
		}
		psLastIndex, err := ps.LastIndex()
		if err != nil {
			return err
		}

		entryFirstIndex := entries[0].Index
		entryLastIndex := entries[len(entries)-1].Index

		// 日志条目已经存在
		if entryLastIndex < psFirstIndex {
			return nil
		}
		// 截去不需要的日志条目头部
		if entryFirstIndex < psFirstIndex {
			entries = entries[psFirstIndex-entryFirstIndex:]
		}

		// 写入 batch
		for _, entry := range entries {
			err = raftWB.SetMeta(meta.RaftLogKey(ps.region.Id, entry.Index), &entry)
			if err != nil {
				return err
			}
		}

		// 截去不需要的日志条目尾部
		for i := entryLastIndex + 1; i <= psLastIndex; i++ {
			raftWB.DeleteMeta(meta.RaftLogKey(ps.region.Id, i))
		}

		// 更新节点 raft 状态
		ps.raftState.LastIndex = entries[len(entries)-1].Index
		ps.raftState.LastTerm = entries[len(entries)-1].Term
	}
	return nil
}

// Apply the peer with given snapshot
// ApplySnapshot 应用给定快照到 PeerStorage
func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	log.Infof("%v begin to apply snapshot", ps.Tag)
	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snapshot.Data); err != nil {
		return nil, err
	}

	// Hint: things need to do here including: update peer storage state like raftState and applyState, etc,
	// and send RegionTaskApply task to region worker through ps.regionSched, also remember call ps.clearMeta
	// and ps.clearExtraData to delete stale data
	// Your Code Here (2C).
	// 提示：在这里需要执行的操作包括：
	// 1. 更新 peer 存储状态，比如 raftState 和 applyState 等
	// 2. 通过 ps.regionSched 发送 RegionTaskApply 任务到区域工作线程
	// 3. 调用 ps.clearMeta 和 ps.clearExtraData 删除过时的数据
	// 4. 处理完成后，返回 ApplySnapResult 结果和任何可能的错误

	// 删除过时的数据
	if err := ps.clearMeta(kvWB, raftWB); err != nil {
		return nil, err
	}
	ps.clearExtraData(snapData.Region)

	// 更新 peer 存储状态
	snapMeta := snapshot.Metadata
	ps.raftState.LastIndex = snapMeta.Index
	ps.raftState.LastTerm = snapMeta.Term
	ps.applyState.AppliedIndex = snapMeta.Index
	ps.applyState.TruncatedState.Index = snapMeta.Index
	ps.applyState.TruncatedState.Term = snapMeta.Term
	ps.snapState.StateType = snap.SnapState_Applying

	// 更新区域的状态
	meta.WriteRegionState(kvWB, snapData.Region, rspb.PeerState_Normal)

	// 向 region worker 发送 runner.RegionTaskApply 任务，并等待 region worker 完成
	ch := make(chan bool, 1)
	ps.regionSched <- &runner.RegionTaskApply{
		RegionId: ps.region.Id,
		Notifier: ch,
		SnapMeta: snapMeta,
		StartKey: snapData.Region.StartKey,
		EndKey:   snapData.Region.EndKey,
	}
	if !(<-ch) {
		return nil, nil
	}

	// 返回快照应用结果
	return &ApplySnapResult{
		PrevRegion: ps.Region(),
		Region:     snapData.Region,
	}, nil
}

// Save memory states to disk.
// Do not modify ready in this function, this is a requirement to advance the ready object properly later.
// 将内存状态保存到磁盘。
// 在此函数中不要修改 ready，这是后续正确推进 ready 对象的要求。
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (applySnapResult *ApplySnapResult, err error) {
	// Hint: you may call `Append()` and `ApplySnapshot()` in this function
	// Your Code Here (2B/2C).
	// 用于写入 raft 数据的 batch
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)

	// 如果快照不为空，先应用快照
	if !raft.IsEmptySnap(&ready.Snapshot) {
		applySnapResult, err = ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB)
		if err != nil {
			return nil, err
		}
	}

	// 如果硬状态不为空，则更新节点的 ps.raftState
	if !raft.IsEmptyHardState(ready.HardState) {
		ps.raftState.HardState = &ready.HardState
	}

	// 将不稳定的日志条目写入 batch，同时更新节点的 ps.raftState
	err = ps.Append(ready.Entries, raftWB)
	if err != nil {
		return nil, err
	}

	// 将节点新的 ps.raftState 写入 batch，注意要在所有改动完成后再写入
	if err = raftWB.SetMeta(meta.RaftStateKey(ps.region.Id), ps.raftState); err != nil {
		return nil, err
	}

	// 将节点新的 ps.applyState 写入 batch，注意要在所有改动完成后再写入
	if err = kvWB.SetMeta(meta.ApplyStateKey(ps.region.Id), ps.applyState); err != nil {
		return nil, err
	}

	// raft batch 中的数据以事务的方式原子地写入数据库
	if err = ps.Engines.WriteRaft(raftWB); err != nil {
		return nil, err
	}

	// kv batch 中的数据以事务的方式原子地写入数据库
	if err = ps.Engines.WriteKV(kvWB); err != nil {
		return nil, err
	}

	return
}

func (ps *PeerStorage) ClearData() {
	ps.clearRange(ps.region.GetId(), ps.region.GetStartKey(), ps.region.GetEndKey())
}

func (ps *PeerStorage) clearRange(regionID uint64, start, end []byte) {
	ps.regionSched <- &runner.RegionTaskDestroy{
		RegionId: regionID,
		StartKey: start,
		EndKey:   end,
	}
}
