// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"sync"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
// 当请求的索引由于在最近的快照之前而不可用时，该错误会被返回。
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
// 当请求的索引早于现有的快照时，该错误会被返回。
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
// 当请求的日志条目不可用时，该错误会被返回。
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
// 当所需的快照暂时不可用时，该错误会被返回。
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.

// Storage 是一个接口，应用程序可以实现该接口以从存储中检索日志条目。
//
// 如果任何 Storage 方法返回错误，Raft 实例将变得不可操作并拒绝参与选举；
// 应用程序需负责清理和恢复。
type Storage interface {
	// InitialState returns the saved HardState and ConfState information.
	// InitialState 返回保存的 HardState 和 ConfState 信息。
	InitialState() (pb.HardState, pb.ConfState, error)

	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
	// Entries 返回范围 [lo, hi) 内的日志条目切片。
	// MaxSize 限制返回的日志条目的总大小，但如果有条目，
	// Entries 至少会返回一个条目。
	Entries(lo, hi uint64) ([]pb.Entry, error)

	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	// Term 返回条目 i 的任期，该条目必须在 [FirstIndex()-1, LastIndex()] 的范围内。
	// 尽管该条目的其它部分可能无法使用，但前一个条目的任期用于匹配目的。
	Term(i uint64) (uint64, error)

	// LastIndex returns the index of the last entry in the log.
	// LastIndex 返回日志中最后一个条目的索引。
	LastIndex() (uint64, error)

	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	// FirstIndex 返回可能通过 Entries 获取到的第一个日志条目的索引。
	// 较旧的条目已被纳入最新快照；如果存储仅包含虚拟条目，则第一个日志条目不可用。
	FirstIndex() (uint64, error)

	// Snapshot returns the most recent snapshot.
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	// Snapshot 返回最新的快照。
	// 如果快照暂时不可用，则应返回 ErrSnapshotTemporarilyUnavailable，
	// 以便 Raft 状态机知道存储需要一些时间来准备快照，并稍后再调用 Snapshot。
	Snapshot() (pb.Snapshot, error)
}

// MemoryStorage implements the Storage interface backed by an
// in-memory array.
// MemoryStorage 实现了由内存数组支持的 Storage 接口。
type MemoryStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	// 保护对所有字段的访问。大部分 MemoryStorage 的方法在 Raft goroutine 中运行，
	// 但 Append() 在应用程序 goroutine 中运行。
	sync.Mutex

	hardState pb.HardState
	snapshot  pb.Snapshot
	// ents[i] has raft log position i+snapshot.Metadata.Index
	// ents[i] 在 Raft 日志中的位置为 i + snapshot.Metadata.Index
	ents []pb.Entry
}

// NewMemoryStorage creates an empty MemoryStorage.
// NewMemoryStorage 创建一个空的 MemoryStorage。
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		// 从头开始时，用一个任期为零的虚拟条目填充列表。
		ents:     make([]pb.Entry, 1),
		snapshot: pb.Snapshot{Metadata: &pb.SnapshotMetadata{ConfState: &pb.ConfState{}}},
	}
}

// InitialState implements the Storage interface.
// InitialState 实现了 Storage 接口，返回保存的 HardState 和 ConfState 信息。
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, *ms.snapshot.Metadata.ConfState, nil
}

// SetHardState saves the current HardState.
// SetHardState 保存当前的 HardState。
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
// Entries 实现了 Storage 接口，返回范围 [lo, hi) 内的日志条目切片。
func (ms *MemoryStorage) Entries(lo, hi uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	// 如果请求的起始索引 lo 小于或等于 offset，则说明该范围已经被压缩
	if lo <= offset {
		return nil, ErrCompacted
	}
	// 该范围上限越界
	if hi > ms.lastIndex()+1 {
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}

	// 得到该范围的日志条目切片
	ents := ms.ents[lo-offset : hi-offset]
	// 如果存储中仅含有一个虚拟条目且需要返回的条目切片不为空，表示没有有效日志条目
	if len(ms.ents) == 1 && len(ents) != 0 {
		// only contains dummy entries.
		// 仅包含虚拟条目。
		return nil, ErrUnavailable
	}
	return ents, nil
}

// Term implements the Storage interface.
// Term 实现了 Storage 接口，返回条目 i 的任期，该条目必须在 [FirstIndex()-1, LastIndex()] 的范围内。
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	// 如果查询的索引 i 小于 offset，这表示该条目已经被压缩，既该条目不存在
	if i < offset {
		return 0, ErrCompacted
	}
	// 如果 i 超过了当前存储中的条目数量，即 i 的值对应的条目不存在
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	// 索引有效，返回该条目的任期
	return ms.ents[i-offset].Term, nil
}

// LastIndex implements the Storage interface.
// LastIndex 实现了 Storage 接口，返回日志中最后一个条目的索引。
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

// lastIndex 将第一个条目的索引加上存储的条目数量减去一，确保正确计算存储中的最后一个条目的索引
func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.
// FirstIndex 实现了 Storage 接口，返回可能通过 Entries 获取到的第一个日志条目的索引。
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

// firstIndex 计算当前日志存储中第一个有效条目的下一个条目的索引
func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// Snapshot implements the Storage interface.
// Snapshot 实现了 Storage 接口，返回最新的快照。
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
// ApplySnapshot 用给定快照的内容覆盖此 Storage 对象的内容。
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	// handle check for old snapshot being applied
	// 检查旧快照和传入的快照哪个更新
	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	// 如果传入的快照索引较旧，返回错误
	if msIndex >= snapIndex {
		return ErrSnapOutOfDate
	}

	// 更新当前快照为新的快照
	ms.snapshot = snap
	// 重置日志条目，只保留快照的最后一项
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
// CreateSnapshot 创建一个快照，可以通过 Snapshot() 方法获得，并用于在该时间点上重建状态。
// 如果自上次压缩以来进行了任何配置更改，必须传入最后一个 ApplyConfChange 的结果。
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	// 检查请求生成的快照索引和当前快照的索引
	if i <= ms.snapshot.Metadata.Index {
		// 请求生成的快照过旧，返回错误
		return pb.Snapshot{}, ErrSnapOutOfDate
	}
	// 检查请求的快照索引是否超出日志的范围
	if i > ms.lastIndex() {
		log.Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}

	// 获取日志条目的起始偏移量
	offset := ms.ents[0].Index

	// 更新快照的元数据：索引
	ms.snapshot.Metadata.Index = i
	// 更新快照的任期，使用给定索引的条目的任期
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	// 如果传入的配置状态不为空，更新快照的配置状态
	if cs != nil {
		ms.snapshot.Metadata.ConfState = cs
	}
	// 更新快照数据部分为传入的数据
	ms.snapshot.Data = data
	// 返回更新后的快照
	return ms.snapshot, nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
// Compact 丢弃所有在 compactIndex 之前的日志条目。
// 确保压缩的索引不大于 raftLog.applied 是应用程序自己的责任。
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	// 获取日志条目的起始偏移量
	offset := ms.ents[0].Index
	// 检查索引是否已经被压缩过
	if compactIndex <= offset {
		// 该索引被压缩过，返回错误
		return ErrCompacted
	}
	// 检查索引是否超出日志范围
	if compactIndex > ms.lastIndex() {
		log.Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	// 计算应压缩的索引在条目中的实际索引
	i := compactIndex - offset

	// 创建新的日志条目切片，保留从 compactIndex 开始的日志
	ents := make([]pb.Entry, 1, 1+uint64(len(ms.ents))-i)
	// 保持 compactIndex 位置的条目信息
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	// 追加 compactIndex 后面的条目
	ents = append(ents, ms.ents[i+1:]...)
	// 更新 ms.ents 为压缩后的条目
	ms.ents = ents

	return nil
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
// Append 将新日志条目追加到存储中。
// TODO (xiangli): 确保日志条目是连续的，并且 entries[0].Index > ms.entries[0].Index
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	// 如果没有新的日志条目，直接返回
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	// 获取存储中第一个日志条目的索引
	first := ms.firstIndex()
	// 计算要追加的日志条目中的最后一个索引
	last := entries[0].Index + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	// 如果新的日志条目在当前已存储的日志之前，直接返回
	if last < first {
		return nil
	}
	// truncate compacted entries
	// 截断已经压缩的条目
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	// 计算偏移，用于确定新条目的插入位置
	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		// 当前存储的条目数量大于新条目的开始位置索引，进行覆盖
		// 提取 offset 位置前面的条目
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...)
		// offset 位置及以后的条目直接用 entries 覆盖
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		// 当前存储的条目数量刚好是新条目的起始位置索引，直接追加
		ms.ents = append(ms.ents, entries...)
	default:
		// 从当前存储的条目数量索引到新条目的开始位置索引中间有段缺失的日志条目，直接抛出异常
		log.Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}
