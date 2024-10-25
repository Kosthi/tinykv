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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
// RaftLog 用于管理日志条目。
// 为了简化 RaftLog 的实现，应该管理所有未被截断的日志条目。
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	// storage 包含自上次快照以来的所有稳定条目。
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// committed 表示已知在多数节点的稳定存储中的最高日志位置。
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// applied 表示应用程序已被指示应用到状态机的最高日志位置。
	// 恒等式： applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// stabled 表示所有已写入持久化存储的日志条目的索引。这意味着所有索引小于或等于 stabled 的日志条目已经安全存储，并可以在故障恢复后使用。
	// 每次处理 `Ready` 时，未稳定的日志条目将被包含在内。
	stabled uint64

	// all entries that have not yet compact.
	// 所有未压缩的条目。
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	// 当前的未稳定快照，如果有的话。
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
// newLog 返回使用给定存储的日志。它恢复日志到刚刚提交的状态，并应用最新的快照。
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	return nil
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
// 我们需要在某个时间点对日志条目进行压缩，以便存储稳定的日志条目，防止日志条目在内存中无限增长。
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
// allEntries 返回所有未压缩的条目。
// 注意，返回值中排除任何虚拟条目。
// 注意，这是你需要实现的测试函数之一。
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// unstableEntries return all the unstable entries
// unstableEntries 返回所有不稳定的条目。
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// nextEnts returns all the committed but not applied entries
// nextEnts 返回所有已提交但未应用的条目。
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
// LastIndex 返回日志条目的最后索引。
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return 0
}

// Term return the term of the entry in the given index
// Term 返回给定索引处条目的任期。
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	return 0, nil
}
