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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

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
	// stabled 表示已经写入持久化存储的日志条目的最高索引。
	// 这意味着所有索引小于或等于 stabled 的日志条目已经安全存储，并可以在故障恢复后使用。
	// 每次处理 `Ready` 时，未稳定的日志条目将被包含在内。
	// 注意，这里稳定的语义是指写入了持久化存储中，但是不一定复制到了大多数节点并提交。
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
	// 从存储中获取持久化状态和配置状态
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err.Error())
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err.Error())
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err.Error())
	}

	// 从存储加载条目，直到已提交的索引
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err.Error())
	}

	// 创建新的 RaftLog 实例
	raftLog := &RaftLog{
		storage:         storage,
		committed:       hardState.Commit,
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
	}

	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
// 我们需要在某个时间点对日志条目进行压缩，以便存储稳定的日志条目，防止日志条目在内存中无限增长。
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) Entry(index uint64) *pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 && l.entries[0].Index <= index {
		firstIndex := l.entries[0].Index
		if index <= l.LastIndex() {
			return &l.entries[index-firstIndex]
		}
		log.Panicf("Entry's index(%d) is out of bound lastindex(%d)", index, l.LastIndex())
	}
	entry, err := l.storage.Entries(index, index+1)
	if err != nil {
		panic(err.Error())
	}
	return &entry[0]
}

func (l *RaftLog) Entries(lo, hi uint64) []*pb.Entry {
	var entries []*pb.Entry
	for i := lo; i < hi; i++ {
		entries = append(entries, l.Entry(i))
	}
	return entries
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
// allEntries 返回所有未压缩的条目。
// 注意，返回值中排除任何虚拟条目。
// 注意，这是你需要实现的测试函数之一。
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
// unstableEntries 返回所有不稳定的条目。
func (l *RaftLog) unstableEntries() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		firstIndex := l.FirstIndex()
		if l.stabled < firstIndex {
			return l.entries
		}
		if l.stabled-firstIndex+1 < uint64(len(l.entries)) {
			return l.entries[l.stabled-firstIndex+1:]
		}
	}
	// 测试里要求返回空条目而不是 nil
	return []pb.Entry{}
}

// nextEnts returns all the committed but not applied entries
// nextEnts 返回所有已提交但未应用的条目。
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		firstIndex := l.FirstIndex()
		if l.applied+1 >= firstIndex {
			return l.entries[l.applied-firstIndex+1 : l.committed-firstIndex+1]
		}
	}
	// 虽然测试没要求，但是与 unstableEntries 一致
	return []pb.Entry{}
}

// FirstIndex 返回日志条目的第一个索引。
func (l *RaftLog) FirstIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		firstIndex, err := l.storage.FirstIndex()
		if err != nil {
			panic(err.Error())
		}
		return firstIndex
	}
	return l.entries[0].Index
}

// LastIndex return the last index of the log entries
// LastIndex 返回日志条目的最后索引。
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var length = len(l.entries)
	if length == 0 {
		lastIndex, err := l.storage.LastIndex()
		if err != nil {
			panic(err.Error())
		}
		return lastIndex
	}
	return l.entries[length-1].Index
}

// Term return the term of the entry in the given index
// Term 返回给定索引处条目的任期。
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		firstIndex := l.FirstIndex()
		lastIndex := l.LastIndex()
		if i >= firstIndex && i <= lastIndex {
			return l.entries[i-firstIndex].Term, nil
		}
	}
	term, err := l.storage.Term(i)
	if err != nil {
		return 0, err
	}
	return term, nil
}

// truncateBelow 将当前日志修剪到指定索引（不包括该索引）。
func (l *RaftLog) truncateBelow(index uint64) {
	// 确保索引在当前日志范围内
	firstIndex := l.FirstIndex()
	if index >= firstIndex && index <= l.LastIndex() {
		l.entries = l.entries[:index-firstIndex]     // 截取到 index-firstIndex
		l.stabled = min(l.stabled, index-firstIndex) // 更新稳定的日志索引
	}
}

// isLogUpToDate 检查指定的日志条目是否为最新
func (l *RaftLog) isLogUpToDate(candidateLastLogTerm, candidateLastLogIndex uint64) bool {
	// 获取当前节点最后一个条目的索引和任期
	lastIndex := l.LastIndex()
	term, err := l.Term(lastIndex)
	if err != nil {
		log.Errorf("Failed to get term for last index %d: %v", lastIndex, err)
		return false
	}

	// 如果候选人的日志任期更高，或者任期相同但索引更高，返回 true
	if candidateLastLogTerm > term {
		log.Debugf("Candidate's log is newer (term: %d > current term: %d)", candidateLastLogTerm, term)
		return true
	} else if candidateLastLogTerm == term && candidateLastLogIndex >= lastIndex {
		log.Debugf("Candidate's log is as new (term: %d == current term: %d) and has index (index: %d >= current index: %d)",
			candidateLastLogTerm, term, candidateLastLogIndex, lastIndex)
		return true
	}

	log.Debugf("Current log is up to date. Candidate's log is not newer: (candidateTerm: %d, candidateIndex: %d) <= (currentTerm: %d, currentIndex: %d)",
		candidateLastLogTerm, candidateLastLogIndex, term, lastIndex)
	return false
}
