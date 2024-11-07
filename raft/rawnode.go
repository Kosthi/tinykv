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

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
// ErrStepLocalMsg 是在尝试处理本地 Raft 消息时返回的错误
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
// ErrStepPeerNotFound 是在尝试处理响应消息时返回的错误
// 但在 raft.Prs 中找不到该节点的对等体（peer）。
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
// SoftState 提供了一个易失的状态，不需要持久化到 WAL（日志文件）中。
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
// Ready 封装了准备好读取、保存到稳定存储、已提交或发送给其他对等体的条目和消息。
// Ready 中的所有字段都是只读的。
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	// 当前节点的易失状态。
	// 如果没有更新，SoftState 将为 nil。
	// 不要求使用或存储 SoftState。
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	// 当前节点的非易失状态，在发送消息之前保存到稳定存储。
	// 如果没有更新，HardState 将等于空状态。
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	// Entries 指定在发送消息之前要保存到稳定存储的条目。
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	// Snapshot 指定要保存到稳定存储的快照。
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	// CommittedEntries 指定要提交到存储/状态机的条目。
	// 这些条目之前已经提交到稳定存储中。
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	// Messages 指定在条目提交到稳定存储后要发送的出站消息。
	// 如果它包含 MessageType_MsgSnapshot 消息，则应用程序必须在接收到快照或快照失败时，通过调用 ReportSnapshot 向 raft 报告。
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
// RawNode 是 Raft 的封装。
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	// 前一个时刻的易失状态
	prevSoftState *SoftState
	// 前一个时刻的非易失状态
	prevHardState pb.HardState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
// NewRawNode 在给定配置和 Raft 节点列表的情况下返回一个新的 RawNode。
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	hardState, _, err := config.Storage.InitialState()
	if err != nil {
		return nil, err
	}

	raft := newRaft(config)

	rawNode := &RawNode{
		Raft:          raft,
		prevSoftState: raft.softState(), // 当前时刻的易失状态
		prevHardState: hardState,        // 从存储中恢复当前时刻的非易失状态
	}

	return rawNode, nil
}

// Tick advances the internal logical clock by a single tick.
// Tick 将内部逻辑时钟推进一个计时单位。
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
// Campaign 使得这个 RawNode 过渡到候选人状态。
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
// Propose 提议将数据附加到 Raft 日志中。
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
// ProposeConfChange 提议进行配置更改。
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
// ApplyConfChange 将配置更改应用到本地节点。
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
// Step 使用给定的消息推进状态机。
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	// 忽略通过网络接收的意外本地消息
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
// Ready 返回此 RawNode 的当前时刻状态。
func (rn *RawNode) Ready() (rd Ready) {
	// Your Code Here (2A).
	raft := rn.Raft

	// 获取当前时刻的易失状态，没有更新时为 nil
	if softState := raft.softState(); !isSoftStateEqual(softState, rn.prevSoftState) {
		rn.prevSoftState = softState
		rd.SoftState = softState
	}

	// 获取当前时刻的非易失状态，没有更新时为空状态
	if hardState := raft.hardState(); !isHardStateEqual(hardState, rn.prevHardState) {
		rn.prevHardState = hardState
		rd.HardState = hardState
	}

	// 获取当前时刻节点不稳定的日志条目
	rd.Entries = raft.RaftLog.unstableEntries()

	// 获取当前时刻的快照，如果有
	if !IsEmptySnap(raft.RaftLog.pendingSnapshot) {
		rd.Snapshot = *raft.RaftLog.pendingSnapshot
	}

	// 获取当前时刻节点已经提交的日志条目
	rd.CommittedEntries = raft.RaftLog.nextEnts()
	// 获取当前时刻节点需要发出的消息
	rd.Messages = rn.Raft.msgs

	return
}

// HasReady called when RawNode user need to check if any Ready pending.
// HasReady 在 RawNode 用户需要检查是否有任何待处理的 Ready 时被调用。
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	raft := rn.Raft
	// 易失状态是否改变
	if softState := raft.softState(); !isSoftStateEqual(softState, rn.prevSoftState) {
		return true
	}
	// 非易失状态是否改变
	if hardState := raft.hardState(); !isHardStateEqual(hardState, rn.prevHardState) {
		return true
	}
	// 是否有快照
	if !IsEmptySnap(raft.RaftLog.pendingSnapshot) {
		return true
	}
	// 是否有待持久化的日志条目或待处理的消息
	if len(raft.RaftLog.unstableEntries()) > 0 || len(raft.RaftLog.nextEnts()) > 0 || len(raft.msgs) > 0 {
		return true
	}
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
// Advance 通知 RawNode 应用程序已经应用并保存了在上一个 Ready 结果中的进展。
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	raft := rn.Raft

	// 更新写入稳定存储的 stabled 指针
	if len(rd.Entries) > 0 {
		raft.RaftLog.stabled = rd.Entries[len(rd.Entries)-1].Index
	}
	// 更新应用到状态机的 applied 指针
	if len(rd.CommittedEntries) > 0 {
		raft.RaftLog.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		// 把已经应用到状态机的日志条目移除
		// 出现日志重复的 bug，暂时先注释
		// raft.RaftLog.entries = raft.RaftLog.entries[raft.RaftLog.applied-raft.RaftLog.FirstIndex()+1:]
	}

	// 尝试压缩日志
	raft.RaftLog.maybeCompact()
	// 快照记得删除
	raft.RaftLog.pendingSnapshot = nil
	// 消息处理完了，清空消息
	raft.msgs = make([]pb.Message, 0)
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
// GetProgress 返回当前节点及其对等节点的进度，前提是该节点是领导者。
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
// TransferLeader 尝试将领导权转移给指定的接任者。
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
