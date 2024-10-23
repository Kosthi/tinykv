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

// None is a placeholder node ID used when there is no leader.
// None 是当没有领导者时使用的占位符节点 ID。
const None uint64 = 0

// StateType represents the role of a node in a cluster.
// StateType 表示节点在集群中的角色。
type StateType uint64

const (
	StateFollower  StateType = iota // 跟随者状态
	StateCandidate                  // 候选者状态
	StateLeader                     // 领导者状态
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

// String 方法返回 StateType 的字符串表示。
func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
// ErrProposalDropped 当提案因某种原因被忽略时返回，以便提案者能够快速失败。
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
// Config 包含启动 Raft 的参数。
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	// ID 是本地 Raft 的身份。ID 不能为 0。
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	// peers 包含 Raft 集群中所有节点（包括自身）的 ID。
	// 仅当启动新的 Raft 集群时才能设置。重新启动 Raft 时若设置此值，会导致 panic 错误。
	// peer 是私有变量，目前仅用于测试。
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	// ElectionTick 是在选举之间必须经过的 Node.Tick 调用次数。
	// 即如果跟随者在 ElectionTick 时间内没有收到来自当前任期领导者的消息，它将成为候选者并开始选举。
	// ElectionTick 必须大于 HeartbeatTick。我们建议将 ElectionTick 设置为 HeartbeatTick 的 10 倍，以避免不必要的领导者切换。
	ElectionTick int

	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	// HeartbeatTick 是在心跳之间必须经过的 Node.Tick 调用次数。
	// 也就是领导者每隔 HeartbeatTick ticks 发送心跳消息以维持其领导地位。
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	// Storage 是 Raft 的存储引擎。
	// Raft 把生成的条目和状态存储在其中，并在需要时读取持久化的条目和状态。
	// Raft 在重启时会从存储引擎中读取之前的状态和配置。
	Storage Storage

	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	// Applied 是最后的应用索引。仅在重新启动 Raft 时设置。
	// Raft 不会返回小于或等于 Applied 的条目。
	// 如果在重新启动时没有设置 Applied，Raft 可能会返回之前已应用的条目。这是一个非常依赖于应用程序的配置。
	Applied uint64
}

// validate 校验配置的合法性。
func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
// Progress 表示某个追随者在领导者视角中的进度。
// 领导者维护所有追随者的进度，并根据其进度向追随者发送条目。
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	// 节点 ID
	id uint64

	// 当前任期
	Term uint64

	// 得到的选票
	Vote uint64

	// the log
	// 日志
	RaftLog *RaftLog

	// log replication progress of each peers
	// 集群中每个节点的日志复制进度
	Prs map[uint64]*Progress

	// this peer's role
	// 该节点的角色
	State StateType

	// votes records
	// 投票记录
	votes map[uint64]bool

	// msgs need to send
	// 需要发送的消息
	msgs []pb.Message

	// the leader id
	// 领导者 ID
	Lead uint64

	// heartbeat interval, should send
	// 心跳间隔
	heartbeatTimeout int

	// baseline of election interval
	// 选举超时的基线
	electionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	// 自上次心跳超时以来的 tick 数量。
	// 只有领导者保持 heartbeatElapsed。
	heartbeatElapsed int

	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	// 当其为领导者或候选者时，自上次选举超时以来的 tick 数量。
	// 当其为跟随者时，自上次选举超时以来或收到来自当前领导者的有效消息后的 tick 数量。
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	// leadTransferee 是领导者转移目标的 ID，当其值不为零时有效。
	// 遵循 Raft 博士论文第 3.10 节中定义的程序。
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (用于 3A 领导者转移)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	// 仅允许一个配置更改处于挂起状态（在日志中，但尚未应用）。
	// 通过 PendingConfIndex 强制执行，该值设置为大于或等于最新挂起配置更改的日志索引（如果有）。
	// 仅在领导者的已应用索引大于该值时才允许提议配置更改。
	// (用于 3A 配置更改)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
// newRaft 返回一个具有给定配置的 Raft 节点。
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	return nil
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// sendAppend 向指定节点发送一个追加条目 RPC，包括新的条目（如果有的话）和当前的提交索引。
// 如果发送了消息，则返回 true。
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
// sendHeartbeat 向指定节点发送心跳 RPC。
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
// tick 在内部逻辑时钟上推进一个 tick。
func (r *Raft) tick() {
	// Your Code Here (2A).
}

// becomeFollower transform this peer's state to Follower
// becomeFollower 将该节点的状态转换为跟随者。
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
// becomeCandidate 将该节点的状态转换为候选者。
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
}

// becomeLeader transform this peer's state to leader
// becomeLeader 将该节点的状态转换为领导者。
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// 注意：领导者应在其任期内提议一个无操作条目（noop entry）。
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// Step 是处理消息的入口，考虑 `eraftpb` 中的 `MessageType` 了解应处理哪些消息。
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
	case StateCandidate:
	case StateLeader:
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
// handleAppendEntries 处理追加条目 RPC 请求。
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
// handleHeartbeat 处理心跳 RPC 请求。
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
// handleSnapshot 处理快照 RPC 请求。
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
// addNode 向 Raft 集群添加一个新节点。
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
// removeNode 从 Raft 集群中移除一个节点。
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
