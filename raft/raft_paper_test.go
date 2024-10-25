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

/*
This file contains tests which verify that the scenarios described
in the raft paper (https://raft.github.io/raft.pdf) are handled by the
raft implementation correctly. Each test focuses on several sentences
written in the paper.

Each test is composed of three parts: init, test and check.
Init part uses simple and understandable way to simulate the init state.
Test part uses Step function to generate the scenario. Check part checks
outgoing messages and state.
*/

/*
此文件包含测试，以验证 Raft 论文中描述的场景（https://raft.github.io/raft.pdf）是否被 Raft 实现正确处理。
每个测试重点关注论文中几句话所描述的内容。

每个测试由三个部分组成：初始化（init）、测试（test）和检查（check）。
初始化部分使用简单易懂的方法来模拟初始状态。
测试部分使用 Step 函数来生成场景。检查部分检查传出的消息和状态。
*/
package raft

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// 以下三种状态都因为发现任期过期而更新任期并成为跟随者
func TestFollowerUpdateTermFromMessage2AA(t *testing.T) {
	testUpdateTermFromMessage(t, StateFollower)
}
func TestCandidateUpdateTermFromMessage2AA(t *testing.T) {
	testUpdateTermFromMessage(t, StateCandidate)
}
func TestLeaderUpdateTermFromMessage2AA(t *testing.T) {
	testUpdateTermFromMessage(t, StateLeader)
}

// testUpdateTermFromMessage tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
// testUpdateTermFromMessage 测试
// 如果一个服务器的当前任期比另一个的任期小，那么它将其当前任期更新为更大的值。
// 如果候选人或领导者发现其任期过时，那么它会立即恢复到跟随者状态。
// 参考: 第 5.1 节
func testUpdateTermFromMessage(t *testing.T, state StateType) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	switch state {
	case StateFollower:
		r.becomeFollower(1, 2)
	case StateCandidate:
		r.becomeCandidate()
	case StateLeader:
		r.becomeCandidate()
		r.becomeLeader()
	}

	r.Step(pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2})

	if r.Term != 2 {
		t.Errorf("term = %d, want %d", r.Term, 2)
	}
	if r.State != StateFollower {
		t.Errorf("state = %v, want %v", r.State, StateFollower)
	}
}

// TestStartAsFollower tests that when servers start up, they begin as followers.
// Reference: section 5.2
// TestStartAsFollower 测试服务器启动时，它们应作为跟随者开始运行。
// 参考: 第 5.2 节
func TestStartAsFollower2AA(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	if r.State != StateFollower {
		t.Errorf("state = %s, want %s", r.State, StateFollower)
	}
}

// TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
// it will send a MessageType_MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries
// as heartbeat to all followers.
// Reference: section 5.2
// TestLeaderBcastBeat 测试如果领导者接收到心跳信号，它将发送一个 MessageType_MsgHeartbeat，
// 其中 m.Index = 0，m.LogTerm=0，并且条目为空，作为心跳发送给所有跟随者。
// 参考: 第 5.2 节
func TestLeaderBcastBeat2AA(t *testing.T) {
	// heartbeat interval
	// 心跳间隔为 1
	hi := 1
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, hi, NewMemoryStorage())
	r.becomeCandidate()
	r.becomeLeader()

	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	r.readMessages() // clear message

	// 经过一个心跳间隔，领导者应该给其他节点发送心跳
	for i := 0; i < hi; i++ {
		r.tick()
	}

	// 读取领导者发送的消息并排序
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	// 期望领导者发送的消息
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 1, MsgType: pb.MessageType_MsgHeartbeat},
		{From: 1, To: 3, Term: 1, MsgType: pb.MessageType_MsgHeartbeat},
	}
	// 检查是否一致
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %v, want %v", msgs, wmsgs)
	}
}

func TestFollowerStartElection2AA(t *testing.T) {
	testNonleaderStartElection(t, StateFollower)
}
func TestCandidateStartNewElection2AA(t *testing.T) {
	testNonleaderStartElection(t, StateCandidate)
}

// testNonleaderStartElection tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
// testNonleaderStartElection 测试如果一个跟随者在选举超时时间内未收到任何通信，它将开始选举以选择新的领导者。
// 它会递增当前任期并转换为候选人状态。然后，它会对自己投票，并并行向集群中的其他服务器发起 RequestVote RPC。
// 参考: 第 5.2 节
// 此外，如果候选人未能获得多数票，它将超时并通过递增其任期开始新的选举，并发起另一轮 RequestVote RPC。
// 参考: 第 5.2 节
func testNonleaderStartElection(t *testing.T, state StateType) {
	// election timeout
	// 选举超时时间
	et := 10
	r := newTestRaft(1, []uint64{1, 2, 3}, et, 1, NewMemoryStorage())
	switch state {
	case StateFollower:
		r.becomeFollower(1, 2)
	case StateCandidate:
		r.becomeCandidate()
	}

	// 在选举超时时间内，跟随者未收到任何通信或候选者未能获得多数票，它将开始选举以选择新的领导者
	// 它会递增当前任期并转换为候选人状态，然后它会对自己投票，并向其他节点发起投票请求
	// 这里经过 19 个时间，所以 timeout 最大设置为 19
	for i := 1; i < 2*et; i++ {
		r.tick()
	}

	if r.Term != 2 {
		t.Errorf("term = %d, want 2", r.Term)
	}
	if r.State != StateCandidate {
		t.Errorf("state = %s, want %s", r.State, StateCandidate)
	}
	if !r.votes[r.id] {
		t.Errorf("vote for self = false, want true")
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 2, MsgType: pb.MessageType_MsgRequestVote},
		{From: 1, To: 3, Term: 2, MsgType: pb.MessageType_MsgRequestVote},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %v, want %v", msgs, wmsgs)
	}
}

// TestLeaderElectionInOneRoundRPC tests all cases that may happen in
// leader election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
// TestLeaderElectionInOneRoundRPC 测试在一次 RequestVote RPC 轮次中可能发生的所有情况：
// a) 它赢得了选举
// b) 它输掉了选举
// c) 它对结果不明确
// 参考: 第 5.2 节
func TestLeaderElectionInOneRoundRPC2AA(t *testing.T) {
	tests := []struct {
		size  int
		votes map[uint64]bool
		state StateType
	}{
		// 每一行是一组测试，一种情况
		// win the election when receiving votes from a majority of the servers
		// 当收到大多数服务器的选票时，赢得选举
		{1, map[uint64]bool{}, StateLeader}, // 集群只有一个节点，没有必要发送投票请求，自动成为领导者
		{3, map[uint64]bool{2: true, 3: true}, StateLeader},
		{3, map[uint64]bool{2: true}, StateLeader},
		{5, map[uint64]bool{2: true, 3: true, 4: true, 5: true}, StateLeader},
		{5, map[uint64]bool{2: true, 3: true, 4: true}, StateLeader},
		{5, map[uint64]bool{2: true, 3: true}, StateLeader},

		// stay in candidate if it does not obtain the majority
		// 如果未获得大多数选票，则保持为候选人
		{3, map[uint64]bool{}, StateCandidate},
		{5, map[uint64]bool{2: true}, StateCandidate},
		{5, map[uint64]bool{2: false, 3: false}, StateCandidate},
		{5, map[uint64]bool{}, StateCandidate},
	}
	for i, tt := range tests {
		// 初始化有 size 个节点的 raft 集群，当前节点初始状态为跟随者
		r := newTestRaft(1, idsBySize(tt.size), 10, 1, NewMemoryStorage())

		// 当前节点收到 MsgHup 消息开始选举
		r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
		// 接收投票请求回复，统计投票情况
		for id, vote := range tt.votes {
			r.Step(pb.Message{From: id, To: 1, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: !vote})
		}

		// 检查当前节点状态是否与期望一致
		if r.State != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, r.State, tt.state)
		}
		// 检查当前节点任期是否与期望一致
		if g := r.Term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

// TestFollowerVote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
// TestFollowerVote 测试每个跟随者在给定任期内最多只会投票给一个候选人，采用先到先得的原则。
// 参考：第 5.2 节
func TestFollowerVote2AA(t *testing.T) {
	tests := []struct {
		vote    uint64 // 当前节点投给了谁
		nvote   uint64 // 哪个节点请求投票
		wreject bool   // 是否投票
	}{
		{None, 1, false}, // 还没有投给其他人，节点 1 的请求先来，投给节点 1
		{None, 2, false}, // 还没有投给其他人，节点 2 的请求先来，投给节点 2
		{1, 1, false},    // 已经投给了节点 1，请求选票的也是节点 1，那么投给节点 1
		{2, 2, false},    // 已经投给了节点 2，请求选票的也是节点 2，那么投给节点 2
		{1, 2, true},     // 已经投给了节点 1，请求选票的是节点 2，拒绝投票
		{2, 1, true},     // 已经投给了节点 2，请求选票的是节点 1，拒绝投票
	}
	for i, tt := range tests {
		// 初始化有 3 个节点的 raft 集群，当前节点初始状态为跟随者
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		// 设置任期为 1
		r.Term = 1
		// 设置选票投给了谁
		r.Vote = tt.vote

		// 当前节点接收到请求选票消息
		r.Step(pb.Message{From: tt.nvote, To: 1, Term: 1, MsgType: pb.MessageType_MsgRequestVote})

		// 读取当前节点发送的消息
		msgs := r.readMessages()
		// 期望当前节点发送的消息内容
		wmsgs := []pb.Message{
			{From: 1, To: tt.nvote, Term: 1, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: tt.wreject},
		}
		// 比较消息内容是否一致
		if !reflect.DeepEqual(msgs, wmsgs) {
			t.Errorf("#%d: msgs = %v, want %v", i, msgs, wmsgs)
		}
	}
}

// TestCandidateFallback tests that while waiting for votes,
// if a candidate receives an AppendEntries RPC from another server claiming
// to be leader whose term is at least as large as the candidate's current term,
// it recognizes the leader as legitimate and returns to follower state.
// Reference: section 5.2
// TestCandidateFallback 测试在等待投票时，如果候选者接收到来自另一台服务器的 AppendEntries RPC，
// 该服务器声称是领导者并且其任期至少与候选者的当前任期一样大，则候选者会识别该领导者为合法的，并返回到跟随者状态。
// 参考：第 5.2 节
func TestCandidateFallback2AA(t *testing.T) {
	tests := []pb.Message{
		{From: 2, To: 1, Term: 1, MsgType: pb.MessageType_MsgAppend},
		{From: 2, To: 1, Term: 2, MsgType: pb.MessageType_MsgAppend},
	}
	for i, tt := range tests {
		// 初始化有 3 个节点的 raft 集群，当前节点初始状态为跟随者
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		// 当前节点发起选举消息，向其他节点请求投票
		r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
		if r.State != StateCandidate {
			t.Fatalf("unexpected state = %s, want %s", r.State, StateCandidate)
		}

		// 等待选票的当前节点收到追加日志消息，判断该领导者是否合法
		r.Step(tt)

		// 领导者合法的话应该转变成跟随者，并把自己的任期设置为领导者相同
		if g := r.State; g != StateFollower {
			t.Errorf("#%d: state = %s, want %s", i, g, StateFollower)
		}
		if g := r.Term; g != tt.Term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.Term)
		}
	}
}

func TestFollowerElectionTimeoutRandomized2AA(t *testing.T) {
	testNonleaderElectionTimeoutRandomized(t, StateFollower)
}
func TestCandidateElectionTimeoutRandomized2AA(t *testing.T) {
	testNonleaderElectionTimeoutRandomized(t, StateCandidate)
}

// testNonleaderElectionTimeoutRandomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
// testNonleaderElectionTimeoutRandomized 测试跟随者或候选者的选举超时时间是随机化的。
// 参考：第 5.2 节
func testNonleaderElectionTimeoutRandomized(t *testing.T, state StateType) {
	et := 10
	r := newTestRaft(1, []uint64{1, 2, 3}, et, 1, NewMemoryStorage())
	timeouts := make(map[int]bool)
	// 执行 500 轮，记住每轮的选举超时时间
	for round := 0; round < 50*et; round++ {
		switch state {
		case StateFollower:
			r.becomeFollower(r.Term+1, 2)
		case StateCandidate:
			r.becomeCandidate()
		}

		// 推进时间直到发生选举
		time := 0
		for len(r.readMessages()) == 0 {
			r.tick()
			time++
		}
		// 存储选举超时时间
		timeouts[time] = true
	}

	// 检查 11~20 的选举超时时间是否发生过
	// 执行轮数保证该范围发生的概率很高
	for d := et + 1; d < 2*et; d++ {
		if !timeouts[d] {
			t.Errorf("timeout in %d ticks should happen", d)
		}
	}
}

func TestFollowersElectionTimeoutNonconflict2AA(t *testing.T) {
	testNonleadersElectionTimeoutNonconflict(t, StateFollower)
}
func TestCandidatesElectionTimeoutNonconflict2AA(t *testing.T) {
	testNonleadersElectionTimeoutNonconflict(t, StateCandidate)
}

// testNonleadersElectionTimeoutNonconflict tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
// testNonleadersElectionTimeoutNonconflict 测试
// 在大多数情况下，只有单个服务器（追随者或候选人）会超时，这减少了在新选举中的分裂投票的可能性。
// 参考: 第5.2节
func testNonleadersElectionTimeoutNonconflict(t *testing.T, state StateType) {
	et := 10
	size := 5
	rs := make([]*Raft, size)
	ids := idsBySize(size)
	// 初始化 size 个 raft 节点
	for k := range rs {
		rs[k] = newTestRaft(ids[k], ids, et, 1, NewMemoryStorage())
	}
	conflicts := 0
	// 执行 1000 轮，记录发生两个及以上的服务器选举超时的次数
	for round := 0; round < 1000; round++ {
		for _, r := range rs {
			switch state {
			case StateFollower:
				r.becomeFollower(r.Term+1, None)
			case StateCandidate:
				r.becomeCandidate()
			}
		}

		// 推进时间，直到同一时刻至少有一个节点发生选举超时
		timeoutNum := 0
		for timeoutNum == 0 {
			for _, r := range rs {
				r.tick()
				if len(r.readMessages()) > 0 {
					timeoutNum++
				}
			}
		}
		// several rafts time out at the same tick
		// 多个 Raft 节点在同一时刻超时
		if timeoutNum > 1 {
			conflicts++
		}
	}

	// 同一时刻发生多个节点选举超时的概率是 20% 左右
	if g := float64(conflicts) / 1000; g > 0.3 {
		t.Errorf("probability of conflicts = %v, want <= 0.3", g)
	}
}

// TestLeaderStartReplication tests that when receiving client proposals,
// the leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate
// the entry. Also, when sending an AppendEntries RPC, the leader includes
// the index and term of the entry in its log that immediately precedes
// the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
// TestLeaderStartReplication 测试当接收到客户端提议时，领导者将提议作为新的条目追加到其日志中，
// 然后并行地向其他服务器发送 AppendEntries RPC 以复制该条目。此外，在发送 AppendEntries RPC 时，
// 领导者还包括其日志中紧接着新条目的条目的索引和任期。同时，领导者将新条目写入稳定存储。
// 参考文献：第 5.3 节
func TestLeaderStartReplication2AB(t *testing.T) {
	s := NewMemoryStorage()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	commitNoopEntry(r, s)
	li := r.RaftLog.LastIndex()

	ents := []*pb.Entry{{Data: []byte("some data")}}
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: ents})

	if g := r.RaftLog.LastIndex(); g != li+1 {
		t.Errorf("lastIndex = %d, want %d", g, li+1)
	}
	if g := r.RaftLog.committed; g != li {
		t.Errorf("committed = %d, want %d", g, li)
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	ent := pb.Entry{Index: li + 1, Term: 1, Data: []byte("some data")}
	wents := []pb.Entry{ent}
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 1, MsgType: pb.MessageType_MsgAppend, Index: li, LogTerm: 1, Entries: []*pb.Entry{&ent}, Commit: li},
		{From: 1, To: 3, Term: 1, MsgType: pb.MessageType_MsgAppend, Index: li, LogTerm: 1, Entries: []*pb.Entry{&ent}, Commit: li},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %+v, want %+v", msgs, wmsgs)
	}
	if g := r.RaftLog.unstableEntries(); !reflect.DeepEqual(g, wents) {
		t.Errorf("ents = %+v, want %+v", g, wents)
	}
}

// TestLeaderCommitEntry tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine.
// Also, the leader keeps track of the highest index it knows to be committed,
// and it includes that index in future AppendEntries RPCs so that the other
// servers eventually find out.
// Reference: section 5.3
// TestLeaderCommitEntry 测试当条目已被安全复制时，领导者会发出可以应用于其状态机的已应用条目。
// 此外，领导者跟踪它所知道的最高提交索引，并将该索引包含在未来的 AppendEntries RPC 中，以便其他服务器最终能够得知。
// 参考文献：第 5.3 节
func TestLeaderCommitEntry2AB(t *testing.T) {
	s := NewMemoryStorage()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	commitNoopEntry(r, s)
	li := r.RaftLog.LastIndex()
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})

	for _, m := range r.readMessages() {
		r.Step(acceptAndReply(m))
	}

	if g := r.RaftLog.committed; g != li+1 {
		t.Errorf("committed = %d, want %d", g, li+1)
	}
	wents := []pb.Entry{{Index: li + 1, Term: 1, Data: []byte("some data")}}
	if g := r.RaftLog.nextEnts(); !reflect.DeepEqual(g, wents) {
		t.Errorf("nextEnts = %+v, want %+v", g, wents)
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	for i, m := range msgs {
		if w := uint64(i + 2); m.To != w {
			t.Errorf("to = %d, want %d", m.To, w)
		}
		if m.MsgType != pb.MessageType_MsgAppend {
			t.Errorf("type = %v, want %v", m.MsgType, pb.MessageType_MsgAppend)
		}
		if m.Commit != li+1 {
			t.Errorf("commit = %d, want %d", m.Commit, li+1)
		}
	}
}

// TestLeaderAcknowledgeCommit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
func TestLeaderAcknowledgeCommit2AB(t *testing.T) {
	tests := []struct {
		size      int
		acceptors map[uint64]bool
		wack      bool
	}{
		{1, nil, true},
		{3, nil, false},
		{3, map[uint64]bool{2: true}, true},
		{3, map[uint64]bool{2: true, 3: true}, true},
		{5, nil, false},
		{5, map[uint64]bool{2: true}, false},
		{5, map[uint64]bool{2: true, 3: true}, true},
		{5, map[uint64]bool{2: true, 3: true, 4: true}, true},
		{5, map[uint64]bool{2: true, 3: true, 4: true, 5: true}, true},
	}
	for i, tt := range tests {
		s := NewMemoryStorage()
		r := newTestRaft(1, idsBySize(tt.size), 10, 1, s)
		r.becomeCandidate()
		r.becomeLeader()
		commitNoopEntry(r, s)
		li := r.RaftLog.LastIndex()
		r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})

		for _, m := range r.readMessages() {
			if tt.acceptors[m.To] {
				r.Step(acceptAndReply(m))
			}
		}

		if g := r.RaftLog.committed > li; g != tt.wack {
			t.Errorf("#%d: ack commit = %v, want %v", i, g, tt.wack)
		}
	}
}

// TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including
// entries created by previous leaders.
// Also, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
func TestLeaderCommitPrecedingEntries2AB(t *testing.T) {
	tests := [][]pb.Entry{
		{},
		{{Term: 2, Index: 1}},
		{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
		{{Term: 1, Index: 1}},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(tt)
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
		r.Term = 2
		r.becomeCandidate()
		r.becomeLeader()
		r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})

		for _, m := range r.readMessages() {
			r.Step(acceptAndReply(m))
		}

		li := uint64(len(tt))
		wents := append(tt, pb.Entry{Term: 3, Index: li + 1}, pb.Entry{Term: 3, Index: li + 2, Data: []byte("some data")})
		if g := r.RaftLog.nextEnts(); !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: ents = %+v, want %+v", i, g, wents)
		}
	}
}

// TestFollowerCommitEntry tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
func TestFollowerCommitEntry2AB(t *testing.T) {
	tests := []struct {
		ents   []*pb.Entry
		commit uint64
	}{
		{
			[]*pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
			},
			1,
		},
		{
			[]*pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
				{Term: 1, Index: 2, Data: []byte("some data2")},
			},
			2,
		},
		{
			[]*pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data2")},
				{Term: 1, Index: 2, Data: []byte("some data")},
			},
			2,
		},
		{
			[]*pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
				{Term: 1, Index: 2, Data: []byte("some data2")},
			},
			1,
		},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		r.becomeFollower(1, 2)

		r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppend, Term: 1, Entries: tt.ents, Commit: tt.commit})

		if g := r.RaftLog.committed; g != tt.commit {
			t.Errorf("#%d: committed = %d, want %d", i, g, tt.commit)
		}
		wents := make([]pb.Entry, 0, tt.commit)
		for _, ent := range tt.ents[:int(tt.commit)] {
			wents = append(wents, *ent)
		}
		if g := r.RaftLog.nextEnts(); !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: nextEnts = %v, want %v", i, g, wents)
		}
	}
}

// TestFollowerCheckMessageType_MsgAppend tests that if the follower does not find an
// entry in its log with the same index and term as the one in AppendEntries RPC,
// then it refuses the new entries. Otherwise it replies that it accepts the
// append entries.
// Reference: section 5.3
func TestFollowerCheckMessageType_MsgAppend2AB(t *testing.T) {
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		term    uint64
		index   uint64
		wreject bool
	}{
		// match with committed entries
		{0, 0, false},
		{ents[0].Term, ents[0].Index, false},
		// match with uncommitted entries
		{ents[1].Term, ents[1].Index, false},

		// unmatch with existing entry
		{ents[0].Term, ents[1].Index, true},
		// unexisting entry
		{ents[1].Term + 1, ents[1].Index + 1, true},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(ents)
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
		r.RaftLog.committed = 1
		r.becomeFollower(2, 2)
		msgs := r.readMessages() // clear message

		r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: tt.term, Index: tt.index})

		msgs = r.readMessages()
		if len(msgs) != 1 {
			t.Errorf("#%d: len(msgs) = %+v, want %+v", i, len(msgs), 1)
		}
		if msgs[0].Term != 2 {
			t.Errorf("#%d: term = %+v, want %+v", i, msgs[0].Term, 2)
		}
		if msgs[0].Reject != tt.wreject {
			t.Errorf("#%d: reject = %+v, want %+v", i, msgs[0].Reject, tt.wreject)
		}
	}
}

// TestFollowerAppendEntries tests that when AppendEntries RPC is valid,
// the follower will delete the existing conflict entry and all that follow it,
// and append any new entries not already in the log.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
func TestFollowerAppendEntries2AB(t *testing.T) {
	tests := []struct {
		index, term uint64
		lterm       uint64
		ents        []*pb.Entry
		wents       []*pb.Entry
		wunstable   []*pb.Entry
	}{
		{
			2, 2, 3,
			[]*pb.Entry{{Term: 3, Index: 3}},
			[]*pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}},
			[]*pb.Entry{{Term: 3, Index: 3}},
		},
		{
			1, 1, 4,
			[]*pb.Entry{{Term: 3, Index: 2}, {Term: 4, Index: 3}},
			[]*pb.Entry{{Term: 1, Index: 1}, {Term: 3, Index: 2}, {Term: 4, Index: 3}},
			[]*pb.Entry{{Term: 3, Index: 2}, {Term: 4, Index: 3}},
		},
		{
			0, 0, 2,
			[]*pb.Entry{{Term: 1, Index: 1}},
			[]*pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
			[]*pb.Entry{},
		},
		{
			0, 0, 3,
			[]*pb.Entry{{Term: 3, Index: 1}},
			[]*pb.Entry{{Term: 3, Index: 1}},
			[]*pb.Entry{{Term: 3, Index: 1}},
		},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}})
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
		r.becomeFollower(2, 2)

		r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppend, Term: tt.lterm, LogTerm: tt.term, Index: tt.index, Entries: tt.ents})

		wents := make([]pb.Entry, 0, len(tt.wents))
		for _, ent := range tt.wents {
			wents = append(wents, *ent)
		}
		if g := r.RaftLog.allEntries(); !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: ents = %+v, want %+v", i, g, wents)
		}
		var wunstable []pb.Entry
		if tt.wunstable != nil {
			wunstable = make([]pb.Entry, 0, len(tt.wunstable))
		}
		for _, ent := range tt.wunstable {
			wunstable = append(wunstable, *ent)
		}
		if g := r.RaftLog.unstableEntries(); !reflect.DeepEqual(g, wunstable) {
			t.Errorf("#%d: unstableEnts = %+v, want %+v", i, g, wunstable)
		}
	}
}

// TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
// into consistency with its own.
// Reference: section 5.3, figure 7
func TestLeaderSyncFollowerLog2AB(t *testing.T) {
	ents := []pb.Entry{
		{},
		{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
		{Term: 4, Index: 4}, {Term: 4, Index: 5},
		{Term: 5, Index: 6}, {Term: 5, Index: 7},
		{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10},
	}
	term := uint64(8)
	tests := [][]pb.Entry{
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5},
			{Term: 5, Index: 6}, {Term: 5, Index: 7},
			{Term: 6, Index: 8}, {Term: 6, Index: 9},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5},
			{Term: 5, Index: 6}, {Term: 5, Index: 7},
			{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10}, {Term: 6, Index: 11},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5},
			{Term: 5, Index: 6}, {Term: 5, Index: 7},
			{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10},
			{Term: 7, Index: 11}, {Term: 7, Index: 12},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5}, {Term: 4, Index: 6}, {Term: 4, Index: 7},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 2, Index: 4}, {Term: 2, Index: 5}, {Term: 2, Index: 6},
			{Term: 3, Index: 7}, {Term: 3, Index: 8}, {Term: 3, Index: 9}, {Term: 3, Index: 10}, {Term: 3, Index: 11},
		},
	}
	for i, tt := range tests {
		leadStorage := NewMemoryStorage()
		leadStorage.Append(ents)
		lead := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, leadStorage)
		lead.Term = term
		lead.RaftLog.committed = lead.RaftLog.LastIndex()
		followerStorage := NewMemoryStorage()
		followerStorage.Append(tt)
		follower := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, followerStorage)
		follower.Term = term - 1
		// It is necessary to have a three-node cluster.
		// The second may have more up-to-date log than the first one, so the
		// first node needs the vote from the third node to become the leader.
		n := newNetwork(lead, follower, nopStepper)
		n.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
		// The election occurs in the term after the one we loaded with
		// lead's term and committed index setted up above.
		n.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgRequestVoteResponse, Term: term + 1})

		n.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})

		if g := diffu(ltoa(lead.RaftLog), ltoa(follower.RaftLog)); g != "" {
			t.Errorf("#%d: log diff:\n%s", i, g)
		}
	}
}

// TestVoteRequest tests that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
func TestVoteRequest2AB(t *testing.T) {
	tests := []struct {
		ents  []*pb.Entry
		wterm uint64
	}{
		{[]*pb.Entry{{Term: 1, Index: 1}}, 2},
		{[]*pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}, 3},
	}
	for j, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		r.Step(pb.Message{
			From: 2, To: 1, MsgType: pb.MessageType_MsgAppend, Term: tt.wterm - 1, LogTerm: 0, Index: 0, Entries: tt.ents,
		})
		r.readMessages()

		for r.State != StateCandidate {
			r.tick()
		}

		msgs := r.readMessages()
		sort.Sort(messageSlice(msgs))
		if len(msgs) != 2 {
			t.Fatalf("#%d: len(msg) = %d, want %d", j, len(msgs), 2)
		}
		for i, m := range msgs {
			if m.MsgType != pb.MessageType_MsgRequestVote {
				t.Errorf("#%d: msgType = %d, want %d", i, m.MsgType, pb.MessageType_MsgRequestVote)
			}
			if m.To != uint64(i+2) {
				t.Errorf("#%d: to = %d, want %d", i, m.To, i+2)
			}
			if m.Term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, m.Term, tt.wterm)
			}
			windex, wlogterm := tt.ents[len(tt.ents)-1].Index, tt.ents[len(tt.ents)-1].Term
			if m.Index != windex {
				t.Errorf("#%d: index = %d, want %d", i, m.Index, windex)
			}
			if m.LogTerm != wlogterm {
				t.Errorf("#%d: logterm = %d, want %d", i, m.LogTerm, wlogterm)
			}
		}
	}
}

// TestVoter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
func TestVoter2AB(t *testing.T) {
	tests := []struct {
		ents    []pb.Entry
		logterm uint64
		index   uint64

		wreject bool
	}{
		// same logterm
		{[]pb.Entry{{Term: 1, Index: 1}}, 1, 1, false},
		{[]pb.Entry{{Term: 1, Index: 1}}, 1, 2, false},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 1, 1, true},
		// candidate higher logterm
		{[]pb.Entry{{Term: 1, Index: 1}}, 2, 1, false},
		{[]pb.Entry{{Term: 1, Index: 1}}, 2, 2, false},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 2, 1, false},
		// voter higher logterm
		{[]pb.Entry{{Term: 2, Index: 1}}, 1, 1, true},
		{[]pb.Entry{{Term: 2, Index: 1}}, 1, 2, true},
		{[]pb.Entry{{Term: 2, Index: 1}, {Term: 1, Index: 2}}, 1, 1, true},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(tt.ents)
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)

		r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgRequestVote, Term: 3, LogTerm: tt.logterm, Index: tt.index})

		msgs := r.readMessages()
		if len(msgs) != 1 {
			t.Fatalf("#%d: len(msg) = %d, want %d", i, len(msgs), 1)
		}
		m := msgs[0]
		if m.MsgType != pb.MessageType_MsgRequestVoteResponse {
			t.Errorf("#%d: msgType = %d, want %d", i, m.MsgType, pb.MessageType_MsgRequestVoteResponse)
		}
		if m.Reject != tt.wreject {
			t.Errorf("#%d: reject = %t, want %t", i, m.Reject, tt.wreject)
		}
	}
}

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
// current term are committed by counting replicas.
// Reference: section 5.4.2
func TestLeaderOnlyCommitsLogFromCurrentTerm2AB(t *testing.T) {
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		index   uint64
		wcommit uint64
	}{
		// do not commit log entries in previous terms
		{1, 0},
		{2, 0},
		// commit log in current term
		{3, 3},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(ents)
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
		r.Term = 2
		// become leader at term 3
		r.becomeCandidate()
		r.becomeLeader()
		r.readMessages()
		// propose a entry to current term
		r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})

		r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppendResponse, Term: r.Term, Index: tt.index})
		if r.RaftLog.committed != tt.wcommit {
			t.Errorf("#%d: commit = %d, want %d", i, r.RaftLog.committed, tt.wcommit)
		}
	}
}

type messageSlice []pb.Message

func (s messageSlice) Len() int           { return len(s) }
func (s messageSlice) Less(i, j int) bool { return fmt.Sprint(s[i]) < fmt.Sprint(s[j]) }
func (s messageSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func commitNoopEntry(r *Raft, s *MemoryStorage) {
	if r.State != StateLeader {
		panic("it should only be used when it is the leader")
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}

		r.sendAppend(id)
	}
	// simulate the response of MessageType_MsgAppend
	msgs := r.readMessages()
	for _, m := range msgs {
		if m.MsgType != pb.MessageType_MsgAppend || len(m.Entries) != 1 || m.Entries[0].Data != nil {
			panic("not a message to append noop entry")
		}
		r.Step(acceptAndReply(m))
	}
	// ignore further messages to refresh followers' commit index
	r.readMessages()
	s.Append(r.RaftLog.unstableEntries())
	r.RaftLog.applied = r.RaftLog.committed
	r.RaftLog.stabled = r.RaftLog.LastIndex()
}

func acceptAndReply(m pb.Message) pb.Message {
	if m.MsgType != pb.MessageType_MsgAppend {
		panic("type should be MessageType_MsgAppend")
	}
	// Note: reply message don't contain LogTerm
	return pb.Message{
		From:    m.To,
		To:      m.From,
		Term:    m.Term,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   m.Index + uint64(len(m.Entries)),
	}
}
