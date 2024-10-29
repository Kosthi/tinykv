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

	// 处理客户端提议的日志条目
	ents := []*pb.Entry{{Data: []byte("some data")}}
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: ents})

	// 日志中两个日志，一个 no-op，一个 'some data'
	if g := r.RaftLog.LastIndex(); g != li+1 {
		t.Errorf("lastIndex = %d, want %d", g, li+1)
	}
	// 'some data' 还没提交，因为还没附加条目给大多数节点
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

	// 模拟客户端提议领导者向其他节点发生附加条目请求，其他节点作出处理并回复
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})
	// 领导者在大多数节点完成日志同步时更新 commited，并向所有节点再发一个 Append，用于同步 committed
	for _, m := range r.readMessages() {
		r.Step(acceptAndReply(m))
	}

	// no-op 和 'some data' 两条日志已经提交
	if g := r.RaftLog.committed; g != li+1 {
		t.Errorf("committed = %d, want %d", g, li+1)
	}
	wents := []pb.Entry{{Index: li + 1, Term: 1, Data: []byte("some data")}}
	// 'some data' 还没应用到状态机
	if g := r.RaftLog.nextEnts(); !reflect.DeepEqual(g, wents) {
		t.Errorf("nextEnts = %+v, want %+v", g, wents)
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	// commited 更新后会再发起附加条目请求，来同步 commited
	for i, m := range msgs {
		// 节点 2、3
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
// TestLeaderAcknowledgeCommit 测试日志条目在创建该条目的领导者在大多数服务器上复制它后是否被提交。
// 参考文献：第 5.3 节
func TestLeaderAcknowledgeCommit2AB(t *testing.T) {
	tests := []struct {
		size      int
		acceptors map[uint64]bool
		wack      bool
	}{
		// 大多数节点完成附加条目后即可提交
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
// TestLeaderCommitPrecedingEntries 测试领导者在提交日志条目时，是否同时提交领导者日志中的所有先前条目，包括由前任领导者创建的条目。
// 此外，它还将条目按日志顺序应用于其本地状态机。
// 参考文献：第 5.3 节
func TestLeaderCommitPrecedingEntries2AB(t *testing.T) {
	tests := [][]pb.Entry{
		// 存储中的先前条目，在提交时要一起提交
		{},
		{{Term: 2, Index: 1}},
		{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
		{{Term: 1, Index: 1}},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		// 持久化日志进存储中
		storage.Append(tt)
		// 日志初始化过程会从存储中读取日志
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
// TestFollowerCommitEntry 测试一旦追随者得知一个日志条目已被提交，它是否将该条目按日志顺序应用于其本地状态机。
// 参考文献：第 5.3 节
func TestFollowerCommitEntry2AB(t *testing.T) {
	// 如果领导者已经提交了日志条目，跟随者也要跟着一起提交
	// 也就是跟随者要跟领导者同步已经提交的日志条目
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
// TestFollowerCheckMessageType_MsgAppend 测试如果追随者在其日志中未找到与 AppendEntries RPC 中的索引和任期相同的条目，则它会拒绝新的条目。
// 否则，它会回复接受追加条目。
// 参考文献：第 5.3 节
func TestFollowerCheckMessageType_MsgAppend2AB(t *testing.T) {
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		term    uint64
		index   uint64
		wreject bool
	}{
		// match with committed entries
		// 与已提交条目匹配
		{0, 0, false},
		{ents[0].Term, ents[0].Index, false},
		// match with uncommitted entries
		// 与未提交条目匹配
		{ents[1].Term, ents[1].Index, false},

		// unmatch with existing entry
		// 与现有条目不匹配
		{ents[0].Term, ents[1].Index, true},
		// unexisting entry
		// 不存在的条目
		{ents[1].Term + 1, ents[1].Index + 1, true},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(ents)
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
		r.RaftLog.committed = 1
		r.becomeFollower(2, 2)
		msgs := r.readMessages() // clear message

		// 实际上这里并没有数据被附加，只是做了索引和任期的检查
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
// TestFollowerAppendEntries 测试当 AppendEntries RPC 有效时，追随者将删除现有的冲突条目及其后面的所有条目，
// 并附加任何尚不存在于日志中的新条目。此外，它还将新条目写入稳定存储。
// 参考文献：第 5.3 节
func TestFollowerAppendEntries2AB(t *testing.T) {
	tests := []struct {
		index, term uint64
		lterm       uint64
		ents        []*pb.Entry
		wents       []*pb.Entry
		wunstable   []*pb.Entry
	}{
		{
			2, 2, 3, // 附加条目请求的prevIndex，prevTerm，leaderTerm
			[]*pb.Entry{{Term: 3, Index: 3}},                                           // 附加条目请求的日志条目
			[]*pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}}, // 期望节点附加条目后的日志内容
			[]*pb.Entry{{Term: 3, Index: 3}},                                           // 附加到了这个节点，但未附加到大多数节点的不稳定的条目
		},
		{
			1, 1, 4,
			[]*pb.Entry{{Term: 3, Index: 2}, {Term: 4, Index: 3}}, // 检测到冲突会删除该日志条目及以后的日志条目
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
// TestLeaderSyncFollowerLog 测试领导者是否能够使跟随者的日志与其自身保持一致。
// 参考：第 5.3 节，图 7
func TestLeaderSyncFollowerLog2AB(t *testing.T) {
	// 领导者节点 1 的日志条目
	ents := []pb.Entry{
		{},
		{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
		{Term: 4, Index: 4}, {Term: 4, Index: 5},
		{Term: 5, Index: 6}, {Term: 5, Index: 7},
		{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10},
	}
	term := uint64(8)
	tests := [][]pb.Entry{
		// 跟随者节点 2 的日志条目，冲突的会发生截断，最后跟随者的日志条目会与领导者完全一致
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
		// 必须有一个三节点集群。
		// 第二个节点的日志可能比第一个节点更新，因此第一个节点需要来自第三个节点的投票才能成为领导者。
		n := newNetwork(lead, follower, nopStepper)
		// 领导者发起选举请求
		n.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
		// The election occurs in the term after the one we loaded with
		// lead's term and committed index setted up above.
		// 选举发生在我们用领导者的任期和上述设置的已提交索引加载的任期之后。
		// 节点 3 返回赞成票，节点 1 被选举为领导者（如果已经是领导者则会忽略这条消息）向其他节点发起 no-op 附加条目请求，用于其他节点日志同步
		n.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgRequestVoteResponse, Term: term + 1})

		// 领导者节点 1 收到客户端数据，向其他节点发起附加条目请求
		n.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})

		if g := diffu(ltoa(lead.RaftLog), ltoa(follower.RaftLog)); g != "" {
			t.Errorf("#%d: log diff:\n%s", i, g)
		}
	}
}

// TestVoteRequest tests that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
// TestVoteRequest 测试投票请求是否包含候选人日志的信息
// 并且是否发送到所有其他节点。
// 参考文献：第 5.4.1 节
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
		// 向这个跟随者节点附加日志条目
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
			// 检查投票请求中是否包含了候选人最后一个日志条目的索引和任期
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
// TestVoter 测试投票者在其自己的日志比候选者的日志更新时拒绝投票
// 参考文献：第 5.4.1 节
func TestVoter2AB(t *testing.T) {
	tests := []struct {
		ents    []pb.Entry // 投票者节点 1 的日志条目
		logterm uint64     // 请求投票的候选人节点 2 的日志任期
		index   uint64     // 请求投票的候选人节点 2 的日志索引

		wreject bool // 投票者是否拒绝这次投票请求
	}{
		// 看两者的最后一个日志条目，先比任期，再比索引，谁大谁更新
		// same logterm
		// 一样的日志任期
		{[]pb.Entry{{Term: 1, Index: 1}}, 1, 1, false},
		{[]pb.Entry{{Term: 1, Index: 1}}, 1, 2, false},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 1, 1, true},
		// candidate higher logterm
		// 候选者有更高的任期
		{[]pb.Entry{{Term: 1, Index: 1}}, 2, 1, false},
		{[]pb.Entry{{Term: 1, Index: 1}}, 2, 2, false},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 2, 1, false},
		// voter higher logterm
		// 投票者有更高的日志任期
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
// TestLeaderOnlyCommitsLogFromCurrentTerm 测试只有来自领导者当前任期的日志条目会被提交，通过计数副本来验证。
// 参考文献：第 5.4.2 节
func TestLeaderOnlyCommitsLogFromCurrentTerm2AB(t *testing.T) {
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		index   uint64 // 之前或当前任期对应的索引
		wcommit uint64 // 是否应该提交
	}{
		// do not commit log entries in previous terms
		// 不要提交前一个任期的日志条目
		{1, 0},
		{2, 0},
		// commit log in current term
		// 提交当前任期的日志条目
		{3, 3},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(ents)
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
		r.Term = 2
		// become leader at term 3
		// 在第 3 任期成为领导者
		r.becomeCandidate()
		r.becomeLeader()
		r.readMessages()
		// propose a entry to current term
		// 向当前任期提议一个条目
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

// 确保领导者能够发送一个空操作条目，并确保所有跟随者能够正确处理和更新他们的日志状态。
// 它们在 Raft 算法中是非常重要的，因为它们用于保持领导者与跟随者之间的一致性和同步性。
// 通过提交 no-op 条目，领导者可以安全地推进其任期和日志，而不引入新的实际数据，同时也为其他节点提供一致的视图。
func commitNoopEntry(r *Raft, s *MemoryStorage) {
	// 检查领导者状态，只有领导者能提交空条目
	if r.State != StateLeader {
		panic("it should only be used when it is the leader")
	}
	// 发送 AppendEntries
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
	// simulate the response of MessageType_MsgAppend
	// 模拟 MessageType_MsgAppend 类型消息的响应
	msgs := r.readMessages()
	for _, m := range msgs {
		if m.MsgType != pb.MessageType_MsgAppend || len(m.Entries) != 1 || m.Entries[0].Data != nil {
			panic("not a message to append noop entry")
		}
		r.Step(acceptAndReply(m))
	}
	// ignore further messages to refresh followers' commit index
	// 忽略进一步的消息以刷新跟随者的提交索引
	r.readMessages()
	// 还未持久化到稳定存储的条目）追加到存储中
	s.Append(r.RaftLog.unstableEntries())
	// 设置状态机应用的索引 applied 为当前提交的索引，因为多数节点都已经提交
	r.RaftLog.applied = r.RaftLog.committed
	// 设置稳定的索引 (stabled) 更新为当前日志的最后一个索引
	r.RaftLog.stabled = r.RaftLog.LastIndex()
}

func acceptAndReply(m pb.Message) pb.Message {
	if m.MsgType != pb.MessageType_MsgAppend {
		panic("type should be MessageType_MsgAppend")
	}
	// Note: reply message don't contain LogTerm
	// 注意：回复消息不包含 LogTerm
	return pb.Message{
		From:    m.To,
		To:      m.From,
		Term:    m.Term,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   m.Index + uint64(len(m.Entries)), // 将当前的索引增加已包含的条目数，以表示成功的追加
	}
}
