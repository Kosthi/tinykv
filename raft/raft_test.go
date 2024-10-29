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
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// returns a new MemoryStorage with only ents filled
// 返回一个新的 MemoryStorage 实例，且只填写了 ents
func newMemoryStorageWithEnts(ents []pb.Entry) *MemoryStorage {
	return &MemoryStorage{
		ents:     ents,
		snapshot: pb.Snapshot{Metadata: &pb.SnapshotMetadata{ConfState: &pb.ConfState{}}},
	}
}

// nextEnts returns the appliable entries and updates the applied index
// nextEnts 返回可以应用的日志条目，并更新已应用的索引
func nextEnts(r *Raft, s *MemoryStorage) (ents []pb.Entry) {
	// Transfer all unstable entries to "stable" storage.
	// 将所有不稳定的条目转移到“稳定”存储中
	s.Append(r.RaftLog.unstableEntries())
	r.RaftLog.stabled = r.RaftLog.LastIndex()

	ents = r.RaftLog.nextEnts()
	r.RaftLog.applied = r.RaftLog.committed
	return ents
}

type stateMachine interface {
	Step(m pb.Message) error
	readMessages() []pb.Message
}

func (r *Raft) readMessages() []pb.Message {
	msgs := r.msgs
	r.msgs = make([]pb.Message, 0)

	return msgs
}

// 测试领导者是否能正确附加日志条目（包括 no-op 日志条目）
func TestProgressLeader2AB(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, NewMemoryStorage())
	r.becomeCandidate()
	r.becomeLeader()

	// Send proposals to r1. The first 5 entries should be appended to the log.
	// 将提议发送给 r1。前 5 个条目应附加到日志中。
	propMsg := pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("foo")}}}
	for i := 0; i < 5; i++ {
		if pr := r.Prs[r.id]; pr.Match != uint64(i+1) || pr.Next != pr.Match+1 {
			t.Errorf("unexpected progress %v", pr)
		}
		if err := r.Step(propMsg); err != nil {
			t.Fatalf("proposal resulted in error: %v", err)
		}
	}
}

// TestLeaderElection2AA 测试选举是否能正确选出领导者
func TestLeaderElection2AA(t *testing.T) {
	var cfg func(*Config)
	tests := []struct {
		*network
		state   StateType
		expTerm uint64
	}{
		{newNetworkWithConfig(cfg, nil, nil, nil), StateLeader, 1},                         // 赢得 3/3 枚选票，节点 1 成为领导者
		{newNetworkWithConfig(cfg, nil, nil, nopStepper), StateLeader, 1},                  // 赢得 2/3 枚选票，节点 1 成为领导者
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper), StateCandidate, 1},        // 赢得 1/3 枚选票，其他节点宕机，节点 1 保持为候选者
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper, nil), StateCandidate, 1},   // 赢得 2/4 枚选票，其他节点宕机，节点 1 保持为候选者
		{newNetworkWithConfig(cfg, nil, nopStepper, nopStepper, nil, nil), StateLeader, 1}, // 赢得 3/5 枚选票，其他节点宕机，节点 1 成为领导者
	}

	for i, tt := range tests {
		// 节点 1 发起选举，因为该测试没有其他节点发起选举，节点 1 会在赢得多数票的情况下成为领导者
		tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
		sm := tt.network.peers[1].(*Raft)
		if sm.State != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, sm.State, tt.state)
		}
		if g := sm.Term; g != tt.expTerm {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.expTerm)
		}
	}
}

// testLeaderCycle verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections work when not
// starting from a clean slate (as they do in TestLeaderElection)
// testLeaderCycle 验证集群中的每个节点都能轮流竞选和当选为领导者。
// 这确保了在不是从干净的状态开始时，选举功能正常（如在 TestLeaderElection 中所做的那样）。
func TestLeaderCycle2AA(t *testing.T) {
	var cfg func(*Config)
	n := newNetworkWithConfig(cfg, nil, nil, nil)
	for campaignerID := uint64(1); campaignerID <= 3; campaignerID++ {
		// 节点 campaignerID 发起选举，会成为领导者，而其他节点成为跟随者
		n.send(pb.Message{From: campaignerID, To: campaignerID, MsgType: pb.MessageType_MsgHup})

		for _, peer := range n.peers {
			sm := peer.(*Raft)
			if sm.id == campaignerID && sm.State != StateLeader {
				t.Errorf("campaigning node %d state = %v, want StateLeader",
					sm.id, sm.State)
			} else if sm.id != campaignerID && sm.State != StateFollower {
				t.Errorf("after campaign of node %d, "+
					"node %d had state = %v, want StateFollower",
					campaignerID, sm.id, sm.State)
			}
		}
	}
}

// TestLeaderElectionOverwriteNewerLogs tests a scenario in which a
// newly-elected leader does *not* have the newest (i.e. highest term)
// log entries, and must overwrite higher-term log entries with
// lower-term ones.
// TestLeaderElectionOverwriteNewerLogs 测试一个场景，其中一位新选出的领导者没有最新的（即，最高任期）日志条目，
// 并且必须用较低任期的条目覆盖较高任期的日志条目。
func TestLeaderElectionOverwriteNewerLogs2AB(t *testing.T) {
	cfg := func(c *Config) {
		c.peers = idsBySize(5)
	}
	// This network represents the results of the following sequence of
	// events:
	// - Node 1 won the election in term 1.
	// - Node 1 replicated a log entry to node 2 but died before sending
	//   it to other nodes.
	// - Node 3 won the second election in term 2.
	// - Node 3 wrote an entry to its logs but died without sending it
	//   to any other nodes.
	//
	// At this point, nodes 1, 2, and 3 all have uncommitted entries in
	// their logs and could win an election at term 3. The winner's log
	// entry overwrites the losers'. (TestLeaderSyncFollowerLog tests
	// the case where older log entries are overwritten, so this test
	// focuses on the case where the newer entries are lost).
	// 该网络代表以下事件序列的结果：
	// - 节点 1 在任期 1 中赢得了选举。
	// - 节点 1 将日志条目复制到节点 2，但在发送给其他节点之前死掉了。
	// - 节点 3 在任期 2 中赢得了第二次选举。
	// - 节点 3 向其日志写入了一个条目，但在发送给其他节点之前死掉了。
	//
	// 此时，节点 1、2 和 3 的日志中都有未提交的条目，并且在任期 3 中可能会赢得选举。
	// 胜者的日志条目将覆盖失败者的日志条目。(TestLeaderSyncFollowerLog 测试
	// 旧日志条目被覆盖的情况，因此此测试侧重于较新条目丢失的情况)。
	n := newNetworkWithConfig(cfg,
		entsWithConfig(cfg, 1, 1),     // 节点 1: 赢得第一次选举
		entsWithConfig(cfg, 2, 1),     // 节点 2: 从节点 1 获取日志
		entsWithConfig(cfg, 3, 2),     // 节点 3: 赢得第二次选举
		votedWithConfig(cfg, 4, 3, 2), // 节点 4: 投票但未获取日志（任期为 2，最后一条日志任期为 0）
		votedWithConfig(cfg, 5, 3, 2)) // 节点 5: 投票但未获取日志（任期为 2，最后一条日志任期为 0）

	// Node 1 campaigns. The election fails because a quorum of nodes
	// know about the election that already happened at term 2. Node 1's
	// term is pushed ahead to 2.
	// 节点 1 发起竞选。由于有法定人数的节点知道在任期 2 中已经发生的选举，因此选举失败。
	// 节点 1 的任期被推迟到 2。
	n.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	sm1 := n.peers[1].(*Raft)
	if sm1.State != StateFollower {
		t.Errorf("state = %s, want StateFollower", sm1.State)
	}
	if sm1.Term != 2 {
		t.Errorf("term = %d, want 2", sm1.Term)
	}

	// Node 1 campaigns again with a higher term. This time it succeeds.
	// 节点 1 再次以更高的任期进行竞选。这次它成功了。
	// 附加条目请求使得较低任期的条目覆盖了较高任期的日志条目
	n.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	if sm1.State != StateLeader {
		t.Errorf("state = %s, want StateLeader", sm1.State)
	}
	if sm1.Term != 3 {
		t.Errorf("term = %d, want 3", sm1.Term)
	}

	// Now all nodes agree on a log entry with term 1 at index 1 (and
	// term 3 at index 2).
	// 现在所有节点在索引 1 上达成了一个包含任期 1 的日志条目（以及在索引 2 上包含任期 3 的日志条目）。
	for i := range n.peers {
		sm := n.peers[i].(*Raft)
		entries := sm.RaftLog.allEntries()
		if len(entries) != 2 {
			t.Fatalf("node %d: len(entries) == %d, want 2", i, len(entries))
		}
		if entries[0].Term != 1 {
			t.Errorf("node %d: term at index 1 == %d, want 1", i, entries[0].Term)
		}
		if entries[1].Term != 3 {
			t.Errorf("node %d: term at index 2 == %d, want 3", i, entries[1].Term)
		}
	}
}

// TestVoteFromAnyState2AA 测试选举中不同状态的节点收到投票请求，能否正确改变状态并按照先来先服务的原则投票
func TestVoteFromAnyState2AA(t *testing.T) {
	vt := pb.MessageType_MsgRequestVote
	vt_resp := pb.MessageType_MsgRequestVoteResponse
	for st := StateType(0); st <= StateLeader; st++ {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		r.Term = 1

		switch st {
		case StateFollower:
			r.becomeFollower(r.Term, 3)
		case StateCandidate:
			r.becomeCandidate()
		case StateLeader:
			r.becomeCandidate()
			r.becomeLeader()
		}
		r.readMessages() // clear message

		// Note that setting our state above may have advanced r.Term
		// past its initial value.
		// 注意，上面设置我们的状态可能已经将 r.Term 推进到其初始值之后。
		newTerm := r.Term + 1

		// 选举投票请求，从节点 2 发给节点 1
		msg := pb.Message{
			From:    2,
			To:      1,
			MsgType: vt,
			Term:    newTerm,
			LogTerm: newTerm,
			Index:   42,
		}
		// 当前节点处理选举投票请求
		if err := r.Step(msg); err != nil {
			t.Errorf("%s,%s: Step failed: %s", vt, st, err)
		}
		// 当前节点应该对选票投票请求做出回复
		if len(r.msgs) != 1 {
			t.Errorf("%s,%s: %d response messages, want 1: %+v", vt, st, len(r.msgs), r.msgs)
		} else {
			resp := r.msgs[0]
			// 消息类型应该是选举投票请求回复
			if resp.MsgType != vt_resp {
				t.Errorf("%s,%s: response message is %s, want %s",
					vt, st, resp.MsgType, vt_resp)
			}
			// 按照先来先服务原则，不应该出现拒绝
			if resp.Reject {
				t.Errorf("%s,%s: unexpected rejection", vt, st)
			}
		}

		// If this was a vote, we reset our state and term.
		// 如果这是一次投票，我们会重置我们的状态和任期。
		// 当前节点状态为初始状态跟随者
		if r.State != StateFollower {
			t.Errorf("%s,%s: state %s, want %s", vt, st, r.State, StateFollower)
		}
		// 当前节点任期要保持与新任期一致
		if r.Term != newTerm {
			t.Errorf("%s,%s: term %d, want %d", vt, st, r.Term, newTerm)
		}
		// 当前节点选票一定是投给了节点 2
		if r.Vote != 2 {
			t.Errorf("%s,%s: vote %d, want 2", vt, st, r.Vote)
		}
	}
}

// 测试集群中每个节点能否正确复制日志并提交
func TestLogReplication2AB(t *testing.T) {
	tests := []struct {
		*network
		msgs       []pb.Message
		wcommitted uint64
	}{
		{
			newNetwork(nil, nil, nil),
			[]pb.Message{
				{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}},
			},
			2,
		},
		{
			newNetwork(nil, nil, nil),
			[]pb.Message{
				{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}},
				{From: 2, To: 2, MsgType: pb.MessageType_MsgHup},
				{From: 1, To: 2, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}},
			},
			4,
		},
	}

	for i, tt := range tests {
		// 选举出领导者
		tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

		// 执行测试，处理客户端提议和新的选举
		for _, m := range tt.msgs {
			tt.send(m)
		}

		// 检查集群中每个节点是否都复制了日志并提交了
		for j, x := range tt.network.peers {
			sm := x.(*Raft)

			if sm.RaftLog.committed != tt.wcommitted {
				t.Errorf("#%d.%d: committed = %d, want %d", i, j, sm.RaftLog.committed, tt.wcommitted)
			}

			ents := []pb.Entry{}
			for _, e := range nextEnts(sm, tt.network.storage[j]) {
				if e.Data != nil {
					ents = append(ents, e)
				}
			}
			props := []pb.Message{}
			for _, m := range tt.msgs {
				if m.MsgType == pb.MessageType_MsgPropose {
					props = append(props, m)
				}
			}
			for k, m := range props {
				if !bytes.Equal(ents[k].Data, m.Entries[0].Data) {
					t.Errorf("#%d.%d: data = %d, want %d", i, j, ents[k].Data, m.Entries[0].Data)
				}
			}
		}
	}
}

// 测试单节点日志条目提交
func TestSingleNodeCommit2AB(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*Raft)
	if sm.RaftLog.committed != 3 {
		t.Errorf("committed = %d, want %d", sm.RaftLog.committed, 3)
	}
}

// TestCommitWithoutNewTermEntry tests the entries could be committed
// when leader changes with noop entry and no new proposal comes in.
// TestCommitWithoutNewTermEntry 测试在领导者变化时，能够提交条目
// 当没有新的提案到达时，系统可以提交一个 no-op 条目。
func TestCommitWithoutNewTermEntry2AB(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	// 0 cannot reach 2,3,4
	// 节点 0 无法到达节点 3、4、5
	tt.cut(1, 3)
	tt.cut(1, 4)
	tt.cut(1, 5)

	// 节点 1 和 2 同步了最新的日志条目
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*Raft)
	if sm.RaftLog.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.RaftLog.committed, 1)
	}

	// network recovery
	// 网络恢复
	tt.recover()

	// elect 2 as the new leader with term 2
	// after append a ChangeTerm entry from the current term, all entries
	// should be committed
	// 将节点 2 选举为新的领导者，任期为 2。
	// 在从当前任期追加一个 ChangeTerm 条目之后，所有条目都应该被提交。
	tt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup})

	if sm.RaftLog.committed != 4 {
		t.Errorf("committed = %d, want %d", sm.RaftLog.committed, 4)
	}
}

// TestCommitWithHeartbeat tests leader can send log
// to follower when it received a heartbeat response
// which indicate it doesn't have update-to-date log
// TestCommitWithHeartbeat 测试领导者在收到心跳响应后能够
// 向跟随者发送日志，该响应表明跟随者没有更新到最新的日志。
// 实际上在收到心跳响应后，领导者检查该跟随者的日志是否落后，落后则发送日志
func TestCommitWithHeartbeat2AB(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	// 节点 1 成为领导者
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	// isolate node 5
	// 隔离节点 5
	tt.isolate(5)
	// 向节点 2、3、4 同步日志条目
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[5].(*Raft)
	if sm.RaftLog.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.RaftLog.committed, 1)
	}

	// network recovery
	// 网络恢复
	tt.recover()

	// leader broadcast heartbeat
	// 领导者广播心跳
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})

	if sm.RaftLog.committed != 3 {
		t.Errorf("committed = %d, want %d", sm.RaftLog.committed, 3)
	}
}

func TestDuelingCandidates2AB(t *testing.T) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())

	nt := newNetwork(a, b, c)
	nt.cut(1, 3)

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	nt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgHup})

	// 1 becomes leader since it receives votes from 1 and 2
	// 1 成为领导者，因为它收到了来自 1 和 2 的投票。
	sm := nt.peers[1].(*Raft)
	if sm.State != StateLeader {
		t.Errorf("state = %s, want %s", sm.State, StateLeader)
	}

	// 3 stays as candidate since it receives a vote from 3 and a rejection from 2
	// 3 仍然作为候选者，因为它仅获得了来自 3 的投票，并且收到了来自 2 的拒绝投票。
	sm = nt.peers[3].(*Raft)
	if sm.State != StateCandidate {
		t.Errorf("state = %s, want %s", sm.State, StateCandidate)
	}

	nt.recover()

	// candidate 3 now increases its term and tries to vote again
	// we expect it to disrupt the leader 1 since it has a higher term
	// 3 will be follower again since both 1 and 2 rejects its vote request since 3 does not have a long enough log
	// 候选者 3 现在增加了它的任期并尝试再次投票。
	// 我们预计它会干扰领导者 1，因为它的任期较高。
	// 但是，3 将再次成为追随者，因为节点 1 和 2 拒绝了它的投票请求，原因是 3 的日志不够长。
	nt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgHup})

	wlog := newLog(newMemoryStorageWithEnts([]pb.Entry{{}, {Data: nil, Term: 1, Index: 1}}))
	wlog.committed = 1
	tests := []struct {
		sm      *Raft
		state   StateType
		term    uint64
		raftLog *RaftLog
	}{
		{a, StateFollower, 2, wlog},
		{b, StateFollower, 2, wlog},
		{c, StateFollower, 2, newLog(NewMemoryStorage())},
	}

	for i, tt := range tests {
		if g := tt.sm.State; g != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, g, tt.state)
		}
		if g := tt.sm.Term; g != tt.term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.term)
		}
		base := ltoa(tt.raftLog)
		if sm, ok := nt.peers[1+uint64(i)].(*Raft); ok {
			l := ltoa(sm.RaftLog)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

// 测试各个节点的日志同步和 committed 能否正确更新
func TestCandidateConcede2AB(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	tt.isolate(1)

	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	tt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgHup})

	// heal the partition
	// 恢复网络分区
	tt.recover()
	// send heartbeat; reset wait
	// 发送心跳；重置等待时间
	tt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgBeat})

	data := []byte("force follower")
	// send a proposal to 3 to flush out a MessageType_MsgAppend to 1
	// 向 3 发送提议，将 MessageType_MsgAppend 刷新到 1
	tt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: data}}})
	// send heartbeat; flush out commit
	// 发送心跳；处理提交
	tt.send(pb.Message{From: 3, To: 3, MsgType: pb.MessageType_MsgBeat})

	a := tt.peers[1].(*Raft)
	if g := a.State; g != StateFollower {
		t.Errorf("state = %s, want %s", g, StateFollower)
	}
	if g := a.Term; g != 1 {
		t.Errorf("term = %d, want %d", g, 1)
	}
	wlog := newLog(newMemoryStorageWithEnts([]pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Data: data}}))
	wlog.committed = 2
	wantLog := ltoa(wlog)
	for i, p := range tt.peers {
		if sm, ok := p.(*Raft); ok {
			l := ltoa(sm.RaftLog)
			if g := diffu(wantLog, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

// TestSingleNodeCandidate2AA 测试单节点发起选举直接成为领导者
func TestSingleNodeCandidate2AA(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	sm := tt.peers[1].(*Raft)
	if sm.State != StateLeader {
		t.Errorf("state = %d, want %d", sm.State, StateLeader)
	}
}

// 测试来自过期的领导者的附加日志条目请求是否会被正确忽略
func TestOldMessages2AB(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	// make 0 leader @ term 3
	// 在第 3 个任期内将节点 1 设为领导者
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	tt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup})
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	// pretend we're an old leader trying to make progress; this entry is expected to be ignored.
	// 假装我们是一个旧的领导者，试图取得进展；此条目预期会被忽略。
	tt.send(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppend, Term: 2, Entries: []*pb.Entry{{Index: 3, Term: 2}}})
	// commit a new entry
	// 提交一个新的条目
	tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}})

	// 检查日志条目是否被正确复制到各个节点并提交
	ilog := newLog(
		newMemoryStorageWithEnts([]pb.Entry{
			{}, {Data: nil, Term: 1, Index: 1},
			{Data: nil, Term: 2, Index: 2}, {Data: nil, Term: 3, Index: 3},
			{Data: []byte("somedata"), Term: 3, Index: 4},
		}))
	ilog.committed = 4
	base := ltoa(ilog)
	for i, p := range tt.peers {
		if sm, ok := p.(*Raft); ok {
			l := ltoa(sm.RaftLog)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

// 测试客户端提议的日志是否能被领导者正确复制到大多数节点并提交，或者被候选者忽略
func TestProposal2AB(t *testing.T) {
	tests := []struct {
		*network
		success bool
	}{
		{newNetwork(nil, nil, nil), true},
		{newNetwork(nil, nil, nopStepper), true},
		{newNetwork(nil, nopStepper, nopStepper), false},
		{newNetwork(nil, nopStepper, nopStepper, nil), false},
		{newNetwork(nil, nopStepper, nopStepper, nil, nil), true},
	}

	for i, tt := range tests {
		data := []byte("somedata")

		// promote 1 to become leader
		// 将节点 1 提升为领导者，如果大多数节点宕机可能会失败
		tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
		// 接收客户端提议提交一个新日志条目
		tt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: data}}})

		// 如果节点 1 没有拿到足够的选票，依然是候选者，那么会忽略客户端的提议，日志为空，其他节点也是
		wantLog := newLog(NewMemoryStorage())
		if tt.success {
			wantLog = newLog(newMemoryStorageWithEnts([]pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Data: data}}))
			wantLog.committed = 2
		}
		base := ltoa(wantLog)
		for j, p := range tt.peers {
			if sm, ok := p.(*Raft); ok {
				l := ltoa(sm.RaftLog)
				if g := diffu(base, l); g != "" {
					t.Errorf("#%d.%d: diff:\n%s", i, j, g)
				}
			}
		}
		sm := tt.network.peers[1].(*Raft)
		if g := sm.Term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

// TestHandleMessageType_MsgAppend ensures:
//  1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
//  2. If an existing entry conflicts with a new one (same index but different terms),
//     delete the existing entry and all that follow it; append any new entries not already in the log.
//  3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
//
// TestHandleMessageType_MsgAppend 确保：
//  1. 如果日志在 prevLogIndex 没有包含一个与 prevLogTerm 匹配的条目，则返回 false。
//  2. 如果现有条目与新条目冲突（索引相同但任期不同），
//     则删除现有条目及其后面的所有条目；追加任何未在日志中的新条目。
//  3. 如果 leaderCommit > commitIndex，设置 commitIndex = min(leaderCommit, 最后一个新条目的索引)。
func TestHandleMessageType_MsgAppend2AB(t *testing.T) {
	tests := []struct {
		m       pb.Message
		wIndex  uint64
		wCommit uint64
		wReject bool
	}{
		// Ensure 1
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 3, LogTerm: 3, Index: 2, Commit: 3}, 2, 0, true}, // previous log mismatch
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 3, LogTerm: 3, Index: 3, Commit: 3}, 2, 0, true}, // previous log non-exist

		// Ensure 2
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 1, Index: 1, Commit: 1}, 2, 1, false},
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 0, Index: 0, Commit: 1, Entries: []*pb.Entry{{Index: 1, Term: 2}}}, 1, 1, false},
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 2, Index: 2, Commit: 3, Entries: []*pb.Entry{{Index: 3, Term: 2}, {Index: 4, Term: 2}}}, 4, 3, false},
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 2, Index: 2, Commit: 4, Entries: []*pb.Entry{{Index: 3, Term: 2}}}, 3, 3, false},
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 1, Index: 1, Commit: 4, Entries: []*pb.Entry{{Index: 2, Term: 2}}}, 2, 2, false},

		// Ensure 3
		// 是和领导者的 commitIndex 与附加过来的最后一条日志条目索引比较
		// 跟随者可能有错误的历史日志条目，所以通过领导者实际发送过来的日志索引来确定那些日志被真正地提交了
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 1, Index: 1, Commit: 3}, 2, 1, false},                                            // match entry 1, commit up to last new entry 1
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 1, Index: 1, Commit: 3, Entries: []*pb.Entry{{Index: 2, Term: 2}}}, 2, 2, false}, // match entry 1, commit up to last new entry 2
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 2, Index: 2, Commit: 3}, 2, 2, false},                                            // match entry 2, commit up to last new entry 2
		{pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: 2, Index: 2, Commit: 4}, 2, 2, false},                                            // commit up to log.last()
	}

	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append([]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}})
		sm := newTestRaft(1, []uint64{1}, 10, 1, storage)
		// 成为跟随者 任期 2
		sm.becomeFollower(2, None)
		// 收到领导者附加日志条目请求
		sm.handleAppendEntries(tt.m)
		if sm.RaftLog.LastIndex() != tt.wIndex {
			t.Errorf("#%d: lastIndex = %d, want %d", i, sm.RaftLog.LastIndex(), tt.wIndex)
		}
		if sm.RaftLog.committed != tt.wCommit {
			t.Errorf("#%d: committed = %d, want %d", i, sm.RaftLog.committed, tt.wCommit)
		}
		m := sm.readMessages()
		if len(m) != 1 {
			t.Fatalf("#%d: msg = nil, want 1", i)
		}
		if m[0].Reject != tt.wReject {
			t.Errorf("#%d: reject = %v, want %v", i, m[0].Reject, tt.wReject)
		}
	}
}

// 测试选举中投票者是否只投给一位候选者，且会正确地把选票投给日志和自己一样或更新的候选者
func TestRecvMessageType_MsgRequestVote2AB(t *testing.T) {
	msgType := pb.MessageType_MsgRequestVote
	msgRespType := pb.MessageType_MsgRequestVoteResponse
	tests := []struct {
		state          StateType
		index, logTerm uint64
		voteFor        uint64
		wreject        bool
	}{
		{StateFollower, 0, 0, None, true},
		{StateFollower, 0, 1, None, true},
		{StateFollower, 0, 2, None, true},
		{StateFollower, 0, 3, None, false},

		{StateFollower, 1, 0, None, true},
		{StateFollower, 1, 1, None, true},
		{StateFollower, 1, 2, None, true},
		{StateFollower, 1, 3, None, false},

		{StateFollower, 2, 0, None, true},
		{StateFollower, 2, 1, None, true},
		{StateFollower, 2, 2, None, false},
		{StateFollower, 2, 3, None, false},

		{StateFollower, 3, 0, None, true},
		{StateFollower, 3, 1, None, true},
		{StateFollower, 3, 2, None, false},
		{StateFollower, 3, 3, None, false},

		{StateFollower, 3, 2, 2, false},
		{StateFollower, 3, 2, 1, true},

		{StateLeader, 3, 3, 1, true},
		{StateCandidate, 3, 3, 1, true},
	}

	max := func(a, b uint64) uint64 {
		if a > b {
			return a
		}
		return b
	}

	for i, tt := range tests {
		sm := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
		sm.State = tt.state
		sm.Vote = tt.voteFor
		sm.RaftLog = newLog(newMemoryStorageWithEnts([]pb.Entry{{}, {Index: 1, Term: 2}, {Index: 2, Term: 2}}))

		// raft.Term is greater than or equal to raft.RaftLog.lastTerm. In this
		// test we're only testing MessageType_MsgRequestVote responses when the campaigning node
		// has a different raft log compared to the recipient node.
		// Additionally we're verifying behaviour when the recipient node has
		// already given out its vote for its current term. We're not testing
		// what the recipient node does when receiving a message with a
		// different term number, so we simply initialize both term numbers to
		// be the same.
		// raft.Term 大于或等于 raft.RaftLog.lastTerm。在这个测试中，我们只测试当竞选节点与接收节点的 raft 日志不同时
		// MessageType_MsgRequestVote 的响应。另一个重点是验证当接收节点已经在当前任期内投出了选票时的行为。我们并不测试
		// 当接收节点收到具有不同任期编号的消息时的处理，因此我们简单地将两个任期编号初始化为相同。
		lterm, err := sm.RaftLog.Term(sm.RaftLog.LastIndex())
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		term := max(lterm, tt.logTerm)
		// 简单地将两个任期编号初始化为相同
		sm.Term = term
		sm.Step(pb.Message{MsgType: msgType, Term: term, From: 2, Index: tt.index, LogTerm: tt.logTerm})

		// 比较投票者的行为是否与预期一致
		msgs := sm.readMessages()
		if g := len(msgs); g != 1 {
			t.Fatalf("#%d: len(msgs) = %d, want 1", i, g)
			continue
		}
		if g := msgs[0].MsgType; g != msgRespType {
			t.Errorf("#%d, m.MsgType = %v, want %v", i, g, msgRespType)
		}
		if g := msgs[0].Reject; g != tt.wreject {
			t.Errorf("#%d, m.Reject = %v, want %v", i, g, tt.wreject)
		}
	}
}

// 测试节点收到更高任期的请求时，是否会正确回退为跟随者，并调整当前的任期和领导者
// 如果收到来自更高任期的候选者的投票请求，当前节点会转变为跟随者，更新任期，领导者为空
// 如果收到来自更高任期的领导者的附加日志条目请求，当前节点会转变为跟随者，更新任期，并把请求节点视为领导者
func TestAllServerStepdown2AB(t *testing.T) {
	tests := []struct {
		state StateType

		wstate StateType
		wterm  uint64
		windex uint64
	}{
		{StateFollower, StateFollower, 3, 0},
		{StateCandidate, StateFollower, 3, 0},
		{StateLeader, StateFollower, 3, 1},
	}

	tmsgTypes := [...]pb.MessageType{pb.MessageType_MsgRequestVote, pb.MessageType_MsgAppend}
	tterm := uint64(3)

	for i, tt := range tests {
		sm := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		switch tt.state {
		case StateFollower:
			sm.becomeFollower(1, None)
		case StateCandidate:
			sm.becomeCandidate()
		case StateLeader:
			sm.becomeCandidate()
			sm.becomeLeader()
		}

		for j, msgType := range tmsgTypes {
			sm.Step(pb.Message{From: 2, MsgType: msgType, Term: tterm, LogTerm: tterm})

			if sm.State != tt.wstate {
				t.Errorf("#%d.%d state = %v , want %v", i, j, sm.State, tt.wstate)
			}
			if sm.Term != tt.wterm {
				t.Errorf("#%d.%d term = %v , want %v", i, j, sm.Term, tt.wterm)
			}
			if sm.RaftLog.LastIndex() != tt.windex {
				t.Errorf("#%d.%d index = %v , want %v", i, j, sm.RaftLog.LastIndex(), tt.windex)
			}
			if uint64(len(sm.RaftLog.allEntries())) != tt.windex {
				t.Errorf("#%d.%d len(ents) = %v , want %v", i, j, len(sm.RaftLog.allEntries()), tt.windex)
			}
			// 如果是附加条目请求那么节点 2 是领导者
			wlead := uint64(2)
			// 如果是投票请求那么节点 2 是候选者，没有领导者
			if msgType == pb.MessageType_MsgRequestVote {
				wlead = None
			}
			if sm.Lead != wlead {
				t.Errorf("#%d, sm.Lead = %d, want %d", i, sm.Lead, wlead)
			}
		}
	}
}

func TestCandidateResetTermMessageType_MsgHeartbeat2AA(t *testing.T) {
	testCandidateResetTerm(t, pb.MessageType_MsgHeartbeat)
}

func TestCandidateResetTermMessageType_MsgAppend2AA(t *testing.T) {
	testCandidateResetTerm(t, pb.MessageType_MsgAppend)
}

// testCandidateResetTerm tests when a candidate receives a
// MessageType_MsgHeartbeat or MessageType_MsgAppend from leader, "Step" resets the term
// with leader's and reverts back to follower.
// testCandidateResetTerm 测试当候选者收到来自领导者的 MessageType_MsgHeartbeat 或 MessageType_MsgAppend 时，
// "Step" 会将任期重置为领导者的任期，并回退到跟随者状态。
func testCandidateResetTerm(t *testing.T, mt pb.MessageType) {
	a := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	b := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	c := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())

	nt := newNetwork(a, b, c)

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	if a.State != StateLeader {
		t.Errorf("state = %s, want %s", a.State, StateLeader)
	}
	if b.State != StateFollower {
		t.Errorf("state = %s, want %s", b.State, StateFollower)
	}
	if c.State != StateFollower {
		t.Errorf("state = %s, want %s", c.State, StateFollower)
	}

	// isolate 3 and increase term in rest
	// 隔离节点 3，并在其余节点中增加任期
	nt.isolate(3)

	nt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup})
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	if a.State != StateLeader {
		t.Errorf("state = %s, want %s", a.State, StateLeader)
	}
	if b.State != StateFollower {
		t.Errorf("state = %s, want %s", b.State, StateFollower)
	}

	// 推进时间直到节点 c 发起选举
	for c.State != StateCandidate {
		c.tick()
	}

	// 节点 c 从隔离中恢复
	nt.recover()

	// leader sends to isolated candidate
	// and expects candidate to revert to follower
	// 领导者发送消息给孤立的候选者，并期望候选者重新转变为追随者
	nt.send(pb.Message{From: 1, To: 3, Term: a.Term, MsgType: mt})

	if c.State != StateFollower {
		t.Errorf("state = %s, want %s", c.State, StateFollower)
	}

	// follower c term is reset with leader's
	// 跟随者 c 的任期随领导者的任期重置
	if a.Term != c.Term {
		t.Errorf("follower term expected same term as leader's %d, got %d", a.Term, c.Term)
	}
}

// TestDisruptiveFollower tests isolated follower,
// with slow network incoming from leader, election times out
// to become a candidate with an increased term. Then, the
// candiate's response to late leader heartbeat forces the leader
// to step down.
// TestDisruptiveFollower 测试孤立的跟随者，在来自领导者的网络延迟情况下，选举时间超时，
// 成为具有增加任期的候选者。然后，候选者对迟到的领导者心跳的响应迫使领导者退位。
func TestDisruptiveFollower2AA(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	n2 := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	n3 := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	nt := newNetwork(n1, n2, n3)

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	// check state
	// n1.State == StateLeader
	// n2.State == StateFollower
	// n3.State == StateFollower
	if n1.State != StateLeader {
		t.Fatalf("node 1 state: %s, want %s", n1.State, StateLeader)
	}
	if n2.State != StateFollower {
		t.Fatalf("node 2 state: %s, want %s", n2.State, StateFollower)
	}
	if n3.State != StateFollower {
		t.Fatalf("node 3 state: %s, want %s", n3.State, StateFollower)
	}

	// etcd server "advanceTicksForElection" on restart;
	// this is to expedite campaign trigger when given larger
	// election timeouts (e.g. multi-datacenter deploy)
	// Or leader messages are being delayed while ticks elapse
	// etcd 服务器中的 "advanceTicksForElection" 在重启时执行；
	// 这可以加快在给定较大选举超时时的选举触发（例如，在多数据中心部署时）。
	// 或者在 ticks 经过而领导者消息被延迟的情况下。
	for n3.State != StateCandidate {
		n3.tick()
	}

	// n1 is still leader yet
	// while its heartbeat to candidate n3 is being delayed
	// n1 仍然是领导者，
	// 而它向候选者 n3 发送的心跳消息却遭到延迟。

	// check state
	// n1.State == StateLeader
	// n2.State == StateFollower
	// n3.State == StateCandidate
	if n1.State != StateLeader {
		t.Fatalf("node 1 state: %s, want %s", n1.State, StateLeader)
	}
	if n2.State != StateFollower {
		t.Fatalf("node 2 state: %s, want %s", n2.State, StateFollower)
	}
	if n3.State != StateCandidate {
		t.Fatalf("node 3 state: %s, want %s", n3.State, StateCandidate)
	}
	// check term
	// n1.Term == 2
	// n2.Term == 2
	// n3.Term == 3
	if n1.Term != 2 {
		t.Fatalf("node 1 term: %d, want %d", n1.Term, 2)
	}
	if n2.Term != 2 {
		t.Fatalf("node 2 term: %d, want %d", n2.Term, 2)
	}
	if n3.Term != 3 {
		t.Fatalf("node 3 term: %d, want %d", n3.Term, 3)
	}

	// while outgoing vote requests are still queued in n3,
	// leader heartbeat finally arrives at candidate n3
	// however, due to delayed network from leader, leader
	// heartbeat was sent with lower term than candidate's
	// 当 n3 发出的投票请求仍在排队时，来自领导者的心跳最终到达候选者 n3。
	// 然而，由于来自领导者的网络延迟，领导者的心跳消息携带的任期（term）低于候选者的任期。
	nt.send(pb.Message{From: 1, To: 3, Term: n1.Term, MsgType: pb.MessageType_MsgHeartbeat})

	// then candidate n3 responds with "pb.MessageType_MsgAppendResponse" of higher term
	// and leader steps down from a message with higher term
	// this is to disrupt the current leader, so that candidate
	// with higher term can be freed with following election
	// 然后候选者 n3 用更高的任期回应 "pb.MessageType_MsgAppendResponse"，
	// 而领导者则因任期更高的信息而自我卸任。
	// 这样做是为了影响当前领导者，使得具备更高任期的候选者可以在后续选举中得到自由。

	// check state
	if n1.State != StateFollower {
		t.Fatalf("node 1 state: %s, want %s", n1.State, StateFollower)
	}

	// check term
	if n1.Term != 3 {
		t.Fatalf("node 1 term: %d, want %d", n1.Term, 3)
	}
}

// 测试心跳消息不会更新提交索引，只有附加日志条目消息会使提交索引更新
func TestHeartbeatUpdateCommit2AB(t *testing.T) {
	tests := []struct {
		failCnt    int
		successCnt int
	}{
		{1, 1},
		{5, 3},
		{5, 10},
	}
	for i, tt := range tests {
		sm1 := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		sm2 := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		sm3 := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		nt := newNetwork(sm1, sm2, sm3)
		nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
		nt.isolate(1)
		// propose log to old leader should fail
		// 向旧的领导者提议日志应该失败
		// 领导者节点 1 被孤立，虽然自己附加日志成功，但是其他节点收不到请求，不会更新提交索引
		for i := 0; i < tt.failCnt; i++ {
			nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
		}
		if sm1.RaftLog.committed > 1 {
			t.Fatalf("#%d: unexpected commit: %d", i, sm1.RaftLog.committed)
		}
		// propose log to cluster should success
		// 向集群提议日志应该成功
		// 节点 2 成为领导者，向节点 3 同步日志并成功提交
		nt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup})
		for i := 0; i < tt.successCnt; i++ {
			nt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
		}
		wCommit := uint64(2 + tt.successCnt) // 2 elctions
		if sm2.RaftLog.committed != wCommit {
			t.Fatalf("#%d: expected sm2 commit: %d, got: %d", i, wCommit, sm2.RaftLog.committed)
		}
		if sm3.RaftLog.committed != wCommit {
			t.Fatalf("#%d: expected sm3 commit: %d, got: %d", i, wCommit, sm3.RaftLog.committed)
		}

		nt.recover()
		// 忽略所有的附加日志条目请求
		nt.ignore(pb.MessageType_MsgAppend)
		nt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgBeat})
		// 因为附加日志条目请求被忽略，节点 1 不会更新提交索引
		if sm1.RaftLog.committed > 1 {
			t.Fatalf("#%d: expected sm1 commit: 1, got: %d", i, sm1.RaftLog.committed)
		}
	}
}

// tests the output of the state machine when receiving MessageType_MsgBeat
// 测试状态机在接收到 MessageType_MsgBeat 时的输出。
func TestRecvMessageType_MsgBeat2AA(t *testing.T) {
	tests := []struct {
		state StateType
		wMsg  int
	}{
		{StateLeader, 2},
		// candidate and follower should ignore MessageType_MsgBeat
		// 候选者和跟随者应该忽略 MessageType_MsgBeat 消息。
		{StateCandidate, 0},
		{StateFollower, 0},
	}

	for i, tt := range tests {
		sm := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		sm.RaftLog = newLog(newMemoryStorageWithEnts([]pb.Entry{{}, {Index: 1, Term: 0}, {Index: 2, Term: 1}}))
		sm.Term = 1
		sm.State = tt.state
		sm.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})

		msgs := sm.readMessages()
		if len(msgs) != tt.wMsg {
			t.Errorf("%d: len(msgs) = %d, want %d", i, len(msgs), tt.wMsg)
		}
		for _, m := range msgs {
			if m.MsgType != pb.MessageType_MsgHeartbeat {
				t.Errorf("%d: msg.Msgtype = %v, want %v", i, m.MsgType, pb.MessageType_MsgHeartbeat)
			}
		}
	}
}

// 测试领导者是否能正确增加其他节点的 nextIndex
func TestLeaderIncreaseNext2AB(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	// previous entries + noop entry + propose + 1
	// 之前的条目 + 无操作条目 + 提议 + 1
	wmatch := uint64(len(previousEnts)) + 1 + 1
	wnext := wmatch + 1

	storage := NewMemoryStorage()
	storage.Append(previousEnts)
	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
	nt := newNetwork(sm, nil, nil)
	// 节点 1 发起选举成为领导者
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	// 领导者接收客户端提议
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}})

	p := sm.Prs[2]
	// 增加对 matchIndex 的校验
	if p.Match != wmatch {
		t.Errorf("match = %d, want %d", p.Match, wmatch)
	}
	if p.Next != wnext {
		t.Errorf("next = %d, want %d", p.Next, wnext)
	}
}

func TestRestoreSnapshot2C(t *testing.T) {
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2, 3}},
		},
	}

	storage := NewMemoryStorage()
	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
	sm.handleSnapshot(pb.Message{Snapshot: &s})

	if sm.RaftLog.LastIndex() != s.Metadata.Index {
		t.Errorf("log.lastIndex = %d, want %d", sm.RaftLog.LastIndex(), s.Metadata.Index)
	}
	if mustTerm(sm.RaftLog.Term(s.Metadata.Index)) != s.Metadata.Term {
		t.Errorf("log.lastTerm = %d, want %d", mustTerm(sm.RaftLog.Term(s.Metadata.Index)), s.Metadata.Term)
	}
	sg := nodes(sm)
	if !reflect.DeepEqual(sg, s.Metadata.ConfState.Nodes) {
		t.Errorf("sm.Nodes = %+v, want %+v", sg, s.Metadata.ConfState.Nodes)
	}
}

func TestRestoreIgnoreSnapshot2C(t *testing.T) {
	previousEnts := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	storage := NewMemoryStorage()
	storage.Append(previousEnts)
	sm := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
	sm.RaftLog.committed = 3

	wcommit := uint64(3)
	commit := uint64(1)
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     commit,
			Term:      1,
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2}},
		},
	}

	// ignore snapshot
	sm.handleSnapshot(pb.Message{Snapshot: &s})
	if sm.RaftLog.committed != wcommit {
		t.Errorf("commit = %d, want %d", sm.RaftLog.committed, wcommit)
	}
}

func TestProvideSnap2C(t *testing.T) {
	// restore the state machine from a snapshot so it has a compacted log and a snapshot
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2}},
		},
	}
	storage := NewMemoryStorage()
	sm := newTestRaft(1, []uint64{1}, 10, 1, storage)
	sm.handleSnapshot(pb.Message{Snapshot: &s})

	sm.becomeCandidate()
	sm.becomeLeader()
	sm.readMessages() // clear message

	// force set the next of node 2 to less than the SnapshotMetadata.Index, so that node 2 needs a snapshot
	sm.Prs[2].Next = 10
	sm.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("somedata")}}})

	msgs := sm.readMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.MsgType != pb.MessageType_MsgSnapshot {
		t.Errorf("m.MsgType = %v, want %v", m.MsgType, pb.MessageType_MsgSnapshot)
	}
}

func TestRestoreFromSnapMsg2C(t *testing.T) {
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{1, 2}},
		},
	}
	m := pb.Message{MsgType: pb.MessageType_MsgSnapshot, From: 1, Term: 2, Snapshot: &s}

	sm := newTestRaft(2, []uint64{1, 2}, 10, 1, NewMemoryStorage())
	sm.Step(m)

	if sm.Lead != uint64(1) {
		t.Errorf("sm.Lead = %d, want 1", sm.Lead)
	}
}

func TestRestoreFromSnapWithOverlapingPeersMsg2C(t *testing.T) {
	s := pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     11, // magic number
			Term:      11, // magic number
			ConfState: &pb.ConfState{Nodes: []uint64{2, 3, 4}},
		},
	}
	m := pb.Message{MsgType: pb.MessageType_MsgSnapshot, From: 1, Term: 2, Snapshot: &s}

	sm := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	sm.Step(m)

	if sm.Lead != uint64(1) {
		t.Errorf("sm.Lead = %d, want 1", sm.Lead)
	}

	nodes := s.Metadata.ConfState.Nodes
	if len(nodes) != len(sm.Prs) {
		t.Errorf("len(sm.Prs) = %d, want %d", len(sm.Prs), len(nodes))
	}

	for _, p := range nodes {
		if _, ok := sm.Prs[p]; !ok {
			t.Errorf("missing peer %d", p)
		}
	}
}

func TestSlowNodeRestore2C(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)
	for j := 0; j <= 100; j++ {
		nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	}
	lead := nt.peers[1].(*Raft)
	nextEnts(lead, nt.storage[1])
	nt.storage[1].CreateSnapshot(lead.RaftLog.applied, &pb.ConfState{Nodes: nodes(lead)}, nil)
	nt.storage[1].Compact(lead.RaftLog.applied)

	nt.recover()

	// send heartbeats so that the leader can learn everyone is active.
	// node 3 will only be considered as active when node 1 receives a reply from it.
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})

	// trigger a snapshot
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})

	follower := nt.peers[3].(*Raft)

	// trigger a commit
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	if follower.RaftLog.committed != lead.RaftLog.committed {
		t.Errorf("follower.committed = %d, want %d", follower.RaftLog.committed, lead.RaftLog.committed)
	}
}

// TestAddNode tests that addNode could update nodes correctly.
func TestAddNode3A(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	r.addNode(2)
	nodes := nodes(r)
	wnodes := []uint64{1, 2}
	if !reflect.DeepEqual(nodes, wnodes) {
		t.Errorf("nodes = %v, want %v", nodes, wnodes)
	}
}

// TestRemoveNode tests that removeNode could update nodes and
// and removed list correctly.
func TestRemoveNode3A(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
	r.removeNode(2)
	w := []uint64{1}
	if g := nodes(r); !reflect.DeepEqual(g, w) {
		t.Errorf("nodes = %v, want %v", g, w)
	}

	// remove all nodes from cluster
	r.removeNode(1)
	w = []uint64{}
	if g := nodes(r); !reflect.DeepEqual(g, w) {
		t.Errorf("nodes = %v, want %v", g, w)
	}
}

// 测试单节点选举直接成为领导者，且领导者即使收到 MsgHup 消息也不直接发起选举，直接忽略
func TestCampaignWhileLeader2AA(t *testing.T) {
	cfg := newTestConfig(1, []uint64{1}, 5, 1, NewMemoryStorage())
	r := newRaft(cfg)
	if r.State != StateFollower {
		t.Errorf("expected new node to be follower but got %s", r.State)
	}
	// We don't call campaign() directly because it comes after the check
	// for our current state.
	// 我们不直接调用 campaign()，因为它是在检查当前状态之后执行的。
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	if r.State != StateLeader {
		t.Errorf("expected single-node election to become leader but got %s", r.State)
	}
	term := r.Term
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
	if r.State != StateLeader {
		t.Errorf("expected to remain leader but got %s", r.State)
	}
	if r.Term != term {
		t.Errorf("expected to remain in term %v but got %v", term, r.Term)
	}
}

// TestCommitAfterRemoveNode verifies that pending commands can become
// committed when a config change reduces the quorum requirements.
func TestCommitAfterRemoveNode3A(t *testing.T) {
	// Create a cluster with two nodes.
	s := NewMemoryStorage()
	r := newTestRaft(1, []uint64{1, 2}, 5, 1, s)
	r.becomeCandidate()
	r.becomeLeader()

	// Begin to remove the second node.
	cc := pb.ConfChange{
		ChangeType: pb.ConfChangeType_RemoveNode,
		NodeId:     2,
	}
	ccData, err := cc.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{
			{EntryType: pb.EntryType_EntryConfChange, Data: ccData},
		},
	})
	// Stabilize the log and make sure nothing is committed yet.
	if ents := nextEnts(r, s); len(ents) > 0 {
		t.Fatalf("unexpected committed entries: %v", ents)
	}
	ccIndex := r.RaftLog.LastIndex()

	// While the config change is pending, make another proposal.
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{
			{EntryType: pb.EntryType_EntryNormal, Data: []byte("hello")},
		},
	})

	// Node 2 acknowledges the config change, committing it.
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    2,
		Index:   ccIndex,
		Term:    r.Term,
	})
	ents := nextEnts(r, s)
	if len(ents) != 2 {
		t.Fatalf("expected two committed entries, got %v", ents)
	}
	if ents[0].EntryType != pb.EntryType_EntryNormal || ents[0].Data != nil {
		t.Fatalf("expected ents[0] to be empty, but got %v", ents[0])
	}
	if ents[1].EntryType != pb.EntryType_EntryConfChange {
		t.Fatalf("expected ents[1] to be EntryType_EntryConfChange, got %v", ents[1])
	}

	// Apply the config change. This reduces quorum requirements so the
	// pending command can now commit.
	r.removeNode(2)
	ents = nextEnts(r, s)
	if len(ents) != 1 || ents[0].EntryType != pb.EntryType_EntryNormal ||
		string(ents[0].Data) != "hello" {
		t.Fatalf("expected one committed EntryType_EntryNormal, got %v", ents)
	}
}

// TestLeaderTransferToUpToDateNode verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
func TestLeaderTransferToUpToDateNode3A(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	lead := nt.peers[1].(*Raft)

	if lead.Lead != 1 {
		t.Fatalf("after election leader is %d, want 1", lead.Lead)
	}

	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})

	nt.send(pb.Message{From: 1, To: 2, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

// TestLeaderTransferToUpToDateNodeFromFollower verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
// Not like TestLeaderTransferToUpToDateNode, where the leader transfer message
// is sent to the leader, in this test case every leader transfer message is sent
// to the follower.
func TestLeaderTransferToUpToDateNodeFromFollower3A(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	lead := nt.peers[1].(*Raft)

	if lead.Lead != 1 {
		t.Fatalf("after election leader is %d, want 1", lead.Lead)
	}

	// Transfer leadership to 2.
	nt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 2)

	// After some log replication, transfer leadership back to 1.
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferToSlowFollower3A(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})

	nt.recover()
	lead := nt.peers[1].(*Raft)
	if lead.Prs[3].Match != 1 {
		t.Fatalf("node 1 has match %d for node 3, want %d", lead.Prs[3].Match, 1)
	}

	// Transfer leadership to 3 when node 3 is lack of log.
	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 3)
}

func TestLeaderTransferAfterSnapshot3A(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)

	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	lead := nt.peers[1].(*Raft)
	nextEnts(lead, nt.storage[1])
	nt.storage[1].CreateSnapshot(lead.RaftLog.applied, &pb.ConfState{Nodes: nodes(lead)}, nil)
	nt.storage[1].Compact(lead.RaftLog.applied)

	nt.recover()
	if lead.Prs[3].Match != 1 {
		t.Fatalf("node 1 has match %d for node 3, want %d", lead.Prs[3].Match, 1)
	}

	// Transfer leadership to 3 when node 3 is lack of snapshot.
	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	// Send pb.MessageType_MsgHeartbeatResponse to leader to trigger a snapshot for node 3.
	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgHeartbeatResponse, Term: lead.Term})

	checkLeaderTransferState(t, lead, StateFollower, 3)
}

func TestLeaderTransferToSelf3A(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	lead := nt.peers[1].(*Raft)

	// Transfer leadership to self, there will be noop.
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferToNonExistingNode3A(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	lead := nt.peers[1].(*Raft)
	// Transfer leadership to non-existing node, there will be noop.
	nt.send(pb.Message{From: 4, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	checkLeaderTransferState(t, lead, StateLeader, 1)
}

func TestLeaderTransferReceiveHigherTermVote3A(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*Raft)

	// Transfer leadership to isolated node to let transfer pending.
	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	nt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup, Index: 1, Term: 2})

	checkLeaderTransferState(t, lead, StateFollower, 2)
}

func TestLeaderTransferRemoveNode3A(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	lead := nt.peers[1].(*Raft)
	lead.removeNode(3)

	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

// TestLeaderTransferBack verifies leadership can transfer back to self when last transfer is pending.
func TestLeaderTransferBack3A(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*Raft)

	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	// Transfer leadership back to self.
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateLeader, 1)
}

// TestLeaderTransferSecondTransferToAnotherNode verifies leader can transfer to another node
// when last transfer is pending.
func TestLeaderTransferSecondTransferToAnotherNode3A(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	nt.isolate(3)

	lead := nt.peers[1].(*Raft)

	nt.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgTransferLeader})
	// Transfer leadership to another node.
	nt.send(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgTransferLeader})

	checkLeaderTransferState(t, lead, StateFollower, 2)
}

func checkLeaderTransferState(t *testing.T, r *Raft, state StateType, lead uint64) {
	if r.State != state || r.Lead != lead {
		t.Fatalf("after transferring, node has state %v lead %v, want state %v lead %v", r.State, r.Lead, state, lead)
	}
}

// TestTransferNonMember verifies that when a MessageType_MsgTimeoutNow arrives at
// a node that has been removed from the group, nothing happens.
// (previously, if the node also got votes, it would panic as it
// transitioned to StateLeader)
func TestTransferNonMember3A(t *testing.T) {
	r := newTestRaft(1, []uint64{2, 3, 4}, 5, 1, NewMemoryStorage())
	r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgTimeoutNow, Term: r.Term})

	r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgRequestVoteResponse, Term: r.Term})
	r.Step(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgRequestVoteResponse, Term: r.Term})
	if r.State != StateFollower {
		t.Fatalf("state is %s, want StateFollower", r.State)
	}
}

// TestSplitVote verifies that after split vote, cluster can complete
// election in next round.
// TestSplitVote 验证在分裂选票后，因为随机选举超时，集群能在下一轮完成选举。
func TestSplitVote2AA(t *testing.T) {
	n1 := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	n2 := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	n3 := newTestRaft(3, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())

	n1.becomeFollower(1, None)
	n2.becomeFollower(1, None)
	n3.becomeFollower(1, None)

	nt := newNetwork(n1, n2, n3)
	nt.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

	// simulate leader down. followers start split vote.
	// 模拟领导者失效。跟随者开始进行分裂投票。
	nt.isolate(1)
	nt.send([]pb.Message{
		{From: 2, To: 2, MsgType: pb.MessageType_MsgHup},
		{From: 3, To: 3, MsgType: pb.MessageType_MsgHup},
	}...)

	// check whether the term values are expected
	// n2.Term == 3
	// n3.Term == 3
	sm := nt.peers[2].(*Raft)
	if sm.Term != 3 {
		t.Errorf("peer 2 term: %d, want %d", sm.Term, 3)
	}
	sm = nt.peers[3].(*Raft)
	if sm.Term != 3 {
		t.Errorf("peer 3 term: %d, want %d", sm.Term, 3)
	}

	// check state
	// n2 == candidate
	// n3 == candidate
	sm = nt.peers[2].(*Raft)
	if sm.State != StateCandidate {
		t.Errorf("peer 2 state: %s, want %s", sm.State, StateCandidate)
	}
	sm = nt.peers[3].(*Raft)
	if sm.State != StateCandidate {
		t.Errorf("peer 3 state: %s, want %s", sm.State, StateCandidate)
	}

	// node 2 election timeout first
	// 假设节点 2 首先发生选举超时
	nt.send(pb.Message{From: 2, To: 2, MsgType: pb.MessageType_MsgHup})

	// check whether the term values are expected
	// n2.Term == 4
	// n3.Term == 4
	sm = nt.peers[2].(*Raft)
	if sm.Term != 4 {
		t.Errorf("peer 2 term: %d, want %d", sm.Term, 4)
	}
	sm = nt.peers[3].(*Raft)
	if sm.Term != 4 {
		t.Errorf("peer 3 term: %d, want %d", sm.Term, 4)
	}

	// check state
	// n2 == leader
	// n3 == follower
	sm = nt.peers[2].(*Raft)
	if sm.State != StateLeader {
		t.Errorf("peer 2 state: %s, want %s", sm.State, StateLeader)
	}
	sm = nt.peers[3].(*Raft)
	if sm.State != StateFollower {
		t.Errorf("peer 3 state: %s, want %s", sm.State, StateFollower)
	}
}

func entsWithConfig(configFunc func(*Config), id uint64, terms ...uint64) *Raft {
	storage := NewMemoryStorage()
	for i, term := range terms {
		storage.Append([]pb.Entry{{Index: uint64(i + 1), Term: term}})
	}
	cfg := newTestConfig(id, []uint64{}, 5, 1, storage)
	if configFunc != nil {
		configFunc(cfg)
	}
	sm := newRaft(cfg)
	sm.Term = terms[len(terms)-1]
	return sm
}

// votedWithConfig creates a raft state machine with Vote and Term set
// to the given value but no log entries (indicating that it voted in
// the given term but has not received any logs).
func votedWithConfig(configFunc func(*Config), id, vote, term uint64) *Raft {
	storage := NewMemoryStorage()
	storage.SetHardState(pb.HardState{Vote: vote, Term: term})
	cfg := newTestConfig(id, []uint64{}, 5, 1, storage)
	if configFunc != nil {
		configFunc(cfg)
	}
	sm := newRaft(cfg)
	sm.Term = term
	return sm
}

type network struct {
	peers   map[uint64]stateMachine
	storage map[uint64]*MemoryStorage
	dropm   map[connem]float64
	ignorem map[pb.MessageType]bool

	// msgHook is called for each message sent. It may inspect the
	// message and return true to send it or false to drop it.
	msgHook func(pb.Message) bool
}

// newNetwork initializes a network from peers.
// A nil node will be replaced with a new *stateMachine.
// A *stateMachine will get its k, id.
// When using stateMachine, the address list is always [1, n].
func newNetwork(peers ...stateMachine) *network {
	return newNetworkWithConfig(nil, peers...)
}

// newNetworkWithConfig is like newNetwork but calls the given func to
// modify the configuration of any state machines it creates.
func newNetworkWithConfig(configFunc func(*Config), peers ...stateMachine) *network {
	size := len(peers)
	peerAddrs := idsBySize(size)

	npeers := make(map[uint64]stateMachine, size)
	nstorage := make(map[uint64]*MemoryStorage, size)

	for j, p := range peers {
		id := peerAddrs[j]
		switch v := p.(type) {
		case nil:
			nstorage[id] = NewMemoryStorage()
			cfg := newTestConfig(id, peerAddrs, 10, 1, nstorage[id])
			if configFunc != nil {
				configFunc(cfg)
			}
			sm := newRaft(cfg)
			npeers[id] = sm
		case *Raft:
			v.id = id
			npeers[id] = v
		case *blackHole:
			npeers[id] = v
		default:
			panic(fmt.Sprintf("unexpected state machine type: %T", p))
		}
	}
	return &network{
		peers:   npeers,
		storage: nstorage,
		dropm:   make(map[connem]float64),
		ignorem: make(map[pb.MessageType]bool),
	}
}

// 模拟同一时刻消息的收发
func (nw *network) send(msgs ...pb.Message) {
	// 循环处理节点发出和收到的消息直到没有任何消息需要处理
	for len(msgs) > 0 {
		m := msgs[0]
		p := nw.peers[m.To]
		p.Step(m)
		msgs = append(msgs[1:], nw.filter(p.readMessages())...)
	}
}

func (nw *network) drop(from, to uint64, perc float64) {
	nw.dropm[connem{from, to}] = perc
}

func (nw *network) cut(one, other uint64) {
	nw.drop(one, other, 2.0) // always drop
	nw.drop(other, one, 2.0) // always drop
}

func (nw *network) isolate(id uint64) {
	for i := 0; i < len(nw.peers); i++ {
		nid := uint64(i) + 1
		if nid != id {
			nw.drop(id, nid, 1.0) // always drop
			nw.drop(nid, id, 1.0) // always drop
		}
	}
}

func (nw *network) ignore(t pb.MessageType) {
	nw.ignorem[t] = true
}

func (nw *network) recover() {
	nw.dropm = make(map[connem]float64)
	nw.ignorem = make(map[pb.MessageType]bool)
}

func (nw *network) filter(msgs []pb.Message) []pb.Message {
	mm := []pb.Message{}
	for _, m := range msgs {
		if nw.ignorem[m.MsgType] {
			continue
		}
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// hups never go over the network, so don't drop them but panic
			panic("unexpected MessageType_MsgHup")
		default:
			perc := nw.dropm[connem{m.From, m.To}]
			if n := rand.Float64(); n < perc {
				continue
			}
		}
		if nw.msgHook != nil {
			if !nw.msgHook(m) {
				continue
			}
		}
		mm = append(mm, m)
	}
	return mm
}

type connem struct {
	from, to uint64
}

type blackHole struct{}

// 无操作，模拟机器宕机
func (blackHole) Step(pb.Message) error      { return nil }
func (blackHole) readMessages() []pb.Message { return nil }

var nopStepper = &blackHole{}

func idsBySize(size int) []uint64 {
	ids := make([]uint64, size)
	for i := 0; i < size; i++ {
		ids[i] = 1 + uint64(i)
	}
	return ids
}

func newTestConfig(id uint64, peers []uint64, election, heartbeat int, storage Storage) *Config {
	return &Config{
		ID:            id,
		peers:         peers,
		ElectionTick:  election,
		HeartbeatTick: heartbeat,
		Storage:       storage,
	}
}

func newTestRaft(id uint64, peers []uint64, election, heartbeat int, storage Storage) *Raft {
	return newRaft(newTestConfig(id, peers, election, heartbeat, storage))
}
