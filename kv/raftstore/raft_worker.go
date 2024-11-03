package raftstore

import (
	"github.com/pingcap-incubator/tinykv/log"
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
)

// raftWorker is responsible for run raft commands and apply raft logs.
// raftWorker 负责执行 Raft 命令并应用 Raft 日志。
type raftWorker struct {
	// 路由
	pr *router

	// receiver of messages should sent to raft, including:
	// * raft command from `raftStorage`
	// * raft inner messages from other peers sent by network
	// 消息的接收者应该将消息发送给 Raft，包括：
	// * 来自 `raftStorage` 的 Raft 命令
	// * 从其他节点通过网络发送的 Raft 内部消息
	raftCh chan message.Msg

	// 全局上下文
	ctx *GlobalContext

	// 接收通道，用于优雅地关闭工作线程
	closeCh <-chan struct{}
}

// newRaftWorker 创建一个 RaftWorker 实例
func newRaftWorker(ctx *GlobalContext, pm *router) *raftWorker {
	return &raftWorker{
		raftCh: pm.peerSender,
		ctx:    ctx,
		pr:     pm,
	}
}

// run runs raft commands.
// On each loop, raft commands are batched by channel buffer.
// After commands are handled, we collect apply messages by peers, make a applyBatch, send it to apply channel.
// run 运行 Raft 命令。
// 在每次循环中，Raft 命令通过通道缓冲区进行批处理。
// 在处理完命令之后，我们收集来自各个 peer 的应用消息，
// 生成一个 applyBatch，并将其发送到应用通道。
func (rw *raftWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()        // 在 goroutine 结束时通知 WaitGroup 该任务已完成
	var msgs []message.Msg // 存储从 raftCh 中接收的消息

	for {
		// 重置消息切片
		msgs = msgs[:0]

		select {
		case <-closeCh: // 如果接收到关闭信号，则退出循环
			log.Debugf("raftWorker: shutting down")
			return
		case msg := <-rw.raftCh: // 从 raft 通道中接收消息
			log.Debugf("raftWorker: received message")
			msgs = append(msgs, msg) // 将消息添加到切片中
		}

		// 获取通道中的待处理消息数量
		pending := len(rw.raftCh)
		// 将所有消息添加到切片中
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-rw.raftCh)
		}

		// 创建一个映射来存储每个节点的状态
		peerStateMap := make(map[uint64]*peerState)

		// 处理每条消息
		for _, msg := range msgs {
			// 获取与消息对应的节点状态
			peerState := rw.getPeerState(peerStateMap, msg.RegionID)
			// 如果找不到节点状态，则跳过
			if peerState == nil {
				continue
			}
			// 处理消息
			log.Debugf("raftWorker: handling message for region ID: %d", msg.RegionID)
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleMsg(msg)
		}

		// 处理每个节点的 Raft 的 ready
		for _, peerState := range peerStateMap {
			log.Debugf("raftWorker: handling Raft ready for region ID: %d", peerState.peer.regionId)
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleRaftReady()
		}
	}
}

// getPeerState 根据 regionID 从 peersMap 中获取相应的 peerState。
// 如果 peersMap 中不存在该 peerState，则通过 rw.pr 获取。
// 并将获取到的 peerState 存储到 peersMap 中，然后返回该 peerState。
func (rw *raftWorker) getPeerState(peersMap map[uint64]*peerState, regionID uint64) *peerState {
	peer, ok := peersMap[regionID]
	if !ok {
		peer = rw.pr.get(regionID)
		if peer == nil {
			return nil
		}
		peersMap[regionID] = peer
	}
	return peer
}
