package raftstore

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"

	"github.com/pingcap/errors"
)

// peerState contains the peer states that needs to run raft command and apply command.
// peerState 包含需要运行 Raft 命令和应用命令的节点状态。
type peerState struct {
	closed uint32 // 表示该节点是否已关闭，使用 uint32 便于原子操作
	peer   *peer  // 当前节点的指针
}

// router routes a message to a peer.
// router 是负责将消息路由到特定节点的结构体。
type router struct {
	peers       sync.Map           // 存储节点状态，使用 regionID 作为键
	peerSender  chan message.Msg   // 用于发送消息到节点的通道
	storeSender chan<- message.Msg // 用于发送消息到存储的通道
}

// newRouter 初始化一个新的 router 实例
func newRouter(storeSender chan<- message.Msg) *router {
	pm := &router{
		peerSender:  make(chan message.Msg, 40960), // 初始化节点消息通道，设定缓冲区大小
		storeSender: storeSender,                   // 存储通道的引用
	}
	return pm
}

// get 返回指定 regionID 的节点状态，如果不存在则返回 nil
func (pr *router) get(regionID uint64) *peerState {
	v, ok := pr.peers.Load(regionID)
	if ok {
		return v.(*peerState)
	}
	return nil
}

// register 注册新的节点到 router
func (pr *router) register(peer *peer) {
	id := peer.regionId
	newPeer := &peerState{
		peer: peer,
	}
	pr.peers.Store(id, newPeer)
}

// close 关闭指定 regionID 的节点并移除其状态
func (pr *router) close(regionID uint64) {
	v, ok := pr.peers.Load(regionID)
	if ok {
		ps := v.(*peerState)
		atomic.StoreUint32(&ps.closed, 1)
		pr.peers.Delete(regionID)
	}
}

// send 发送消息到指定 regionID 的节点
func (pr *router) send(regionID uint64, msg message.Msg) error {
	msg.RegionID = regionID
	p := pr.get(regionID)
	if p == nil || atomic.LoadUint32(&p.closed) == 1 {
		return errPeerNotFound
	}
	pr.peerSender <- msg
	return nil
}

// sendStore 发送消息到存储
func (pr *router) sendStore(msg message.Msg) {
	pr.storeSender <- msg
}

// 错误信息：节点未找到
var errPeerNotFound = errors.New("peer not found")

// RaftstoreRouter 封装 router，提供高层次的路由操作
type RaftstoreRouter struct {
	router *router
}

// NewRaftstoreRouter 创建一个新的 RaftstoreRouter
func NewRaftstoreRouter(router *router) *RaftstoreRouter {
	return &RaftstoreRouter{router: router}
}

// Send 通过 RaftstoreRouter 发送消息
func (r *RaftstoreRouter) Send(regionID uint64, msg message.Msg) error {
	return r.router.send(regionID, msg)
}

// SendRaftMessage 发送 Raft 消息
func (r *RaftstoreRouter) SendRaftMessage(msg *raft_serverpb.RaftMessage) error {
	regionID := msg.RegionId
	// 尝试发送 Raft 消息，如果失败，则将其发送到存储
	if r.router.send(regionID, message.NewPeerMsg(message.MsgTypeRaftMessage, regionID, msg)) != nil {
		r.router.sendStore(message.NewPeerMsg(message.MsgTypeStoreRaftMessage, regionID, msg))
	}
	return nil
}

// SendRaftCommand 发送 Raft 命令
func (r *RaftstoreRouter) SendRaftCommand(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) error {
	cmd := &message.MsgRaftCmd{
		Request:  req,
		Callback: cb,
	}
	regionID := req.Header.RegionId
	return r.router.send(regionID, message.NewPeerMsg(message.MsgTypeRaftCmd, regionID, cmd))
}
