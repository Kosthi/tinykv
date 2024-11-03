package message

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

// RaftStore 消息类型
type MsgType int64

const (
	// just a placeholder
	// 占位符
	MsgTypeNull MsgType = 0
	// message to start the ticker of peer
	// 消息用于启动 peer 的定时器
	MsgTypeStart MsgType = 1
	// message of base tick to drive the ticker
	// 消息用于驱动定时器的基本滴答
	MsgTypeTick MsgType = 2
	// message wraps a raft message that should be forwarded to Raft module
	// the raft message is from peer on other store
	// 消息封装一个应转发给 Raft 模块的 Raft 消息
	// 该 Raft 消息来自其他存储中的 peer
	MsgTypeRaftMessage MsgType = 3
	// message wraps a raft command that maybe a read/write request or admin request
	// the raft command should be proposed to Raft module
	// 消息封装了一个可能是读/写请求或管理员请求的 Raft 命令
	// Raft 命令应提议给 Raft 模块
	MsgTypeRaftCmd MsgType = 4
	// message to trigger split region
	// it first asks Scheduler for allocating new split region's ids, then schedules a
	// MsyTypeRaftCmd with split admin command
	// 消息用于触发区域拆分
	// 首先向调度器请求分配新的拆分区域 ID，然后调度一个带有拆分管理员命令的 MsyTypeRaftCmd 消息
	MsgTypeSplitRegion MsgType = 5
	// message to update region approximate size
	// it is sent by split checker
	// 消息用于更新区域的近似大小，由拆分检查器发送
	MsgTypeRegionApproximateSize MsgType = 6
	// message to trigger gc generated snapshots
	// 消息用于触发垃圾回收生成的快照
	MsgTypeGcSnap MsgType = 7

	// message wraps a raft message to the peer not existing on the Store.
	// It is due to region split or add peer conf change
	// 消息封装了一个 Raft 消息，发送给在存储上不存在的节点
	// 这可能是由于区域拆分或添加节点的配置更改导致的
	MsgTypeStoreRaftMessage MsgType = 101
	// message of store base tick to drive the store ticker, including store heartbeat
	// 消息用于 store 基础滴答，用于驱动 store 滴答器，包括 store 心跳
	MsgTypeStoreTick MsgType = 106
	// message to start the ticker of store
	// 消息用于启动 store 的滴答器
	MsgTypeStoreStart MsgType = 107
)

// Msg 结构体表示一条消息，包括消息类型、区域 ID 和附加数据。
type Msg struct {
	Type     MsgType     // 消息类型
	RegionID uint64      // 区域 ID
	Data     interface{} // 附加数据，可以是任意类型
}

// NewMsg 创建一个新的消息，只有类型和数据。
func NewMsg(tp MsgType, data interface{}) Msg {
	return Msg{Type: tp, Data: data}
}

// NewPeerMsg 创建一个新的消息，同时设置区域 ID。
func NewPeerMsg(tp MsgType, regionID uint64, data interface{}) Msg {
	return Msg{Type: tp, RegionID: regionID, Data: data}
}

// MsgGCSnap 表示触发垃圾回收快照的消息。
type MsgGCSnap struct {
	Snaps []snap.SnapKeyWithSending // 包含待发送的快照列表
}

// MsgRaftCmd 表示一个 Raft 命令消息。
type MsgRaftCmd struct {
	Request  *raft_cmdpb.RaftCmdRequest // Raft 命令请求
	Callback *Callback                  // 回调函数，用于处理命令完成后事务
}

// MsgSplitRegion 表示触发区域拆分的消息。
type MsgSplitRegion struct {
	RegionEpoch *metapb.RegionEpoch // 区域的时期
	SplitKey    []byte              // 拆分的键值
	Callback    *Callback           // 回调函数，用于处理拆分完成后的事务
}
