package raftstore

import (
	"bytes"
	"sync"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/scheduler_client"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

// regionItem 实现了 btree.Item 接口
var _ btree.Item = &regionItem{}

// regionItem 结构体用于在 BTree 中存储区域信息。
type regionItem struct {
	// 区域信息
	region *metapb.Region
}

// Less returns true if the region start key is less than the other.
// Less 返回 true，如果区域的起始键小于另一个键。
func (r *regionItem) Less(other btree.Item) bool {
	left := r.region.GetStartKey()
	right := other.(*regionItem).region.GetStartKey()
	return bytes.Compare(left, right) < 0
}

// storeMeta 结构体用于存储区域元数据。
type storeMeta struct {
	sync.RWMutex
	// region start key -> region
	regionRanges *btree.BTree
	// region_id -> region
	regions map[uint64]*metapb.Region
	// `MsgRequestVote` messages from newly split Regions shouldn't be dropped if there is no
	// such Region in this store now. So the messages are recorded temporarily and will be handled later.
	// 当新分割的区域发出 `MsgRequestVote` 消息时，如果当前存储中没有该区域，这些消息不应被丢弃。
	// 因此，这些消息会被暂时记录，并将在稍后处理。
	pendingVotes []*rspb.RaftMessage
}

// newStoreMeta 创建新的 storeMeta 实例
func newStoreMeta() *storeMeta {
	return &storeMeta{
		regionRanges: btree.New(2),
		regions:      map[uint64]*metapb.Region{},
	}
}

// setRegion 设置区域并关联相应的节点。
func (m *storeMeta) setRegion(region *metapb.Region, peer *peer) {
	m.regions[region.Id] = region
	peer.SetRegion(region)
}

// getOverlaps gets the regions which are overlapped with the specified region range.
// getOverlapRegions 获取与指定区域范围重叠的区域。
func (m *storeMeta) getOverlapRegions(region *metapb.Region) []*metapb.Region {
	item := &regionItem{region: region}
	var result *regionItem
	// find is a helper function to find an item that contains the regions start key.
	// find 是一个辅助函数，用于查找包含该区域起始键的条目。
	m.regionRanges.DescendLessOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem)
		return false
	})

	// 如果没有找到，使用当前区域项
	if result == nil || engine_util.ExceedEndKey(region.GetStartKey(), result.region.GetEndKey()) {
		result = item
	}

	// 存储重叠区域
	var overlaps []*metapb.Region
	m.regionRanges.AscendGreaterOrEqual(result, func(i btree.Item) bool {
		over := i.(*regionItem)
		// 如果该重叠区域的起始键超出当前区域的结束键，则结束迭代
		if engine_util.ExceedEndKey(over.region.GetStartKey(), region.GetEndKey()) {
			return false
		}
		overlaps = append(overlaps, over.region)
		return true
	})
	return overlaps
}

// GlobalContext 结构体存储全局上下文信息。
type GlobalContext struct {
	cfg                  *config.Config          // 配置
	engine               *engine_util.Engines    // 存储引擎
	store                *metapb.Store           // 存储信息
	storeMeta            *storeMeta              // 区域元数据
	snapMgr              *snap.SnapManager       // 快照管理器
	router               *router                 // 路由
	trans                Transport               // 传输接口
	schedulerTaskSender  chan<- worker.Task      // 调度任务发送通道
	regionTaskSender     chan<- worker.Task      // 区域任务发送通道
	raftLogGCTaskSender  chan<- worker.Task      // Raft 日志 GC 任务发送通道
	splitCheckTaskSender chan<- worker.Task      // 分裂检查任务发送通道
	schedulerClient      scheduler_client.Client // 调度客户端
	tickDriverSender     chan uint64             // 定时器发送通道
}

// Transport 接口用于发送 Raft 消息。
type Transport interface {
	Send(msg *rspb.RaftMessage) error
}

// loadPeers loads peers in this store. It scans the db engine, loads all regions and their peers from it
// WARN: This store should not be used before initialized.
// loadPeers 从当前 store 加载所有节点。它扫描数据库引擎，从中加载所有区域及其节点。
// 警告：不要在初始化之前使用此 store
func (bs *Raftstore) loadPeers() ([]*peer, error) {
	// Scan region meta to get saved regions.
	// 扫描区域元数据以获取保存的区域。
	startKey := meta.RegionMetaMinKey
	endKey := meta.RegionMetaMaxKey
	ctx := bs.ctx
	kvEngine := ctx.engine.Kv
	storeID := ctx.store.Id

	var totalCount, tombStoneCount int
	var regionPeers []*peer

	t := time.Now()
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	err := kvEngine.View(func(txn *badger.Txn) error {
		// get all regions from RegionLocalState
		// 从 RegionLocalState 获取所有区域。
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			if bytes.Compare(item.Key(), endKey) >= 0 {
				break
			}
			regionID, suffix, err := meta.DecodeRegionMetaKey(item.Key())
			if err != nil {
				return err
			}
			if suffix != meta.RegionStateSuffix {
				continue
			}
			val, err := item.Value()
			if err != nil {
				return errors.WithStack(err)
			}
			totalCount++
			localState := new(rspb.RegionLocalState)
			err = localState.Unmarshal(val)
			if err != nil {
				return errors.WithStack(err)
			}
			region := localState.Region
			if localState.State == rspb.PeerState_Tombstone {
				tombStoneCount++
				bs.clearStaleMeta(kvWB, raftWB, localState)
				continue
			}

			peer, err := createPeer(storeID, ctx.cfg, ctx.regionTaskSender, ctx.engine, region)
			if err != nil {
				return err
			}
			ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
			ctx.storeMeta.regions[regionID] = region
			// No need to check duplicated here, because we use region id as the key
			// in DB.
			// 这里不需要检查重复，因为我们在数据库中使用区域 ID 作为键。
			regionPeers = append(regionPeers, peer)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	kvWB.MustWriteToDB(ctx.engine.Kv)
	raftWB.MustWriteToDB(ctx.engine.Raft)

	log.Infof("start store %d, region_count %d, tombstone_count %d, takes %v",
		storeID, totalCount, tombStoneCount, time.Since(t))
	return regionPeers, nil
}

// clearStaleMeta 清理无效的元数据。
func (bs *Raftstore) clearStaleMeta(kvWB, raftWB *engine_util.WriteBatch, originState *rspb.RegionLocalState) {
	region := originState.Region
	raftState, err := meta.GetRaftLocalState(bs.ctx.engine.Raft, region.Id)
	if err != nil {
		// it has been cleaned up.
		// 元数据已被清理。
		return
	}
	err = ClearMeta(bs.ctx.engine, kvWB, raftWB, region.Id, raftState.LastIndex)
	if err != nil {
		panic(err)
	}
	if err := kvWB.SetMeta(meta.RegionStateKey(region.Id), originState); err != nil {
		panic(err)
	}
}

// workers 结构体用于管理各个工作线程。
type workers struct {
	raftLogGCWorker  *worker.Worker  // Raft 日志 GC 工作线程
	schedulerWorker  *worker.Worker  // 调度工作线程
	splitCheckWorker *worker.Worker  // 分裂检查工作线程
	regionWorker     *worker.Worker  // 区域工作线程
	wg               *sync.WaitGroup // 等待组，用于协同关闭
}

// Raftstore 结构体表示 Raft 存储服务。
type Raftstore struct {
	ctx        *GlobalContext  // 全局上下文
	storeState *storeState     // 存储状态
	router     *router         // 路由
	workers    *workers        // 工作线程集合
	tickDriver *tickDriver     // 定时驱动器
	closeCh    chan struct{}   // 关闭信号通道
	wg         *sync.WaitGroup // 等待组
}

// start 初始化 Raftstore 的工作环境并启动相关工作线程。
func (bs *Raftstore) start(
	meta *metapb.Store,
	cfg *config.Config,
	engines *engine_util.Engines,
	trans Transport,
	schedulerClient scheduler_client.Client,
	snapMgr *snap.SnapManager) error {
	y.Assert(bs.workers == nil)
	// TODO: we can get cluster meta regularly too later.
	if err := cfg.Validate(); err != nil {
		return err
	}
	err := snapMgr.Init()
	if err != nil {
		return err
	}
	wg := new(sync.WaitGroup)
	bs.workers = &workers{
		splitCheckWorker: worker.NewWorker("split-check", wg),
		regionWorker:     worker.NewWorker("snapshot-worker", wg),
		raftLogGCWorker:  worker.NewWorker("raft-gc-worker", wg),
		schedulerWorker:  worker.NewWorker("scheduler-worker", wg),
		wg:               wg,
	}
	bs.ctx = &GlobalContext{
		cfg:                  cfg,
		engine:               engines,
		store:                meta,
		storeMeta:            newStoreMeta(),
		snapMgr:              snapMgr,
		router:               bs.router,
		trans:                trans,
		schedulerTaskSender:  bs.workers.schedulerWorker.Sender(),
		regionTaskSender:     bs.workers.regionWorker.Sender(),
		splitCheckTaskSender: bs.workers.splitCheckWorker.Sender(),
		raftLogGCTaskSender:  bs.workers.raftLogGCWorker.Sender(),
		schedulerClient:      schedulerClient,
		tickDriverSender:     bs.tickDriver.newRegionCh,
	}
	regionPeers, err := bs.loadPeers()
	if err != nil {
		return err
	}

	for _, peer := range regionPeers {
		bs.router.register(peer)
	}
	bs.startWorkers(regionPeers)
	return nil
}

// startWorkers 启动 Raftstore 的各个工作线程。
func (bs *Raftstore) startWorkers(peers []*peer) {
	ctx := bs.ctx
	workers := bs.workers
	router := bs.router
	bs.wg.Add(2) // raftWorker, storeWorker
	rw := newRaftWorker(ctx, router)
	go rw.run(bs.closeCh, bs.wg)
	sw := newStoreWorker(ctx, bs.storeState)
	go sw.run(bs.closeCh, bs.wg)
	router.sendStore(message.Msg{Type: message.MsgTypeStoreStart, Data: ctx.store})
	for i := 0; i < len(peers); i++ {
		regionID := peers[i].regionId
		_ = router.send(regionID, message.Msg{RegionID: regionID, Type: message.MsgTypeStart})
	}
	engines := ctx.engine
	cfg := ctx.cfg
	workers.splitCheckWorker.Start(runner.NewSplitCheckHandler(engines.Kv, NewRaftstoreRouter(router), cfg))
	workers.regionWorker.Start(runner.NewRegionTaskHandler(engines, ctx.snapMgr))
	workers.raftLogGCWorker.Start(runner.NewRaftLogGCTaskHandler())
	workers.schedulerWorker.Start(runner.NewSchedulerTaskHandler(ctx.store.Id, ctx.schedulerClient, NewRaftstoreRouter(router)))
	go bs.tickDriver.run()
}

// shutDown 优雅地关闭 Raftstore。
func (bs *Raftstore) shutDown() {
	close(bs.closeCh)
	bs.wg.Wait()
	bs.tickDriver.stop()
	if bs.workers == nil {
		return
	}
	workers := bs.workers
	bs.workers = nil
	workers.splitCheckWorker.Stop()
	workers.regionWorker.Stop()
	workers.raftLogGCWorker.Stop()
	workers.schedulerWorker.Stop()
	workers.wg.Wait()
}

// CreateRaftstore 创建并返回 RaftstoreRouter 和 Raftstore 实例。
func CreateRaftstore(cfg *config.Config) (*RaftstoreRouter, *Raftstore) {
	storeSender, storeState := newStoreState(cfg)
	router := newRouter(storeSender)
	raftstore := &Raftstore{
		router:     router,
		storeState: storeState,
		tickDriver: newTickDriver(cfg.RaftBaseTickInterval, router, storeState.ticker),
		closeCh:    make(chan struct{}),
		wg:         new(sync.WaitGroup),
	}
	return NewRaftstoreRouter(router), raftstore
}
