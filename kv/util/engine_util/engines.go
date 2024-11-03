package engine_util

import (
	"os"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/log"
)

// Engines keeps references to and data for the engines used by unistore.
// All engines are badger key/value databases.
// the Path fields are the filesystem path to where the data is stored.
// Engines 维护了用于 unistore 的引擎的引用和数据。
// 所有引擎都是基于 Badger 的键值数据库。
// Path 字段是数据存储在文件系统中的路径。
type Engines struct {
	// Data, including data which is committed (i.e., committed across other nodes) and un-committed (i.e., only present
	// locally).
	// Kv 引擎，可以视为 Raft 论文中提到的状态机，用于存储数据，包括已提交（即跨其他节点已提交）和未提交（即仅存在于本地）数据
	// 跨节点提交的数据：客户端的命令；本地数据：RaftApplyState 和 RegionLocalState
	Kv *badger.DB
	// Kv 引擎的数据存储路径
	KvPath string
	// Metadata used by Raft.
	// Raft 引擎，用于存储 Raft 元数据，包括日志和 RaftLocalState
	Raft *badger.DB
	// Raft 引擎的数据存储路径
	RaftPath string
}

// NewEngines 创建并返回一个新的 Engines 实例。
func NewEngines(kvEngine, raftEngine *badger.DB, kvPath, raftPath string) *Engines {
	return &Engines{
		Kv:       kvEngine,
		KvPath:   kvPath,
		Raft:     raftEngine,
		RaftPath: raftPath,
	}
}

// WriteKV 将一个写入批处理操作写入 Kv 引擎。
func (en *Engines) WriteKV(wb *WriteBatch) error {
	return wb.WriteToDB(en.Kv)
}

// WriteRaft 将一个写入批处理操作写入 Raft 引擎。
func (en *Engines) WriteRaft(wb *WriteBatch) error {
	return wb.WriteToDB(en.Raft)
}

// Close 关闭 Kv 和 Raft 引擎的数据库连接。
func (en *Engines) Close() error {
	dbs := []*badger.DB{en.Kv, en.Raft}
	for _, db := range dbs {
		if db == nil {
			continue
		}
		if err := db.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Destroy 关闭数据库并删除它们在文件系统中的数据。
func (en *Engines) Destroy() error {
	if err := en.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(en.KvPath); err != nil {
		return err
	}
	if err := os.RemoveAll(en.RaftPath); err != nil {
		return err
	}
	return nil
}

// CreateDB creates a new Badger DB on disk at path.
// CreateDB 在指定路径上创建一个新的 Badger 数据库。
func CreateDB(path string, raft bool) *badger.DB {
	// 获取 Badger 的默认配置
	opts := badger.DefaultOptions
	if raft {
		// Do not need to write blob for raft engine because it will be deleted soon.
		// 不需要为 raft 引擎写入 blob，因为它很快就会被删除。
		opts.ValueThreshold = 0
	}
	// 设置数据库目录
	opts.Dir = path
	// 设置值存储目录
	opts.ValueDir = opts.Dir
	if err := os.MkdirAll(opts.Dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}
	// 用给定配置打开数据库
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return db
}
