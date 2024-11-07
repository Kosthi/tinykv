package config

import (
	"fmt"
	"os"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
)

// Config 是整个应用的配置结构体
type Config struct {
	StoreAddr     string // 存储服务地址
	Raft          bool   // 是否启用 Raft 协议
	SchedulerAddr string // 调度服务地址
	LogLevel      string // 日志级别

	DBPath string // 存储数据的目录。该目录应已存在且可写

	// raft_base_tick_interval is a base tick interval (ms).
	// Raft 基础滴答间隔（毫秒）
	RaftBaseTickInterval time.Duration
	// 心跳滴答数
	RaftHeartbeatTicks int
	// 选举超时滴答数
	RaftElectionTimeoutTicks int

	// Interval to gc unnecessary raft log (ms).
	// 清理不必要的 Raft 日志的间隔（毫秒）。
	RaftLogGCTickInterval time.Duration // Raft 日志垃圾收集间隔
	// When entry count exceed this value, gc will be forced trigger.
	// 当条目计数超过该值时，强制触发垃圾收集。
	RaftLogGcCountLimit uint64

	// Interval (ms) to check region whether need to be split or not.
	// 检查区域是否需要拆分的间隔（毫秒）。
	SplitRegionCheckTickInterval time.Duration
	// delay time before deleting a stale peer
	// 删除过时的成员之前的延迟时间
	SchedulerHeartbeatTickInterval      time.Duration // 调度者心跳间隔
	SchedulerStoreHeartbeatTickInterval time.Duration // 存储心跳间隔

	// 当区域 [a,e) 的大小达到 regionMaxSize 时，它将拆分为多个区域
	// [a,b), [b,c), [c,d), [d,e)，并且 [a,b)，[b,c)，[c,d) 的大小将是 regionSplitSize（可能稍大）。
	RegionMaxSize   uint64 // 区域最大大小
	RegionSplitSize uint64 // 区域拆分大小
}

// Validate 用于验证配置的有效性
func (c *Config) Validate() error {
	// 心跳滴答数必须大于0
	if c.RaftHeartbeatTicks == 0 {
		return fmt.Errorf("heartbeat tick must greater than 0")
	}

	// 选举超时滴答数需要在整个集群中保持一致，否则可能造成不一致
	if c.RaftElectionTimeoutTicks != 10 {
		log.Warnf("Election timeout ticks needs to be same across all the cluster, " +
			"otherwise it may lead to inconsistency.")
	}

	// 选举滴答数必须大于心跳滴答数
	if c.RaftElectionTimeoutTicks <= c.RaftHeartbeatTicks {
		return fmt.Errorf("election tick must be greater than heartbeat tick.")
	}

	return nil
}

const (
	KB uint64 = 1024
	MB uint64 = 1024 * 1024
)

// getLogLevel 从环境变量中获取日志级别（默认为 "info"）
func getLogLevel() (logLevel string) {
	logLevel = "info"
	// 如果环境变量存在，则使用该值
	if l := os.Getenv("LOG_LEVEL"); len(l) != 0 {
		logLevel = l
	}
	return
}

// NewDefaultConfig 创建并返回默认配置
func NewDefaultConfig() *Config {
	return &Config{
		SchedulerAddr:            "127.0.0.1:2379",  // 调度器默认地址
		StoreAddr:                "127.0.0.1:20160", // 存储服务默认地址
		LogLevel:                 getLogLevel(),     // 获取日志级别
		Raft:                     true,              // 启用 Raft 协议
		RaftBaseTickInterval:     1 * time.Second,   // Raft 基础滴答间隔设置为 1 秒
		RaftHeartbeatTicks:       2,                 // 默认心跳滴答数为 2
		RaftElectionTimeoutTicks: 10,                // 默认选举超时滴答数为 10
		RaftLogGCTickInterval:    10 * time.Second,  // Raft 日志垃圾收集间隔设置为 10 秒
		// 假设条目平均大小为 1k。
		RaftLogGcCountLimit:                 128000,           // Raft 日志垃圾收集计数限制
		SplitRegionCheckTickInterval:        10 * time.Second, // 拆分区域检查间隔设置为 10 秒
		SchedulerHeartbeatTickInterval:      10 * time.Second, // 调度者心跳间隔设置为 10 秒
		SchedulerStoreHeartbeatTickInterval: 10 * time.Second, // 存储心跳间隔设置为 10 秒
		RegionMaxSize:                       144 * MB,         // 区域最大大小设置为 144MB
		RegionSplitSize:                     96 * MB,          // 区域拆分大小设置为 96MB
		DBPath:                              "/tmp/badger",    // 数据存储目录
	}
}

// NewTestConfig 创建并返回测试配置
func NewTestConfig() *Config {
	return &Config{
		LogLevel:                 getLogLevel(),         // 获取日志级别
		Raft:                     true,                  // 启用 Raft 协议
		RaftBaseTickInterval:     50 * time.Millisecond, // Raft 基础滴答间隔设置为 50 毫秒
		RaftHeartbeatTicks:       2,                     // 默认心跳滴答数为 2
		RaftElectionTimeoutTicks: 10,                    // 默认选举超时滴答数为 10
		RaftLogGCTickInterval:    50 * time.Millisecond, // Raft 日志垃圾收集间隔设置为 50 毫秒
		// 假设条目平均大小为 1k。
		RaftLogGcCountLimit:                 128000,                 // Raft 日志垃圾收集计数限制
		SplitRegionCheckTickInterval:        100 * time.Millisecond, // 拆分区域检查间隔设置为 100 毫秒
		SchedulerHeartbeatTickInterval:      100 * time.Millisecond, // 调度者心跳间隔设置为 100 毫秒
		SchedulerStoreHeartbeatTickInterval: 500 * time.Millisecond, // 存储心跳间隔设置为 500 毫秒
		RegionMaxSize:                       144 * MB,               // 区域最大大小设置为 144MB
		RegionSplitSize:                     96 * MB,                // 区域拆分大小设置为 96MB
		DBPath:                              "/tmp/badger",          // 数据存储目录
	}
}
