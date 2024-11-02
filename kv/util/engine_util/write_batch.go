package engine_util

import (
	"github.com/Connor1996/badger"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
)

// WriteBatch 表示一组待写入的数据库操作（包括插入和删除）。
//
// 该结构体的主要目的是批量处理操作，以提高性能并减少对数据库的频繁访问。
// 通过将多个操作累积在一起，最终可以一次性将所有操作写入数据库。
type WriteBatch struct {
	entries       []*badger.Entry // 待写入的条目列表，包括键值对和删除操作。
	size          int             // 当前 WriteBatch 中所有条目的总字节大小，包括键和值的长度。
	safePoint     int             // 安全点的索引，用于记录回滚的基准。
	safePointSize int             // 安全点时的大小，用于支持回滚操作。
	safePointUndo int             // 可以用来维护其他回滚状态的备份。
}

// 定义用于操作不同列族的常量字符串。
const (
	CfDefault string = "default" // 默认列族，用于存储常规数据
	CfWrite   string = "write"   // 写操作列族，专门存储待写入的数据
	CfLock    string = "lock"    // 锁列族，用于存储锁相关的信息以支持并发控制
)

// CFs 是一个字符串数组，包含所有可用的列族名称。
// 该数组的索引与列族含义相对应，可以用于在执行数据库操作时明确指定列族。
var CFs [3]string = [3]string{CfDefault, CfWrite, CfLock}

// Len 返回当前写入批次中条目的数量。
func (wb *WriteBatch) Len() int {
	return len(wb.entries)
}

// SetCF 将指定的列族、键和值新增到 WriteBatch 的条目中。
func (wb *WriteBatch) SetCF(cf string, key, val []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key:   KeyWithCF(cf, key),
		Value: val,
	})
	wb.size += len(key) + len(val)
}

// DeleteMeta 将指定的键标记为删除（通过有无值来区分插入和删除），并将其添加到 WriteBatch 的条目中。
func (wb *WriteBatch) DeleteMeta(key []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key: key,
	})
	wb.size += len(key)
}

// DeleteCF 将指定的列族和键标记为删除，并将其添加到 WriteBatch 的条目中。
func (wb *WriteBatch) DeleteCF(cf string, key []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key: KeyWithCF(cf, key),
	})
	wb.size += len(key)
}

// SetMeta 将指定的键和消息序列化后存储到 WriteBatch 的条目中。
func (wb *WriteBatch) SetMeta(key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	wb.entries = append(wb.entries, &badger.Entry{
		Key:   key,
		Value: val,
	})
	wb.size += len(key) + len(val)
	return nil
}

// SetSafePoint 设置当前条目的安全点，用于后续的回滚操作。
func (wb *WriteBatch) SetSafePoint() {
	wb.safePoint = len(wb.entries)
	wb.safePointSize = wb.size
}

// RollbackToSafePoint 回滚到最后设置的安全点，撤销之前的操作。
func (wb *WriteBatch) RollbackToSafePoint() {
	wb.entries = wb.entries[:wb.safePoint]
	wb.size = wb.safePointSize
}

// WriteToDB 将 WriteBatch 中的所有条目写入指定的数据库。
func (wb *WriteBatch) WriteToDB(db *badger.DB) error {
	if len(wb.entries) > 0 {
		err := db.Update(func(txn *badger.Txn) error {
			for _, entry := range wb.entries {
				var err1 error
				if len(entry.Value) == 0 {
					err1 = txn.Delete(entry.Key)
				} else {
					err1 = txn.SetEntry(entry)
				}
				if err1 != nil {
					return err1
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// MustWriteToDB 将 WriteBatch 写入数据库，如果发生错误则触发 panic。
func (wb *WriteBatch) MustWriteToDB(db *badger.DB) {
	err := wb.WriteToDB(db)
	if err != nil {
		panic(err)
	}
}

// Reset 重置 WriteBatch，清空所有条目和状态。
func (wb *WriteBatch) Reset() {
	wb.entries = wb.entries[:0]
	wb.size = 0
	wb.safePoint = 0
	wb.safePointSize = 0
	wb.safePointUndo = 0
}
