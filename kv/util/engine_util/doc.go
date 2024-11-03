package engine_util

/*
An engine is a low-level system for storing key/value pairs locally (without distribution or any transaction support,
etc.). This package contains code for interacting with such engines.

CF means 'column family'. A good description of column families is given in https://github.com/facebook/rocksdb/wiki/Column-Families
(specifically for RocksDB, but the general concepts are universal). In short, a column family is a key namespace.
Multiple column families are usually implemented as almost separate databases. Importantly each column family can be
configured separately. Writes can be made atomic across column families, which cannot be done for separate databases.

engine_util includes the following packages:

* engines: a data structure for keeping engines required by unistore.
* write_batch: code to batch writes into a single, atomic 'transaction'.
* cf_iterator: code to iterate over a whole column family in badger.
*/

/*
Engine 是一个低级系统，用于本地存储键/值对（不支持分布或任何事务等）。该包包含与此类引擎交互的代码。

CF 代表“列族”。对列族的良好描述可以在 https://github.com/facebook/rocksdb/wiki/Column-Families 找到
（具体是针对 RocksDB 的，但一般概念是普遍适用的）。简而言之，列族是一个键命名空间。
多个列族通常被实现为几乎独立的数据库。重要的是，每个列族可以单独配置。写入操作可以在列族之间原子执行，这是在独立数据库中无法做到的。

engine_util 包含以下子包：

* engines：一个数据结构，用于存储 unistore 所需的引擎。
* write_batch：将多条写入操作批量处理为单个原子“事务”的代码。
* cf_iterator：在 Badger 中迭代整个列族的代码。
*/
