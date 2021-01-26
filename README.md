## 分支说明
6.4.tikv：PingCAP基于Facebook的6.4.6修改的
shk-hust-cloud：shk的多路径方案，基于6.4.tikv
hust-cloud：wal基本方案，基于6.4.tikv
hust-cloud-test：hust-cloud的测试代码，用于XP的论文最终测试
wal-test：基于hust-cloud的测试代码，会发生段错误
fast-recover：将元数据写到云上，准备用于快速恢复方案，但还有bug，已经启用
recovery_tool：快速恢复方案
fch-meta-v6.4.6：fch将元数据和数据库分开写的方案

## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

[![Linux/Mac Build Status](https://travis-ci.org/facebook/rocksdb.svg?branch=master)](https://travis-ci.org/facebook/rocksdb)
[![Windows Build status](https://ci.appveyor.com/api/projects/status/fbgfu0so3afcno78/branch/master?svg=true)](https://ci.appveyor.com/project/Facebook/rocksdb/branch/master)
[![PPC64le Build Status](http://140.211.168.68:8080/buildStatus/icon?job=Rocksdb)](http://140.211.168.68:8080/job/Rocksdb)

RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on [LevelDB](https://github.com/google/leveldb) by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it specially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/master/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Design discussions are conducted in https://www.facebook.com/groups/rocksdb.dev/

## License

RocksDB is dual-licensed under both the GPLv2 (found in the COPYING file in the root directory) and Apache 2.0 License (found in the LICENSE.Apache file in the root directory).  You may select, at your option, one of the above-listed licenses.
