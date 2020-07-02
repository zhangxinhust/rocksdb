// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iostream>
#include <cstdio>
#include <string>
#include <unistd.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_simple_example";
std::string kMetaPath = "/tmp/rocksdb_simple_example/meta";

int main() {
  DB* db;
  Options options;
  int i = 0;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  options.db_paths.emplace_back(kDBPath, std::numeric_limits<uint64_t>::max());
  options.db_paths.emplace_back(kMetaPath, std::numeric_limits<uint64_t>::max());
  options.meta_file_path_id = options.db_paths.size()-1;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());
  for(i = 0; i < 100000000; i++) {
    s = db->Put(WriteOptions(), std::string("key")+std::to_string(i), std::string("value")+std::to_string(i)+std::string("xxxxxxxxxxxxxx"));
    assert(s.ok());
  }
  std::cout << "write finish" << std::endl;
  sleep(10);
  std::string value;
  for(i = 0; i < 1000000; i++) {
    // get value
    s = db->Get(ReadOptions(), std::string("key")+std::to_string(i), &value);
    assert(s.ok());
    if(value != std::string("value")+std::to_string(i)+std::string("xxxxxxxxxxxxxx")) {
        printf("k:%s\n", std::string("key")+std::to_string(i));
    }
  }
  delete db;

  return 0;
}


