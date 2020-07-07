// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iostream>
#include <cstdio>
#include <string>
#include <unistd.h>
#include <fstream>


#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_simple_example";
std::string kMetaPath = "/tmp/rocksdb_simple_example/meta";

int main() {
  DB* db;
  Options options;

  std::string temp, key, value;
 
  std::string value_read;
  std::ifstream readfile(kDBPath+"/keyvalue.txt");
  if (!readfile.is_open()) {
    std::cout << "open file fail!" << std::endl;
    return 0;
  }

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

  std::cout << "get starting..." << std::endl;
  while(getline(readfile, temp)) {
    int idx = temp.find(" ");
    key = temp.substr(0, idx);
    value = temp.substr(idx+1);
    //std::cout << key << " " << value << std::endl;
    s = db->Get(ReadOptions(), key, &value_read);
    assert(s.ok());
    if(value != value_read) {
        printf("k:%s\n", key.c_str());
    }
  }
  readfile.close();
  std::cout << "get end" << std::endl;

  delete db;

  return 0;
}

