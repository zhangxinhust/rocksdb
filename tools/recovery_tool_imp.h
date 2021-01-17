// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/recovery_tool.h"

#include <memory>
#include <string>
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/db_impl/db_impl.h"
#include "file/filename.h"
#include "options/cf_options.h"
#include "util/file_reader_writer.h"

namespace rocksdb {

class DBImpl;
class Status;
class FastRecovery;
struct FileMetaData;

Status RecoverTableFile(FastRecovery* recoverer ,FileMetaData*         meta, uint32_t cf_id);

class FastRecovery {
 public:
  explicit FastRecovery(const Options& options, const std::string& file_name,
                         const std::string& output_dir, uint32_t num_column_families = 3);
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // nullptr if immutable_db_options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      ROCKS_LOG_WARN(info_log, "%s%s: dropping %d bytes; %s",
                     (this->status == nullptr ? "(ignoring error) " : ""),
                     fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) {
        *this->status = s;
      }
    }
  };

  Status OpenDB();
  void CloseDB();
  Status Recover();
  Status PrepareLogReaders(std::vector<log::Reader*>& log_readers);
  Status BuildTableFromWals(TableBuilder* builder, InternalIterator* iter,
                                       uint32_t cf_id, uint64_t sst_number);
  Status ParseBatchAndAddKV(Slice& record, TableBuilder* builder,
                                       std::set<std::string>& all_keys,
                                       std::set<std::string>& keys_found,
                                       uint32_t cf_id);
  Status GetTableReader(const std::string& file_path, std::unique_ptr<TableReader> *table_reader);
  Status GetLogReader(uint64_t log_number, log::Reader** log_reader);

  DB* GetDB() {
    return db_;
  }

  std::string ColumnFamilyName(size_t i);

  std::string dbname_;
  std::string output_path_;
  uint32_t num_column_families_;
  std::vector<ColumnFamilyDescriptor> column_families_;
  std::vector<ColumnFamilyHandle*> handles_;
  std::unique_ptr<Directory> output_dir_ = nullptr;
  DB* db_ = nullptr;
  EnvOptions soptions_;

  // options_ and internal_comparator_ will also be used in
  // ReadSequential internally (specifically, seek-related operations)
  Options options_;
  const ImmutableDBOptions ioptions_;
  const ImmutableCFOptions icfoptions_;
  const MutableCFOptions mcfoptions_;

  InternalKeyComparator internal_comparator_;

  std::vector<uint64_t> logs_number_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE

