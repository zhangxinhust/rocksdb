
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "tools/recovery_tool_imp.h"

#include <cinttypes>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <vector>
#include <future>
#include <thread>
#include <algorithm>
#include <time.h>
#include <unistd.h>

#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_builder.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/plain/plain_table_factory.h"
#include "table/table_reader.h"
#include "util/compression.h"
#include "util/random.h"

#include "port/port.h"

namespace rocksdb {
class FastRecovery;
struct ValueSeqType;
extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;

Status ReadCfWalToBuffer(FastRecovery* recoverer, uint32_t cf_id) {
  fprintf(stdout, "ReadCfWalToBuffer begin.\n");
  Status s;
  uint64_t start = recoverer->ioptions_.env->NowMicros();
  uint64_t parse_add_last = 0, construct_last = 0, map_last = 0;
  uint64_t total_entry = 0, short_entry, reader_failed = 0, parse_failed = 0;

  for (uint32_t i = 0; i < recoverer->logs_number_.size(); i++) {
    uint64_t log_number = recoverer->logs_number_[i];
    log::Reader *reader = nullptr;
    s = recoverer->GetLogReader(log_number, &reader);
    if (!s.ok() || !reader) {
      fprintf(stdout, "GetLogReader : %s.\n", s.ToString().c_str());
      fprintf(stdout, "log %lu skipped since log Reader null.\n", log_number);
      reader_failed++;
      continue;
    }
    std::string scratch;
    Slice record;
    WriteBatch batch;
    SequenceNumber sequence;
    while (true) {
      if (!s.ok()) {
        fprintf(stdout, "ReadRecord break : %s.\n", s.ToString().c_str());
        break;
      }
      if (!reader->ReadRecord(&record, &scratch, recoverer->ioptions_.wal_recovery_mode)) {
        break;
      }
      total_entry++;
      if (record.size() < WriteBatchInternal::kHeader) {
        short_entry++;
        continue;
      }
      WriteBatchInternal::SetContents(&batch, record);
      sequence = WriteBatchInternal::Sequence(&batch);

      uint64_t parse_start = recoverer->ioptions_.env->NowMicros();
      s = recoverer->ParseBatchAndAddToMap(record, sequence, cf_id, construct_last, map_last);
      uint64_t parse_end = recoverer->ioptions_.env->NowMicros();
      parse_add_last += parse_end - parse_start;
      if (!s.ok()) {
        parse_failed++;
        fprintf(stdout, "ParseBatchAndAddToMap : %s.\n", s.ToString().c_str());
      }
    }
    delete reader;
    reader = nullptr;
  }

  uint64_t end = recoverer->ioptions_.env->NowMicros();
  fprintf(stdout, "ReadWalsToBuffer last %lu s, parse add: %lu s, construct: %lu, map: %lu\n",
                  (end - start) / 1000000,
                  parse_add_last / 1000000,
                  construct_last / 1000000,
                  map_last / 1000000);
  fprintf(stdout, "total: %lu, map: %lu, short: %lu, reader fail: %lu, parse fail: %lu\n",
                  total_entry, recoverer->k2v[cf_id].size(), short_entry, reader_failed, parse_failed);
  return s;
}


Status RecoverTableFile(FastRecovery* recoverer ,FileMetaData*         meta, uint32_t cf_id) {
  fprintf(stdout, "RecoverTableFile begin %lu++++++++++.\n", meta->fd.GetNumber());
  Status s;
  uint64_t start = recoverer->ioptions_.env->NowMicros();

  uint64_t sst_number = meta->fd.GetNumber();
  uint32_t path_id = meta->fd.GetPathId();
  std::string sst_name = TableFileName(recoverer->ioptions_.db_paths, sst_number, path_id);

  // SST reader
  std::unique_ptr<TableReader> table_reader;
  s = recoverer->GetTableReader(sst_name, &table_reader);
  if (!s.ok()) {
    fprintf(stdout, "GetTableReader : %s.\n", s.ToString().c_str());
    return s;
  }

  // SST writer
  // output_path_ must not have "/" at the end
  std::string new_sst_name = MakeTableFileName(recoverer->output_path_, sst_number);

  std::unique_ptr<WritableFile> out_file;
  recoverer->ioptions_.env->NewWritableFile(new_sst_name, &out_file, recoverer->soptions_);
  std::unique_ptr<WritableFileWriter> dest_writer;
  dest_writer.reset(
      new WritableFileWriter(std::move(out_file), new_sst_name, recoverer->soptions_));

  // Table Builder
  BlockBasedTableOptions table_options;
  BlockBasedTableFactory block_based_tf(table_options);
  std::unique_ptr<TableBuilder> table_builder;
  std::vector<std::unique_ptr<IntTblPropCollectorFactory> >
      block_based_table_factories;
  CompressionOptions compress_opt;
  std::string column_family_name;
  int unknown_level = -1;

  TableBuilderOptions tb_opts(
      recoverer->icfoptions_, recoverer->mcfoptions_,
      recoverer->internal_comparator_, &block_based_table_factories, recoverer->mcfoptions_.compression,
      0 /* sample_for_compression */, compress_opt,
      false /* skip_filters */, column_family_name, unknown_level);

  table_builder.reset(block_based_tf.NewTableBuilder(
      tb_opts,
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
      dest_writer.get()));

  // Scan all the keys in SST
  std::unique_ptr<InternalIterator> iter(table_reader->NewIterator(
      ReadOptions(), recoverer->mcfoptions_.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kFastRecovery));

  uint64_t total_keys = 0, iter_miss_keys = 0, wal_miss_keys = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    total_keys++;
    if (!iter->status().ok()) {
      iter_miss_keys++;
      continue;
    }
    std::string user_key = ExtractUserKey(iter->key()).ToString();
    if (!recoverer->k2v[cf_id].count(user_key)) {
      wal_miss_keys++;
      continue;
    }

    // 到底是用SST还是wal里的seq和type？
    // 元数据里面的key有没有存seq呢？如果没有，那么不能用SST的
    // 可是wal里面的seq会不一样，会不会让builder Add出错？
    SequenceNumber sequence;
    ValueType type;
    uint64_t pack = ExtractInternalKeyFooter(iter->key());
    UnPackSequenceAndType(pack, &sequence, &type);
    ValueSeqType &vst =  recoverer->k2v[cf_id][user_key];

    InternalKey ikey;
    ikey.Set(user_key, sequence, type);
    //table_builder->Add(ikey.Encode(), value, true);??true
    table_builder->Add(iter->key(), vst.value);
  }
  s = table_builder->Finish();
  if (!s.ok()) {
    fprintf(stdout, "Finish : %s.\n", s.ToString().c_str());
  }
  uint64_t end = recoverer->ioptions_.env->NowMicros();
  fprintf(stdout, "SST %lu last %lu s, total: %lu, iter miss: %lu, wal miss: %lu.\n",
                  meta->fd.GetNumber(), (end - start) / 1000000,
                  total_keys, iter_miss_keys, wal_miss_keys);
  return s;
}

FastRecovery::FastRecovery(const Options& options, const std::string& dbname,
    const std::string& output_path, uint32_t num_column_families)
    : dbname_(dbname),
      output_path_(output_path),
      num_column_families_(num_column_families),
      options_(options),
      ioptions_(options),
      icfoptions_(options),
      mcfoptions_(ColumnFamilyOptions(options_)),
      internal_comparator_(BytewiseComparator()) {
  k2v = std::vector<std::unordered_map<std::string, ValueSeqType> >(num_column_families, 
                                                          std::unordered_map<std::string, ValueSeqType>());
  for (size_t i = 0; i < num_column_families_; i++) {
    column_families_.push_back(ColumnFamilyDescriptor(
      ColumnFamilyName(i), ColumnFamilyOptions(options)));
  }
  fprintf(stdout, "dbname_: %s, output_path_: %s, wal_dir: %s, path0: %s, path1: %s.\n",
          dbname_.c_str(), output_path_.c_str(), ioptions_.wal_dir.c_str(),
          ioptions_.db_paths[0].path.c_str(), ioptions_.db_paths[1].path.c_str());
}

Status FastRecovery::OpenDB() {
  Status s = DB::Open(options_, dbname_, column_families_, &handles_, &db_, true/*skip_wal*/);
  if (!s.ok()) {
    fprintf(stdout, "DB::Open : %s.\n", s.ToString().c_str());
    return s;
  }
  s = DBImpl::CreateAndNewDirectory(ioptions_.env, output_path_, &output_dir_);
  if (!s.ok()) {
    fprintf(stdout, "CreateAndNewDirectory : %s.\n", s.ToString().c_str());
    return s;
  }

  // Get wal files from wal_dir
  std::vector<std::string> filenames;
  s = ioptions_.env->GetChildren(ioptions_.wal_dir, &filenames);
  if (!s.ok()) {
    fprintf(stdout, "GetChildren of path[%s] : %s.\n", ioptions_.wal_dir.c_str(), s.ToString().c_str());
    return s;
  }
  for (size_t i = 0; i < filenames.size(); i++) {
    uint64_t number;
    FileType type;
    if (ParseFileName(filenames[i], &number, &type) && type == kLogFile) {
      logs_number_.push_back(number);
    } else {
      fprintf(stdout, "ParseFileName %s: type: %d.\n", filenames[i].c_str(), int(type));
    }
  }
  sort(logs_number_.begin(), logs_number_.end(), std::less<uint64_t>()); // older file first
  fprintf(stdout, "logs_number_ size: %lu.\n", logs_number_.size());
  return s;
}

Status FastRecovery::Recover() {
  Status s;
  std::vector<std::future<Status> > statuses;

  // Read wals to buffer
  for (ColumnFamilyData* cfd : *db_->GetVersionSet()->GetColumnFamilySet()) {
    fprintf(stdout, "Read wal of cf %s to buffer-------.\n", cfd->GetName().c_str());
    if (cfd->IsDropped() || !cfd->initialized()) {
      fprintf(stdout, "Column family %s skipped.\n", cfd->GetName().c_str());
      continue;
    }
    statuses.emplace_back(std::async(std::launch::async, ReadCfWalToBuffer,
                                     this, cfd->GetID()));
  }
  for (auto & status : statuses) {
    Status ret_status = status.get();
    if (!ret_status.ok()) {
      fprintf(stdout, "Status returned by wal thread: %s.\n", ret_status.ToString().c_str());
    }
  }
  statuses.clear();

  uint64_t map_count = 0;
  for (auto &kv : k2v) {
    map_count += kv.size();
    fprintf(stdout, "k2v size: %lu\n", kv.size());
  }
  fprintf(stdout, "Total kvs in map: %lu.\n", map_count);


  // Recover SST with the kvs in buffer
  uint32_t thread_count = 0;

  for (ColumnFamilyData* cfd : *db_->GetVersionSet()->GetColumnFamilySet()) {
    fprintf(stdout, "Recover SSTs of cf %s----------------------.\n", cfd->GetName().c_str());
    if (cfd->IsDropped() || !cfd->initialized()) {
      fprintf(stdout, "Column family %s skipped.\n", cfd->GetName().c_str());
      continue;
    }
    assert(cfd->NumberLevels() > 1);
    std::vector<FileMetaData*> files = cfd->current()->storage_info()->LevelFiles(0);
    uint64_t l0_size = files.size(), l1_size = cfd->current()->storage_info()->LevelFiles(1).size();
    for (auto file : cfd->current()->storage_info()->LevelFiles(1)) {
      files.push_back(file);
    }
    fprintf(stdout, "Level0 size: %lu, Level1 size: %lu.\n", l0_size, l1_size);

    for (uint64_t i = 0; i < files.size(); i++) {
      FileMetaData* file = files[i];
      if (!file) {continue;}
      //FileMetaData _file = *file;
      fprintf(stdout, "Add SST %lu in L%lu.\n",
                      file->fd.GetNumber(),
                      i < l0_size ? 0ul : 1ul);
      statuses.emplace_back(std::async(std::launch::async, RecoverTableFile,
                                       this, file, cfd->GetID()));
      thread_count++;
      /*
      if (thread_count >= 10) {
        fprintf(stdout, "statuses size: %lu.\n", statuses.size());
        thread_count = 0;
        for (auto & status : statuses) {
          Status ret_status = status.get();
          if (!ret_status.ok()) {
            fprintf(stdout, "Status returned by thread: %s.\n", ret_status.ToString().c_str());
          }
        }
        statuses.clear();
      }
      */
    }
  }
  fprintf(stdout, "statuses size: %lu.\n", statuses.size());
  thread_count = 0;
  for (auto & status : statuses) {
    Status ret_status = status.get();
    if (!ret_status.ok()) {
      fprintf(stdout, "Status returned by SST thread: %s.\n", ret_status.ToString().c_str());
    }
  }
  statuses.clear();

  if (output_dir_) {
    s = output_dir_->Fsync();
  }
  return s;
}

Status FastRecovery::ParseBatchAndAddToMap(Slice& record, 
                                                        SequenceNumber sequence,
                                                        uint32_t cf_id,
                                                        uint64_t& construct_last,
                                                        uint64_t& map_last) {
  //fprintf(stdout, "ParseBatchAndAddToMap begin\n");
  if (record.size() < WriteBatchInternal::kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  record.remove_prefix(WriteBatchInternal::kHeader);
  Slice key, value, blob, xid;
  // Sometimes a sub-batch starts with a Noop. We want to exclude such Noops as
  // the batch boundary symbols otherwise we would mis-count the number of
  // batches. We do that by checking whether the accumulated batch is empty
  // before seeing the next Noop.
  Status s;
  char tag = 0;
  uint32_t column_family = 0;  // default
  bool last_was_try_again = false;
  while ( (s.ok() && !record.empty()) || UNLIKELY(s.IsTryAgain()) ) {
    if (LIKELY(!s.IsTryAgain())) {
      last_was_try_again = false;
      tag = 0;
      column_family = 0;  // default
      s = ReadRecordFromWriteBatch(&record, &tag, &column_family, &key, &value,
                                   &blob, &xid);
      if (!s.ok()) {
        return s;
      }
      if (column_family != cf_id) {continue;}
    } else {
      assert(s.IsTryAgain());
      assert(!last_was_try_again); // to detect infinite loop bugs
      if (UNLIKELY(last_was_try_again)) {
        return Status::Corruption(
            "two consecutive TryAgain in WriteBatch handler; this is either a "
            "software bug or data corruption.");
      }
      last_was_try_again = true;
      s = Status::OK();
    }

    std::string user_value = value.ToString();
    uint64_t construct_start = ioptions_.env->NowMicros();
    ValueSeqType vst;
    vst.value = user_value;
    vst.sequence = sequence;
    vst.type = ValueType(tag);
    uint64_t construct_end = ioptions_.env->NowMicros();
    construct_last += construct_end - construct_start;
    k2v[column_family][key.ToString()] = vst;
    uint64_t map_end = ioptions_.env->NowMicros();
    map_last += map_end - construct_end;
  }
  return s;
}

Status FastRecovery::GetTableReader(const std::string& file_path, 
                                             std::unique_ptr<TableReader> *table_reader) {
  std::unique_ptr<RandomAccessFile> file;
  std::unique_ptr<RandomAccessFileReader> file_reader;
  uint64_t file_size = 0;
  Status s = options_.env->NewRandomAccessFile(file_path, &file, soptions_);
  if (s.ok()) {
    s = options_.env->GetFileSize(file_path, &file_size);
  } else {
    return s;
  }
  file_reader.reset(new RandomAccessFileReader(std::move(file), file_path));

  // We need to turn off pre-fetching of index and filter nodes for
  // BlockBasedTable
  if (BlockBasedTableFactory::kName == options_.table_factory->Name()) {
    return options_.table_factory->NewTableReader(
        TableReaderOptions(icfoptions_, mcfoptions_.prefix_extractor.get(),
                           soptions_, internal_comparator_),
        std::move(file_reader), file_size, table_reader, /*enable_prefetch=*/false);
  }
  // For all other factory implementation
  return options_.table_factory->NewTableReader(
      TableReaderOptions(icfoptions_, mcfoptions_.prefix_extractor.get(), soptions_,
                         internal_comparator_),
      std::move(file_reader), file_size, table_reader);
}

Status FastRecovery::GetLogReader(uint64_t log_number, log::Reader** log_reader) {
  Status s;
  // Open the log file
  std::string fname = LogFileName(ioptions_.wal_dir, log_number);
  std::unique_ptr<SequentialFileReader> file_reader;
  {
    std::unique_ptr<SequentialFile> file;
    s = ioptions_.env->NewSequentialFile(fname, &file,
                                         ioptions_.env->OptimizeForLogRead(soptions_));
    if (!s.ok()) {
      return s;
    }
    file_reader.reset(new SequentialFileReader(
        std::move(file), fname, ioptions_.log_readahead_size));
  }
   // Create the log reader.
  LogReporter reporter;
  reporter.env = ioptions_.env;
  reporter.info_log = ioptions_.info_log.get();
  reporter.fname = fname.c_str();
  if (!ioptions_.paranoid_checks ||
      ioptions_.wal_recovery_mode ==
          WALRecoveryMode::kSkipAnyCorruptedRecords) {
    reporter.status = nullptr;
  } else {
    reporter.status = &s;
  }
  // We intentially make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  *log_reader = new log::Reader(ioptions_.info_log, std::move(file_reader),
                                &reporter, true /*checksum*/, log_number);
  return s;
}

std::string FastRecovery::ColumnFamilyName(size_t i) {
  if (i == 0) {
    return rocksdb::kDefaultColumnFamilyName;
  } else {
    char name[100];
    snprintf(name, sizeof(name), "column_family_name_%06zu", i);
    return std::string(name);
  }
}

void FastRecovery::CloseDB() {
  for (auto handle : handles_) {
    delete handle;
  }
  handles_.clear();
  db_->Close();
  delete db_;
  logs_number_.clear();
  k2v.clear();
  //if (output_dir_) {
  //  delete output_dir_.get();
  //}
}

/*
Status FastRecovery::ReadWalsToBuffer() {
  fprintf(stdout, "ReadWalsToBuffer begin.\n");
  Status s;
  uint64_t start = ioptions_.env->NowMicros(), parse_add_last = 0, construct_last = 0, map_last = 0;
  uint64_t total_entry = 0, short_entry, reader_failed = 0, parse_failed = 0;

  for (uint32_t i = 0; i < logs_number_.size(); i++) {
    uint64_t log_number = logs_number_[i];
    log::Reader *reader = nullptr;
    s = GetLogReader(log_number, &reader);
    if (!s.ok() || !reader) {
      fprintf(stdout, "GetLogReader : %s.\n", s.ToString().c_str());
      fprintf(stdout, "log %lu skipped since log Reader null.\n", log_number);
      reader_failed++;
      continue;
    }
    std::string scratch;
    Slice record;
    WriteBatch batch;
    SequenceNumber sequence;
    while (true) {
      if (!s.ok()) {
        fprintf(stdout, "ReadRecord break : %s.\n", s.ToString().c_str());
        break;
      }
      if (!reader->ReadRecord(&record, &scratch, ioptions_.wal_recovery_mode)) {
        break;
      }
      total_entry++;
      if (record.size() < WriteBatchInternal::kHeader) {
        short_entry++;
        continue;
      }
      WriteBatchInternal::SetContents(&batch, record);
      sequence = WriteBatchInternal::Sequence(&batch);

      uint64_t parse_start = ioptions_.env->NowMicros();
      s = ParseBatchAndAddToMap(record, sequence, construct_last, map_last);
      uint64_t parse_end = ioptions_.env->NowMicros();
      parse_add_last += parse_end - parse_start;
      if (!s.ok()) {
        parse_failed++;
        fprintf(stdout, "ParseBatchAndAddToMap : %s.\n", s.ToString().c_str());
      }
    }
    delete reader;
    reader = nullptr;
  }

  uint64_t end = ioptions_.env->NowMicros();
  fprintf(stdout, "ReadWalsToBuffer last %lu s, parse add: %lu s, construct: %lu, map: %lu\n",
                  (end - start) / 1000000,
                  parse_add_last / 1000000,
                  construct_last / 1000000,
                  map_last / 1000000);
  uint64_t map_count = 0;
  for (auto &kv : k2v) {
    map_count += kv.size();
    fprintf(stdout, "k2v size: %lu\n", kv.size());
  }
  fprintf(stdout, "total: %lu, map: %lu, short: %lu, reader fail: %lu, parse fail: %lu\n",
                  total_entry, map_count, short_entry, reader_failed, parse_failed);
  return s;
}

Status FastRecovery::TestReadLatency() {
  Status s;
  uint64_t start = ioptions_.env->NowMicros();
  uint64_t entry_count = 0;

  for (uint32_t i = 0; i < logs_number_.size(); i++) {
    uint64_t wal_start = ioptions_.env->NowMicros();
    uint64_t log_number = logs_number_[i];
    log::Reader *reader = nullptr;
    s = GetLogReader(log_number, &reader);
    uint64_t get_reader_end = ioptions_.env->NowMicros();
    if (!s.ok()) {
      fprintf(stdout, "GetLogReader : %s.\n", s.ToString().c_str());
      continue;
    }
    if (!reader) {
      fprintf(stdout, "log %lu skipped since log Reader null.\n", log_number);
      continue;
    }
    std::string scratch;
    Slice record;
    while (true) {
      if (!s.ok()) {
        fprintf(stdout, "ReadRecord break : %s.\n", s.ToString().c_str());
        break;
      }
      uint64_t read_begin = ioptions_.env->NowMicros();
      if (!reader->ReadRecord(&record, &scratch, ioptions_.wal_recovery_mode)) {
        fprintf(stdout, "ReadRecord fail %lu.\n", log_number);
        break;
      }
      entry_count++;
    }
  }
  uint64_t end = ioptions_.env->NowMicros();
  fprintf(stdout, "TestReadLatency %lu s to read %lu wals, entries: %lu.\n",
                  (end - start) / 1000000, logs_number_.size(), entry_count);
  return s;
}

Status FastRecovery::TestReadAndParseLatency() {
  Status s;
  uint64_t start = ioptions_.env->NowMicros();
  uint64_t entry_count = 0, kv_count = 0, retry_count = 0, short_count = 0;
  uint64_t loop_count = 0, print_count = 0, non_ok_count = 0;

  for (uint32_t i = 0; i < logs_number_.size(); i++) {
    uint64_t log_number = logs_number_[i];
    log::Reader *reader = nullptr;
    s = GetLogReader(log_number, &reader);
    uint64_t get_reader_end = ioptions_.env->NowMicros();
    if (!s.ok()) {
      fprintf(stdout, "GetLogReader : %s.\n", s.ToString().c_str());
      continue;
    }
    if (!reader) {
      fprintf(stdout, "log %lu skipped since log Reader null.\n", log_number);
      continue;
    }
    std::string scratch;
    Slice record;
    while (true) {
      if (!s.ok()) {
        fprintf(stdout, "ReadRecord break : %s.\n", s.ToString().c_str());
        break;
      }
      uint64_t read_begin = ioptions_.env->NowMicros();
      if (!reader->ReadRecord(&record, &scratch, ioptions_.wal_recovery_mode)) {
        fprintf(stdout, "ReadRecord non-ok %lu.\n", log_number);
        break;
      }
      entry_count++;
      if (record.size() < WriteBatchInternal::kHeader) {
        short_count++;
        continue;
      }

      record.remove_prefix(WriteBatchInternal::kHeader);
      Slice key, value, blob, xid;
      // Sometimes a sub-batch starts with a Noop. We want to exclude such Noops as
      // the batch boundary symbols otherwise we would mis-count the number of
      // batches. We do that by checking whether the accumulated batch is empty
      // before seeing the next Noop.
      char tag = 0;
      uint32_t column_family = 0;  // default
       bool last_was_try_again = false;
      while ( (s.ok() && !record.empty()) || UNLIKELY(s.IsTryAgain()) ) {
        if (LIKELY(!s.IsTryAgain())) {
          last_was_try_again = false;
          tag = 0;
          column_family = 0;  // default
          s = ReadRecordFromWriteBatch(&record, &tag, &column_family, &key, &value,
                                       &blob, &xid);
          kv_count++;
          if (!s.ok()) {
            non_ok_count++;
          } else if (print_count < 2) {
            print_count++;
            fprintf(stdout, "key %lu:\n%s\n"
                            "value %lu:\n%s.\n",
                            key.ToString().length(), key.ToString().c_str(),
                            value.ToString().length(), value.ToString().c_str());
          }
        } else {
          retry_count++;
          assert(s.IsTryAgain());
          assert(!last_was_try_again); // to detect infinite loop bugs
          if (UNLIKELY(last_was_try_again)) {
            loop_count++;
            return Status::Corruption(
                "two consecutive TryAgain in WriteBatch handler; this is either a "
                "software bug or data corruption.");
          }
          last_was_try_again = true;
          s = Status::OK();
        }
      }
    }
  }
  uint64_t end = ioptions_.env->NowMicros();
  fprintf(stdout, "TestReadAndParseLatency %lu s to read and parse %lu wals,"
                  " entries: %lu, kvs: %lu, retry: %lu, loop: %lu, short: %lu, non-ok: %lu.\n",
                  (end - start) / 1000000, logs_number_.size(),
                  entry_count, kv_count, retry_count, loop_count,
                  short_count, non_ok_count);
  return s;
}

Status FastRecovery::TestSstKV() {
  Status s;

  for (ColumnFamilyData* cfd : *db_->GetVersionSet()->GetColumnFamilySet()) {
    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }
    const std::vector<FileMetaData*>& level0_files = cfd->current()->storage_info()->LevelFiles(0);
    for (FileMetaData* file : level0_files) {
      if (!file) {continue;}
      uint64_t sst_number = file->fd.GetNumber();
      uint32_t path_id = file->fd.GetPathId();
      std::string sst_name = TableFileName(ioptions_.db_paths, sst_number, path_id);
      
      // SST reader
      std::unique_ptr<TableReader> table_reader;
      s = GetTableReader(sst_name, &table_reader);
      if (!s.ok()) {
        fprintf(stdout, "GetTableReader : %s.\n", s.ToString().c_str());
        continue;
      }

      // Scan all the keys in SST
      std::unique_ptr<InternalIterator> iter(table_reader->NewIterator(
          ReadOptions(), mcfoptions_.prefix_extractor.get(), nullptr,
          false, TableReaderCaller::kFastRecovery));

      uint64_t print_count = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        if (!iter->status().ok()) {
          continue;
        }
        if (print_count > 1) {continue;}
        print_count++;
        std::string user_key = ExtractUserKey(iter->key()).ToString(true);
        fprintf(stdout, "original length: %lu, user length: %lu, "
                        "original key: %s.\n"
                        "key: %s.\n"
                        "value length: %lu.\n"
                        "value: %s\n",
                        iter->key().ToString().length(),
                        user_key.length(),
                        iter->key().ToString().c_str(),
                        user_key.c_str(),
                        iter->value().ToString().length(),
                        iter->value().ToString().c_str());
      }
      break;
    }
    break;
  }
  return s;
}
*/

/*
Status FastRecovery::BuildTableFromWals(TableBuilder* builder, InternalIterator* iter,
                                                   uint32_t cf_id, uint64_t sst_number) {
  if (!builder || !iter) {
    return Status::Aborted(Slice("TableBuilder or iter null.\n"));
  }
  fprintf(stdout, "BuildTableFromWals %lu begin*****.\n", sst_number);
  Status s;
  uint64_t read_last = 0, parseadd_last = 0, parse_last = 0, add_last = 0, find_last = 0;
  uint64_t start = ioptions_.env->NowMicros();
  std::set<std::string> all_keys;
  uint64_t keys_count = 0, keys_missed_iter = 0, keys_missed_wals = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    keys_count++;
    if (!iter->status().ok()) {
      keys_missed_iter++;
      continue;
    }
    std::string user_key = ExtractUserKey(iter->key()).ToString();
    all_keys.insert(user_key);
    //fprintf(stdout, "%s-%s  ", user_key.c_str(), iter->key().ToString().c_str());
  }
  uint64_t scan_sst_end = ioptions_.env->NowMicros();
  fprintf(stdout, "%lu / %lu keys in SST %lu, %lu ms to scan them.\n",
                  all_keys.size(), keys_count, sst_number,
                  (scan_sst_end - start) / 1000);

  for (uint32_t i = 0; i < logs_number_.size(); i++) {
    uint64_t wal_start = ioptions_.env->NowMicros();
    uint64_t log_number = logs_number_[i];
    log::Reader *reader = nullptr;
    s = GetLogReader(log_number, &reader);
    uint64_t get_reader_end = ioptions_.env->NowMicros();
    if (!s.ok()) {
      fprintf(stdout, "GetLogReader : %s.\n", s.ToString().c_str());
      continue;
    }
    if (!reader) {
      fprintf(stdout, "log %lu skipped since log Reader null.\n", log_number);
      continue;
    }
    //fprintf(stdout, "No.%u reader log number: %lu.\n", i, log_number);
    std::string scratch;
    Slice record;
    WriteBatch batch;
    SequenceNumber sequence;
    std::set<std::string> keys_found;
    while (true) {
      if (!s.ok()) {
        fprintf(stdout, "ReadRecord break : %s.\n", s.ToString().c_str());
        break;
      }
      uint64_t read_begin = ioptions_.env->NowMicros();
      if (!reader->ReadRecord(&record, &scratch, ioptions_.wal_recovery_mode)) {
        break;
      }
      uint64_t read_end = ioptions_.env->NowMicros();
      read_last += read_end - read_begin;
      if (record.size() < WriteBatchInternal::kHeader) {
        continue;
      }
      WriteBatchInternal::SetContents(&batch, record);
      sequence = WriteBatchInternal::Sequence(&batch);

      uint64_t parse_begin = ioptions_.env->NowMicros();
      s = ParseBatchAndAddKV(record, builder, all_keys, keys_found, cf_id, sequence,
                             parse_last, add_last, find_last);
      uint64_t parse_end = ioptions_.env->NowMicros();
      parseadd_last += parse_end - parse_begin;
      if (!s.ok()) {
        fprintf(stdout, "ParseBatchAndAddKV : %s.\n", s.ToString().c_str());
      }
    }
    //fprintf(stdout, "keys_found in log %lu: %lu.\n", log_number, keys_found.size());
    for (auto& key : keys_found) {
      all_keys.erase(key);
    }
    if (all_keys.empty()) { break; }
    delete reader;
    reader = nullptr;
    uint64_t wal_end = ioptions_.env->NowMicros();
    //fprintf(stdout, "wal %lu last %lu s.\n", log_number,
    //                (wal_end - wal_start) / 1000000);
  }
  keys_missed_wals = all_keys.size();
  uint64_t wals_end = ioptions_.env->NowMicros();
  fprintf(stdout, "%lu SST Total keys: %lu, invalid iters: %lu, missed keys in wal: %lu,"
                  " %lu s to recover, \n",
                  sst_number, keys_count, keys_missed_iter, keys_missed_wals,
                  (wals_end - scan_sst_end) / 1000000);

  s = builder->Finish();
  uint64_t end = ioptions_.env->NowMicros();
  fprintf(stdout, "BuildTableFromWals last %lu s, Finish last %lu s, read %lu s, parseadd %lu s"
                  " parse: %lu s, add: %lu s, find: %lu s.\n",
                  (end - start) / 1000000,
                  (end - wals_end) / 1000000,
                  read_last / 1000000, parseadd_last / 1000000,
                  parse_last / 1000000, add_last / 1000000,
                  find_last / 1000000);
  if (!s.ok()) {
    fprintf(stdout, "Finish : %s.\n", s.ToString().c_str());
  }
  fprintf(stdout, "BuildTableFromWals %lu end*****.\n", sst_number);
  return s;
}

Status FastRecovery::PrepareLogReaders(std::vector<log::Reader*>& log_readers) {
  Status s, ret;
  //fprintf(stdout, "PrepareLogReaders begin.\n");
  for (uint32_t i = 0; i < logs_number_.size(); i++) {
    uint64_t log_number = logs_number_[i];
    // Open the log file
    std::string fname = LogFileName(ioptions_.wal_dir, log_number);
    std::unique_ptr<SequentialFileReader> file_reader;
    {
      std::unique_ptr<SequentialFile> file;
      s = ioptions_.env->NewSequentialFile(fname, &file,
                                       ioptions_.env->OptimizeForLogRead(soptions_));
      if (!s.ok()) {
        // Fail with one log file, but that's ok.
        // Try next one.
        ret = s;
        continue;
      }
      file_reader.reset(new SequentialFileReader(
          std::move(file), fname, ioptions_.log_readahead_size));
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = ioptions_.env;
    reporter.info_log = ioptions_.info_log.get();
    reporter.fname = fname.c_str();
    if (!ioptions_.paranoid_checks ||
        ioptions_.wal_recovery_mode ==
            WALRecoveryMode::kSkipAnyCorruptedRecords) {
      reporter.status = nullptr;
    } else {
      reporter.status = &s;
    }
    // We intentially make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    log::Reader reader(ioptions_.info_log, std::move(file_reader),
                             &reporter, true, log_number);
    log_readers[i] = &reader;
  }
  for (uint32_t i = 0; i < log_readers.size(); i++) {
    if (log_readers[i] == nullptr) {continue;}
    fprintf(stdout, "No.%u log number: %lu.  ", i, log_readers[i]->GetLogNumber());
  }
  fprintf(stdout, "\n");
  return ret;
}

Status FastRecovery::ParseBatchAndAddKV(Slice& record, TableBuilder* builder,
                                                   std::set<std::string>& all_keys,
                                                   std::set<std::string>& keys_found,
                                                   uint32_t cf_id,
                                                   SequenceNumber sequence,
                                                   uint64_t& parse_last,
                                                   uint64_t& add_last,
                                                   uint64_t& find_last) {
  //fprintf(stdout, "ParseBatchAndAddKV begin\n");
  if (record.size() < WriteBatchInternal::kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  record.remove_prefix(WriteBatchInternal::kHeader);
  Slice key, value, blob, xid;
  // Sometimes a sub-batch starts with a Noop. We want to exclude such Noops as
  // the batch boundary symbols otherwise we would mis-count the number of
  // batches. We do that by checking whether the accumulated batch is empty
  // before seeing the next Noop.
  bool empty_batch = true;
  int found = 0;
  Status s;
  char tag = 0;
  uint32_t column_family = 0;  // default
  bool last_was_try_again = false;
  uint64_t begin = ioptions_.env->NowMicros();
  uint64_t tmp_add_last = 0;
  while ( (s.ok() && !record.empty()) || UNLIKELY(s.IsTryAgain()) ) {
    if (LIKELY(!s.IsTryAgain())) {
      last_was_try_again = false;
      tag = 0;
      column_family = 0;  // default

      s = ReadRecordFromWriteBatch(&record, &tag, &column_family, &key, &value,
                                   &blob, &xid);
      if (!s.ok()) {
        return s;
      }
      if (column_family != cf_id) {
        return Status::OK();
      }
      uint64_t find_begin = ioptions_.env->NowMicros();
      std::string user_key = key.ToString();
      if (all_keys.count(user_key)) {
        if (!keys_found.count(user_key)) {
          keys_found.insert(user_key);
        }
        find_last += ioptions_.env->NowMicros() - find_begin;
      } else {
        find_last += ioptions_.env->NowMicros() - find_begin;
        continue;
      }
    } else {
      assert(s.IsTryAgain());
      assert(!last_was_try_again); // to detect infinite loop bugs
      if (UNLIKELY(last_was_try_again)) {
        return Status::Corruption(
            "two consecutive TryAgain in WriteBatch handler; this is either a "
            "software bug or data corruption.");
      }
      last_was_try_again = true;
      s = Status::OK();
    }

    uint64_t add_begin = ioptions_.env->NowMicros();
    InternalKey ikey;
    switch (tag) {
      case kTypeColumnFamilyValue: // If cf mismatch, it will not get here.
      case kTypeValue:
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        ikey.Set(key, sequence, kTypeValue);
        builder->Add(ikey.Encode(), value, true);
        break;
      case kTypeColumnFamilyDeletion:
      case kTypeDeletion:
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        ikey.Set(key, sequence, kTypeDeletion);
        builder->Add(ikey.Encode(), value, true);
        break;
      case kTypeColumnFamilySingleDeletion:
      case kTypeSingleDeletion:
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        ikey.Set(key, sequence, kTypeSingleDeletion);
        builder->Add(ikey.Encode(), value, true);
        break;
      case kTypeColumnFamilyRangeDeletion:
      case kTypeRangeDeletion:
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        ikey.Set(key, sequence, kTypeRangeDeletion);
        builder->Add(ikey.Encode(), value, true);
        break;
      case kTypeColumnFamilyMerge:
      case kTypeMerge:
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        ikey.Set(key, sequence, kTypeMerge);
        builder->Add(ikey.Encode(), value, true);
        break;
      case kTypeColumnFamilyBlobIndex:
      case kTypeBlobIndex:
        if (LIKELY(s.ok())) {
          found++;
        }
        break;
      case kTypeLogData:
        // handler->LogData(blob);
        // A batch might have nothing but LogData. It is still a batch.
        empty_batch = false;
        break;
      case kTypeBeginPrepareXID:
        empty_batch = false;
        break;
      case kTypeBeginPersistedPrepareXID:
        empty_batch = false;
        break;
      case kTypeBeginUnprepareXID:
        empty_batch = false;
        break;
      case kTypeEndPrepareXID:
        empty_batch = true;
        break;
      case kTypeCommitXID:
        empty_batch = true;
        break;
      case kTypeRollbackXID:
        empty_batch = true;
        break;
      case kTypeNoop:
        empty_batch = true;
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
    uint64_t add_end = ioptions_.env->NowMicros();
    tmp_add_last += add_end - add_begin;
  }
  uint64_t end = ioptions_.env->NowMicros();
  parse_last += end - begin - tmp_add_last;
  add_last += tmp_add_last;
  (void)empty_batch;
  (void)found;
  return s;
}

*/

int RecoveryTool::Run(int argc, char** argv, Options options) {
  std::string dbname;
  std::string wal_dir;
  std::string output_dir;
  std::string path0;
  std::string path1;
  const std::string default_dbname = "/home/zhangxin/test-rocksdb/";
  const std::string default_waldir = "/home/zhangxin/test-rocksdb/";
  const std::string default_path0 = "/data2/zhangxin/ssd/path0/";
  const std::string default_path1 = "/data2/zhangxin/ssd/path1/";
  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--dbname=", 9) == 0) {
      dbname = argv[i] + 9;
    } else if (strncmp(argv[i], "--wal_dir=", 10) == 0) {
      wal_dir = argv[i] + 10;
    } else if (strncmp(argv[i], "--output_dir=", 13) == 0) {
      output_dir = argv[i] + 13;
    } else if (strncmp(argv[i], "--path0=", 8) == 0) {
      path0 = argv[i] + 8;
    } else if (strncmp(argv[i], "--path1=", 8) == 0) {
      path1 = argv[i] + 8;
    }
  }

  if (dbname.length() == 0) {
    dbname = default_dbname;
  }
  if (wal_dir.length() == 0) {
    wal_dir = dbname;
  }
  if (output_dir.length() == 0) {
    output_dir = dbname[dbname.length() - 1] == '/' ?
                                                dbname + "data_recovered" :
                                                dbname + "/data_recovered";
  }
  if (path0.length() == 0) {
    path0 = default_path0;
  }
  if (path1.length() == 0) {
    path1 = default_path1;
  }
  if (path0[path0.length() - 1] != '/') {
    path0 += '/';
  }
  if (path1[path1.length() - 1] != '/') {
    path1 += '/';
  }

  fprintf(stdout, "dbname: %s, wal_dir: %s, path0: %s, path1: %s, output_dir: %s.\n",
          dbname.c_str(), wal_dir.c_str(), path0.c_str(), path1.c_str(), output_dir.c_str());

  // Options要与db_bench运行时的参数保持一致
  uint32_t num_column_families = 3;
  options.max_background_jobs = 8;
  options.max_bytes_for_level_base = 512l * 1024 * 1024;
  options.write_buffer_size = 128l * 1024 * 1024;
  options.level0_file_num_compaction_trigger = 4;
  options.target_file_size_base = 8 * 1024 * 1024;
  options.use_wal_stage = true;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.disable_auto_compactions = true;
  options.wal_dir = wal_dir;
  options.db_paths = std::vector<DbPath>();
  options.db_paths.push_back(DbPath(path0, 2l * options.max_bytes_for_level_base));
  options.db_paths.push_back(DbPath(path1, 100l * options.max_bytes_for_level_base));

  rocksdb::FastRecovery recoverer(options, dbname, output_dir, num_column_families);

  Status s = recoverer.OpenDB();
  if (!s.ok()) {
    fprintf(stdout, "Open db failed: %s.\n", s.ToString().c_str());
    return -1;
  }

  //sleep(10);

  uint64_t t_begin = recoverer.ioptions_.env->NowMicros();
  fprintf(stdout, "Before recover.\n");
  s = recoverer.Recover();
  fprintf(stdout, "After recover.\n");
  uint64_t t_end = recoverer.ioptions_.env->NowMicros();
  if (!s.ok()) {
    fprintf(stdout, "Recover : %s.\n", s.ToString().c_str());
  }
  fprintf(stdout, "Recover last %lu s.\n", (t_end - t_begin) / 1000000);

  recoverer.CloseDB();

  return 0;
}
}  // namespace rocksdb

#endif  // ROCKSDB_LITE

