
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
extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;

Status RecoverTableFile(FastRecovery* recoverer ,FileMetaData*         meta, uint32_t cf_id) {
  Status s;
  // Create Reader for all wal files
  std::vector<log::Reader*> log_readers(recoverer->logs_number_.size(), nullptr);
  s = recoverer->PrepareLogReaders(log_readers); // TODO: Ignoring some errors when creating Reader
  if (!s.ok()) {
    fprintf(stderr, "PrepareLogReaders : %s.\n", s.ToString().c_str());
    // return s;
  }

  uint64_t sst_number = meta->fd.GetNumber();
  uint32_t path_id = meta->fd.GetPathId();
  std::string sst_name = TableFileName(recoverer->ioptions_.db_paths, sst_number, path_id);

  // SST reader
  std::unique_ptr<TableReader> table_reader;
  s = recoverer->GetTableReader(sst_name, &table_reader);
  if (!s.ok()) {
    fprintf(stderr, "GetTableReader : %s.\n", s.ToString().c_str());
    return s;
  }

  // SST writer
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
  s = recoverer->BuildTableFromWals(table_builder.get(), iter.get(), log_readers, cf_id);
  if (!s.ok()) {
    fprintf(stderr, "BuildTableFromWals : %s.\n", s.ToString().c_str());
  }
  return s;
}

FastRecovery::FastRecovery(const Options& options, const std::string& dbname,
    const std::string& output_path, int num_column_families)
    : dbname_(dbname),
      output_path_(output_path),
      num_column_families_(num_column_families),
      options_(options),
      icfoptions_(options),
      mcfoptions_(ColumnFamilyOptions(options_)),
      internal_comparator_(BytewiseComparator()) {
  for (size_t i = 0; i < num_column_families_; i++) {
    column_families_.push_back(ColumnFamilyDescriptor(
      ColumnFamilyName(i), ColumnFamilyOptions(options)));
  }
}

Status FastRecovery::OpenDB() {
  Status s = DB::Open(options_, dbname_, column_families_, &handles_, &db_, true/*skip_wal*/);
  if (!s.ok()) {
    fprintf(stderr, "DB::Open : %s.\n", s.ToString().c_str());
    return s;
  }
  s = DBImpl::CreateAndNewDirectory(ioptions_.env, output_path_, &output_dir_);
  if (!s.ok()) {
    fprintf(stderr, "CreateAndNewDirectory : %s.\n", s.ToString().c_str());
    return s;
  }

  // Get wal files from wal_dir
  std::vector<std::string> filenames;
  s = ioptions_.env->GetChildren(ioptions_.wal_dir, &filenames);
  if (s.IsNotFound()) {
    return Status::InvalidArgument("wal_dir not found", ioptions_.wal_dir);
  } else if (!s.ok()) {
    fprintf(stderr, "GetChildren : %s.\n", s.ToString().c_str());
    return s;
  }
  for (size_t i = 0; i < filenames.size(); i++) {
    uint64_t number;
    FileType type;
    if (ParseFileName(filenames[i], &number, &type) && type == kLogFile) {
      logs_number_.push_back(number);
    }
  }
  return s;
}

Status FastRecovery::Recover() {
  Status s;
  std::vector<std::future<Status> > statuss;

  for (ColumnFamilyData* cfd : *db_->GetVersionSet()->GetColumnFamilySet()) {
    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }
    assert(cfd->NumberLevels() > 1);
    const std::vector<FileMetaData*>& level0_files = cfd->current()->storage_info()->LevelFiles(0);
    const std::vector<FileMetaData*>& level1_files = cfd->current()->storage_info()->LevelFiles(1);
    /*
    std::vector<std::thread> threads;
    for (auto file : level0_files) {
      if (!file) {continue;}
      
      //std::pair<FileMetaData*, uint32_t> arg(file, cfd->GetID());
      //int rc = pthread_create(&threads[index], NULL, MultiThread, (void*)&arg);
      threads.emplace_back(RecoverTableFile, file, cfd->GetID());
    }
    for (auto file : level1_files) {
      if (!file) {continue;}
      //std::pair<FileMetaData*, uint32_t> arg(file, cfd->GetID());
      //int rc = pthread_create(&threads[index], NULL, MultiThread, (void*)&arg);
      threads.emplace_back(RecoverTableFile, file, cfd->GetID());
    }
    for (auto& thread: threads) {
      thread.join();
    }
    */
    /*
    for (FileMetaData* file : level0_files) {
      if (!file) {continue;}
      uint64_t sst_number = file->fd.GetNumber();
      uint32_t path_id = file->fd.GetPathId();
      uint32_t cfd_id = cfd->GetID();
      statuss.emplace_back(std::async(std::launch::async, FastRecovery::RecoverTableFile, 
                                       //std::move(*file), std::move(cfd->GetID())));
                                       static_cast<uint64_t&&>(sst_number),
                                       static_cast<uint32_t&&>(path_id),
                                       static_cast<uint32_t&&>(cfd_id)));
    }
    */
    for (FileMetaData* file : level0_files) {
      if (!file) {continue;}
      FileMetaData _file = *file;
      statuss.emplace_back(std::async(std::launch::async, RecoverTableFile,
                                       this, &_file, cfd->GetID()));
    }
    for (auto & status : statuss) {
      Status ret_status = status.get();
      if (!ret_status.ok()) {

      }
    }
  }
  if (output_dir_) {
    s = output_dir_->Fsync();
  }
  return s;
}

/*
Status FastRecovery::MultiThread(void *arg) {
  auto* p = static_cast<std::pair<FileMetaData*, uint32_t> >(arg);
  return RecoverTableFile(p->first, p->second);
}
*/

Status FastRecovery::PrepareLogReaders(std::vector<log::Reader*>& log_readers) {
  Status s, ret;
  uint32_t index = 0;
  for (auto log_number : logs_number_) {
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
                             &reporter, true /*checksum*/, log_number);
    log_readers[index++] = &reader;
  }
  return ret;
}

Status FastRecovery::BuildTableFromWals(TableBuilder* builder, InternalIterator* iter,
                                                   std::vector<log::Reader*> log_readers, uint32_t cf_id) {
  if (!builder || !iter) {
    return Status::Aborted(Slice("TableBuilder or iter null.\n"));
  }
  Status s;
  std::set<std::string> all_keys;
  uint64_t keys_count = 0, keys_missed_iter = 0, keys_missed_wals = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    keys_count++;
    if (!iter->status().ok()) {
      keys_missed_iter++;
      continue;
    }
    all_keys.insert(iter->key().ToString());
  }

  for (log::Reader* reader : log_readers) {
    uint64_t log_number = reader->GetLogNumber();
    //LogReporter* reporter = reader->GetReporter();
    std::string fname = LogFileName(ioptions_.wal_dir, log_number);
    std::string scratch;
    Slice record;
    WriteBatch batch;
    std::set<std::string> keys_found;
    while (reader->ReadRecord(&record, &scratch, ioptions_.wal_recovery_mode) && s.ok()) {
      if (record.size() < WriteBatchInternal::kHeader) {
        //reporter->Corruption(record.size(), Status::Corruption("log record too small"));
        continue;
      }
      WriteBatchInternal::SetContents(&batch, record);
      s = ParseBatchAndAddKV(record, builder, all_keys, keys_found, cf_id);
      if (!s.ok()) {
        fprintf(stderr, "ParseBatchAndAddKV : %s.\n", s.ToString().c_str());
      }

      for (auto& key : keys_found) {
        all_keys.erase(key);
      }
      if (all_keys.empty()) { break; }
    }
  }
  s = builder->Finish();
  if (!s.ok()) {
    fprintf(stderr, "Finish : %s.\n", s.ToString().c_str());
  }
  return s;
}

Status FastRecovery::ParseBatchAndAddKV(Slice& record, TableBuilder* builder,
                                                   std::set<std::string>& all_keys,
                                                   std::set<std::string>& keys_found,
                                                   uint32_t cf_id) {
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
  while (((s.ok() && !record.empty()) || UNLIKELY(s.IsTryAgain()))) {
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
      std::string key_str = key.ToString();
      if (all_keys.count(key_str) && !keys_found.count(key_str)) {
        keys_found.insert(key_str);
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

    switch (tag) {
      case kTypeColumnFamilyValue:
      case kTypeValue:
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        builder->Add(key, value);
        break;
      case kTypeColumnFamilyDeletion:
      case kTypeDeletion:
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        builder->Add(key, value);
        break;
      case kTypeColumnFamilySingleDeletion:
      case kTypeSingleDeletion:
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        builder->Add(key, value);
        break;
      case kTypeColumnFamilyRangeDeletion:
      case kTypeRangeDeletion:
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        builder->Add(key, value);
        break;
      case kTypeColumnFamilyMerge:
      case kTypeMerge:
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        builder->Add(key, value);
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
  }
  (void)empty_batch;
  (void)found;
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

std::string FastRecovery::ColumnFamilyName(size_t i) {
  if (i == 0) {
    return rocksdb::kDefaultColumnFamilyName;
  } else {
    char name[100];
    snprintf(name, sizeof(name), "column_family_name_%06zu", i);
    return std::string(name);
  }
}

namespace {
void print_help() {
  fprintf(stderr,
          R"(sst_dump --file=<data_dir_OR_sst_file> [--command=check|scan|raw]
    --file=<data_dir_OR_sst_file>
      Path to SST file or directory containing SST files

    --output_hex
      Can be combined with scan command to print the keys and values in Hex
)");
}

}  // namespace

int RecoveryTool::Run(int argc, char** argv, Options options) {
  std::string dbname;
  std::string wal_dir;
  std::string output_dir;
  std::string path1;
  std::string path2;
  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--dbname=", 5) == 0) {
      dbname = argv[i] + 5;
    } else if (strncmp(argv[i], "--wal_dir=", 10) == 0) {
      wal_dir = argv[i] + 10;
    } else if (strncmp(argv[i], "--output_dir=", 13) == 0) {
      output_dir = argv[i] + 13;
    } else if (strncmp(argv[i], "--path1=", 8) == 0) {
      path1 = argv[i] + 8;
    } else if (strncmp(argv[i], "--path2=", 8) == 0) {
      path2 = argv[i] + 8;
    }
  }

  if (dbname.length() == 0) {
    dbname = "/home/zhangxin/test-rocksdb/";
  }
  if (wal_dir.length() == 0) {
    wal_dir = dbname;
  }
  if (output_dir.length() == 0) {
    output_dir = dbname[dbname.length() - 1] == '/' ?
                                                dbname + "data_recovered" :
                                                dbname + "/data_recovered";
  }
  if (path1.length() == 0) {
    path1 = "/data2/zhangxin/ssd/path1/";
  }
  if (path2.length() == 0) {
    path2 = "/data2/zhangxin/ssd/path2/";
  }
  if (path1[path1.length() - 1] != '/') {
    path1 += '/';
  }
  if (path2[path2.length() - 1] != '/') {
    path2 += '/';
  }

  // Options要与db_bench运行时的参数保持一致
  int num_column_families = 3;
  options.max_background_jobs = 8;
  options.max_bytes_for_level_base = 512l * 1024 * 1024;
  options.write_buffer_size = 128l * 1024 * 1024;
  options.level0_file_num_compaction_trigger = 4;
  options.target_file_size_base = 8 * 1024 * 1024;
  options.use_wal_stage = true;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.wal_dir = wal_dir;
  options.db_paths = std::vector<DbPath>();
  options.db_paths.push_back(DbPath(path1, 2l * options.max_bytes_for_level_base));
  options.db_paths.push_back(DbPath(path2, 100l * options.max_bytes_for_level_base));

  rocksdb::FastRecovery recoverer(options, dbname, output_dir);

  Status s = recoverer.OpenDB();
  if (!s.ok()) {
    fprintf(stdout, "Open db failed: %s.\n", s.ToString().c_str());
    return -1;
  }
  s = recoverer.Recover();
  if (!s.ok()) {
    fprintf(stderr, "Recover : %s.\n", s.ToString().c_str());
  }

  return 0;
}
}  // namespace rocksdb

#endif  // ROCKSDB_LITE

