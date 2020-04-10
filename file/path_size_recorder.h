//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <unordered_map>

#include "port/port.h"
#include "util/mutexlock.h"
#include "rocksdb/status.h"
#include "include/rocksdb/env.h"

namespace rocksdb {

class PathSizeRecorder {
public:
    PathSizeRecorder() = default;

    Status OnAddFile(const std::string& file_path, uint32_t path_id) {
        uint64_t file_size;
        Status s = env_->GetFileSize(file_path, &file_size);
        if (s.ok()) {
            MutexLock l(&mu_);
            auto tracked_file = tracked_files_.find(file_path);
            if (tracked_file != tracked_files_.end()) {
                // File was added before, we will just update the size
                auto path_size = paths_size_.find(path_id);
                assert(path_size != paths_size_.end());
                path_size->second -= (tracked_file->second).second;
                path_size->second += file_size;
            } else {
                paths_size_[path_id] += file_size;
            }
            tracked_files_[file_path] = 
                std::make_pair<uint32_t, uint64_t>(path_id, file_size);
        }
        return s;
    }

    void OnDeleteFile(const std::string& file_path, uint32_t path_id) {
        MutexLock l(&mu_);
        auto tracked_file = tracked_files_.find(file_path);
        if (tracked_file == tracked_files_.end()) {
            // File is not tracked
            return;
        }
        auto path_size = paths_size_.find(path_id);
        assert(path_size != paths_size_.end());
        path_size->second -= (tracked_file->second).second;
        tracked_files_.erase(tracked_file);
    }

    uint64_t GetPathSizeByPathId(uint32_t path_id) {
        MutexLock lk(&mu_);
        auto path_size = paths_size_.find(path_id);
        if (path_size == paths_size_.end())
            return 0;
        return path_size->second;
    }

private:
    Env* env_;
    // Mutex to protect tracked_files_, total_files_size_
    port::Mutex mu_;
    // A map containing all tracked files and there path id & sizes
    //  file_path => (path id, file_size).
    std::unordered_map<std::string, std::pair<uint32_t, uint64_t>> tracked_files_;
    // A map from path id to sum of file size in the path indicated by path id.
    std::unordered_map<uint32_t, uint64_t> paths_size_;
};

} // namespace rocksdb