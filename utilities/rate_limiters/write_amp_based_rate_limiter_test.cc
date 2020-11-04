//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/rate_limiter.h"
#include "utilities/rate_limiters/write_amp_based_rate_limiter.h"

#include <cinttypes>
#include <chrono>
#include <limits>

#include "db/db_test_util.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/random.h"

namespace rocksdb {

// TODO(yhchiang): the rate will not be accurate when we run test in parallel.
class WriteAmpBasedRateLimiterTest : public testing::Test {};

TEST_F(WriteAmpBasedRateLimiterTest, OverflowRate) {
  WriteAmpBasedRateLimiter limiter(port::kMaxInt64, 1000, 10,
                                   RateLimiter::Mode::kWritesOnly,
                                   Env::Default(), false /* auto_tuned */);
  ASSERT_GT(limiter.GetSingleBurstBytes(), 1000000000ll);
}

TEST_F(WriteAmpBasedRateLimiterTest, StartStop) {
  std::unique_ptr<RateLimiter> limiter(
      NewWriteAmpBasedRateLimiter(100, 100, 10));
}

TEST_F(WriteAmpBasedRateLimiterTest, Modes) {
  for (auto mode : {RateLimiter::Mode::kWritesOnly,
                    RateLimiter::Mode::kReadsOnly, RateLimiter::Mode::kAllIo}) {
    WriteAmpBasedRateLimiter limiter(
        2000 /* rate_bytes_per_sec */, 1000 * 1000 /* refill_period_us */,
        10 /* fairness */, mode, Env::Default(), false /* auto_tuned */);
    limiter.Request(1000 /* bytes */, Env::IO_HIGH, nullptr /* stats */,
                    RateLimiter::OpType::kRead);
    if (mode == RateLimiter::Mode::kWritesOnly) {
      ASSERT_EQ(0, limiter.GetTotalBytesThrough(Env::IO_HIGH));
    } else {
      ASSERT_EQ(1000, limiter.GetTotalBytesThrough(Env::IO_HIGH));
    }

    limiter.Request(1000 /* bytes */, Env::IO_HIGH, nullptr /* stats */,
                    RateLimiter::OpType::kWrite);
    if (mode == RateLimiter::Mode::kAllIo) {
      ASSERT_EQ(2000, limiter.GetTotalBytesThrough(Env::IO_HIGH));
    } else {
      ASSERT_EQ(1000, limiter.GetTotalBytesThrough(Env::IO_HIGH));
    }
  }
}

#if !(defined(TRAVIS) && defined(OS_MACOSX))
TEST_F(WriteAmpBasedRateLimiterTest, Rate) {
  auto* env = Env::Default();
  struct Arg {
    Arg(int32_t _target_rate, int _burst)
        : limiter(NewWriteAmpBasedRateLimiter(_target_rate, 100 * 1000, 10)),
          request_size(_target_rate / 10),
          burst(_burst) {}
    std::unique_ptr<RateLimiter> limiter;
    int32_t request_size;
    int burst;
  };

  auto writer = [](void* p) {
    auto* thread_env = Env::Default();
    auto* arg = static_cast<Arg*>(p);
    // Test for 2 seconds
    auto until = thread_env->NowMicros() + 2 * 1000000;
    Random r((uint32_t)(thread_env->NowNanos() %
                        std::numeric_limits<uint32_t>::max()));
    while (thread_env->NowMicros() < until) {
      for (int i = 0; i < static_cast<int>(r.Skewed(arg->burst) + 1); ++i) {
        arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1,
                              Env::IO_HIGH, nullptr /* stats */,
                              RateLimiter::OpType::kWrite);
      }
      arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1, Env::IO_LOW,
                            nullptr /* stats */, RateLimiter::OpType::kWrite);
    }
  };

  for (int i = 1; i <= 16; i *= 2) {
    int32_t target = i * 1024 * 10;
    Arg arg(target, i / 4 + 1);
    int64_t old_total_bytes_through = 0;
    for (int iter = 1; iter <= 2; ++iter) {
      // second iteration changes the target dynamically
      if (iter == 2) {
        target *= 2;
        arg.limiter->SetBytesPerSecond(target);
      }
      auto start = env->NowMicros();
      for (int t = 0; t < i; ++t) {
        env->StartThread(writer, &arg);
      }
      env->WaitForJoin();

      auto elapsed = env->NowMicros() - start;
      double rate =
          (arg.limiter->GetTotalBytesThrough() - old_total_bytes_through) *
          1000000.0 / elapsed;
      old_total_bytes_through = arg.limiter->GetTotalBytesThrough();
      fprintf(stderr,
              "request size [1 - %" PRIi32 "], limit %" PRIi32
              " KB/sec, actual rate: %lf KB/sec, elapsed %.2lf seconds\n",
              arg.request_size - 1, target / 1024, rate / 1024,
              elapsed / 1000000.0);

      ASSERT_GE(rate / target, 0.75);
      ASSERT_LE(rate / target, 1.25);
    }
  }
}
#endif

TEST_F(WriteAmpBasedRateLimiterTest, LimitChangeTest) {
  // starvation test when limit changes to a smaller value
  int64_t refill_period = 1000 * 1000;
  auto* env = Env::Default();
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  struct Arg {
    Arg(int32_t _request_size, Env::IOPriority _pri,
        std::shared_ptr<RateLimiter> _limiter)
        : request_size(_request_size), pri(_pri), limiter(_limiter) {}
    int32_t request_size;
    Env::IOPriority pri;
    std::shared_ptr<RateLimiter> limiter;
  };

  auto writer = [](void* p) {
    auto* arg = static_cast<Arg*>(p);
    arg->limiter->Request(arg->request_size, arg->pri, nullptr /* stats */,
                          RateLimiter::OpType::kWrite);
  };

  for (uint32_t i = 1; i <= 16; i <<= 1) {
    int32_t target = i * 1024 * 10;
    // refill per second
    for (int iter = 0; iter < 2; iter++) {
      std::shared_ptr<RateLimiter> limiter =
          std::make_shared<WriteAmpBasedRateLimiter>(
              target, refill_period, 10, RateLimiter::Mode::kWritesOnly,
              Env::Default(), false /* auto_tuned */);
      rocksdb::SyncPoint::GetInstance()->LoadDependency(
          {{"WriteAmpBasedRateLimiter::Request",
            "WriteAmpBasedRateLimiterTest::LimitChangeTest:changeLimitStart"},
           {"WriteAmpBasedRateLimiterTest::LimitChangeTest:changeLimitEnd",
            "WriteAmpBasedRateLimiter::Refill"}});
      Arg arg(target, Env::IO_HIGH, limiter);
      // The idea behind is to start a request first, then before it refills,
      // update limit to a different value (2X/0.5X). No starvation should
      // be guaranteed under any situation
      // TODO(lightmark): more test cases are welcome.
      env->StartThread(writer, &arg);
      int32_t new_limit = (target << 1) >> (iter << 1);
      TEST_SYNC_POINT(
          "WriteAmpBasedRateLimiterTest::LimitChangeTest:changeLimitStart");
      arg.limiter->SetBytesPerSecond(new_limit);
      TEST_SYNC_POINT(
          "WriteAmpBasedRateLimiterTest::LimitChangeTest:changeLimitEnd");
      env->WaitForJoin();
      fprintf(stderr,
              "[COMPLETE] request size %" PRIi32 " KB, new limit %" PRIi32
              "KB/sec, refill period %" PRIi64 " ms\n",
              target / 1024, new_limit / 1024, refill_period / 1000);
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
