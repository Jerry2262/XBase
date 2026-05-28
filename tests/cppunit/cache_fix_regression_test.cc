/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <gtest/gtest.h>

#include <functional>
#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>

#include "cluster/redis_slot.h"
#include "config.h"
#include "storage/cache/kvrocks_cache.h"
#include "storage/storage.h"
#include "types/redis_string.h"

namespace {

constexpr size_t kCacheShardCount = 16;

std::string FindKeyInShard(size_t shard, const std::string &prefix) {
  for (int i = 0; i < 4096; ++i) {
    std::string candidate = prefix + "-" + std::to_string(i);
    const auto candidate_shard = std::hash<std::string_view>{}(candidate) % kCacheShardCount;
    if (candidate_shard == shard) {
      return candidate;
    }
  }
  return prefix + "-fallback";
}

}  // namespace

TEST(KVRocksCacheRegressionTest, GetReturnsDetachedCopy) {
  using Cache = KVRocksCache<std::string, std::string>;
  static_assert(std::is_same_v<decltype(std::declval<Cache &>().Get(std::declval<const std::string &>())),
                               std::string>);

  Cache cache(1024, 16);
  const std::string key = "cache-key";

  cache.Put(key, "value-v1");
  auto value = cache.Get(key);

  ASSERT_TRUE(cache.Remove(key));
  cache.Put(key, "value-v2");

  EXPECT_EQ("value-v1", value);
  EXPECT_EQ("value-v2", cache.Get(key));
}

TEST(KVRocksCacheRegressionTest, DisabledCacheBypassesReadsAndWrites) {
  using Cache = KVRocksCache<std::string, std::string>;
  Cache cache(0, 0);

  cache.Put("disabled-key", "disabled-value");

  std::string value;
  EXPECT_FALSE(cache.TryGet("disabled-key", &value));
  EXPECT_FALSE(cache.Contains("disabled-key"));
  EXPECT_FALSE(cache.Remove("disabled-key"));
  EXPECT_EQ(0, cache.Count());
  EXPECT_EQ(0, cache.Size());
}

TEST(KVRocksCacheRegressionTest, SmallPositiveCountStillEvictsWithinShard) {
  using Cache = KVRocksCache<std::string, std::string>;
  Cache cache(1024, 1);

  const std::string key1 = FindKeyInShard(0, "same-shard-a");
  const std::string key2 = FindKeyInShard(0, "same-shard-b");
  ASSERT_NE(key1, key2);
  ASSERT_EQ(0U, std::hash<std::string_view>{}(key1) % kCacheShardCount);
  ASSERT_EQ(0U, std::hash<std::string_view>{}(key2) % kCacheShardCount);

  cache.Put(key1, "value-a");
  cache.Put(key2, "value-b");

  std::string value;
  EXPECT_FALSE(cache.TryGet(key1, &value));
  ASSERT_TRUE(cache.TryGet(key2, &value));
  EXPECT_EQ("value-b", value);
  EXPECT_EQ(1, cache.Count());
}

class RedisStringSlotCacheTest : public testing::Test {
 protected:
  void SetUp() override {
    CleanupDbDir();

    config_ = std::make_unique<Config>();
    config_->db_dir = db_dir_;
    config_->backup_dir = db_dir_ + "/backup";
    config_->slot_id_encoded = true;

    storage_ = std::make_unique<Engine::Storage>(config_.get());
    auto s = storage_->Open();
    ASSERT_TRUE(s.IsOK()) << s.Msg();

    string_ = std::make_unique<Redis::String>(storage_.get(), ns_);
  }

  void TearDown() override {
    string_.reset();
    storage_.reset();
    config_.reset();
    CleanupDbDir();
  }

  static std::string FindKeyWithDifferentSlot(const std::string &base_key) {
    const auto base_slot = GetSlotNumFromKey(base_key);
    for (int i = 0; i < 1024; ++i) {
      auto candidate = base_key + "-" + std::to_string(i);
      if (GetSlotNumFromKey(candidate) != base_slot) {
        return candidate;
      }
    }
    return base_key + "-fallback";
  }

  void CleanupDbDir() const {
    auto cleanup = "rm -rf " + db_dir_;
    ASSERT_EQ(0, std::system(cleanup.c_str()));
  }

  const std::string db_dir_ = "testsdb_slot_cache";
  const std::string ns_ = "string_slot_ns";
  std::unique_ptr<Config> config_;
  std::unique_ptr<Engine::Storage> storage_;
  std::unique_ptr<Redis::String> string_;
};

class RedisStringDisabledCacheTest : public testing::Test {
 protected:
  void SetUp() override {
    CleanupDbDir();

    config_ = std::make_unique<Config>();
    config_->db_dir = db_dir_;
    config_->backup_dir = db_dir_ + "/backup";
    config_->KVrocksCache.capacity = 0;
    config_->KVrocksCache.count = 0;

    storage_ = std::make_unique<Engine::Storage>(config_.get());
    auto s = storage_->Open();
    ASSERT_TRUE(s.IsOK()) << s.Msg();

    string_ = std::make_unique<Redis::String>(storage_.get(), ns_);
  }

  void TearDown() override {
    string_.reset();
    storage_.reset();
    config_.reset();
    CleanupDbDir();
  }

  void CleanupDbDir() const {
    auto cleanup = "rm -rf " + db_dir_;
    ASSERT_EQ(0, std::system(cleanup.c_str()));
  }

  const std::string db_dir_ = "testsdb_disabled_cache";
  const std::string ns_ = "string_disabled_cache_ns";
  std::unique_ptr<Config> config_;
  std::unique_ptr<Engine::Storage> storage_;
  std::unique_ptr<Redis::String> string_;
};

TEST_F(RedisStringSlotCacheTest, ClearKeysOfSlotClearsCachedStringValues) {
  const std::string target_key = "slot-cache-target";
  const std::string other_key = FindKeyWithDifferentSlot(target_key);
  ASSERT_NE(GetSlotNumFromKey(target_key), GetSlotNumFromKey(other_key));

  ASSERT_TRUE(string_->Set(target_key, "target-value").ok());
  ASSERT_TRUE(string_->Set(other_key, "other-value").ok());

  std::string value;
  ASSERT_TRUE(string_->Get(target_key, &value).ok());
  EXPECT_EQ("target-value", value);
  ASSERT_TRUE(string_->Get(other_key, &value).ok());
  EXPECT_EQ("other-value", value);

  const auto slot = static_cast<int>(GetSlotNumFromKey(target_key));
  ASSERT_TRUE(string_->ClearKeysOfSlot(ns_, slot).ok());

  auto target_status = string_->Get(target_key, &value);
  EXPECT_TRUE(target_status.IsNotFound());

  ASSERT_TRUE(string_->Get(other_key, &value).ok());
  EXPECT_EQ("other-value", value);
}

TEST_F(RedisStringDisabledCacheTest, StringOperationsFallbackWhenCacheIsDisabled) {
  ASSERT_TRUE(string_->Set("disabled-cache-key", "value-a").ok());
  EXPECT_EQ(0, storage_->Cache().Count());

  std::string value;
  ASSERT_TRUE(string_->Get("disabled-cache-key", &value).ok());
  EXPECT_EQ("value-a", value);
  EXPECT_EQ(0, storage_->Cache().Count());

  ASSERT_TRUE(string_->Set("disabled-cache-key", "value-b").ok());
  ASSERT_TRUE(string_->Get("disabled-cache-key", &value).ok());
  EXPECT_EQ("value-b", value);
  EXPECT_EQ(0, storage_->Cache().Count());
}
