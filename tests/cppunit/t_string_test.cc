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

#include <memory>

#include "test_base.h"
#include "types/redis_hash.h"
#include "types/redis_string.h"

class RedisStringTest : public TestBase {
 protected:
  explicit RedisStringTest() : TestBase() { string = std::make_unique<Redis::String>(storage_, "string_ns"); }
  ~RedisStringTest() = default;
  void SetUp() override {
    key_ = "test-string-key";
    pairs_ = {
        {"test-string-key1", "test-strings-value1"}, {"test-string-key2", "test-strings-value2"},
        {"test-string-key3", "test-strings-value3"}, {"test-string-key4", "test-strings-value4"},
        {"test-string-key5", "test-strings-value5"}, {"test-string-key6", "test-strings-value6"},
    };
  }

 protected:
  std::unique_ptr<Redis::String> string;
  std::vector<StringPair> pairs_;
};

TEST_F(RedisStringTest, Append) {
  int ret;
  for (size_t i = 0; i < 32; i++) {
    rocksdb::Status s = string->Append(key_, "a", &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(static_cast<int>(i + 1), ret);
  }
  string->Del(key_);
}

TEST_F(RedisStringTest, GetAndSet) {
  for (size_t i = 0; i < pairs_.size(); i++) {
    string->Set(pairs_[i].key.ToString(), pairs_[i].value.ToString());
  }
  for (size_t i = 0; i < pairs_.size(); i++) {
    std::string got_value;
    string->Get(pairs_[i].key.ToString(), &got_value);
    EXPECT_EQ(pairs_[i].value, got_value);
  }
  for (size_t i = 0; i < pairs_.size(); i++) {
    string->Del(pairs_[i].key);
  }
}

TEST_F(RedisStringTest, CacheUsesNamespaceAwareKey) {
  Redis::String other_ns_string(storage_, "string_ns_other");
  const std::string shared_key = "shared-cache-key";

  ASSERT_TRUE(string->Set(shared_key, "value-a").ok());
  ASSERT_TRUE(other_ns_string.Set(shared_key, "value-b").ok());

  std::string value_a, value_b;
  ASSERT_TRUE(string->Get(shared_key, &value_a).ok());
  ASSERT_TRUE(other_ns_string.Get(shared_key, &value_b).ok());
  EXPECT_EQ("value-a", value_a);
  EXPECT_EQ("value-b", value_b);

  ASSERT_TRUE(string->Del(shared_key).ok());
  ASSERT_TRUE(other_ns_string.Del(shared_key).ok());
}

TEST_F(RedisStringTest, FlushDBClearsCachedStringValues) {
  ASSERT_TRUE(string->Set(key_, "flushdb-value").ok());

  std::string value;
  ASSERT_TRUE(string->Get(key_, &value).ok());
  EXPECT_EQ("flushdb-value", value);

  ASSERT_TRUE(string->FlushDB().ok());
  auto s = string->Get(key_, &value);
  EXPECT_TRUE(s.IsNotFound());
}

TEST_F(RedisStringTest, FlushAllClearsCachedStringValuesAcrossNamespaces) {
  Redis::String other_ns_string(storage_, "string_ns_other");
  const std::string other_key = "flushall-other-key";

  ASSERT_TRUE(string->Set(key_, "flushall-value-a").ok());
  ASSERT_TRUE(other_ns_string.Set(other_key, "flushall-value-b").ok());

  std::string value;
  ASSERT_TRUE(string->Get(key_, &value).ok());
  ASSERT_TRUE(other_ns_string.Get(other_key, &value).ok());

  ASSERT_TRUE(string->FlushAll().ok());
  EXPECT_TRUE(string->Get(key_, &value).IsNotFound());
  EXPECT_TRUE(other_ns_string.Get(other_key, &value).IsNotFound());
}

TEST_F(RedisStringTest, GetPopulatesReadThroughCache) {
  ASSERT_TRUE(string->Set(key_, "read-through-value").ok());

  std::string ns_key;
  string->AppendNamespacePrefix(key_, &ns_key);
  storage_->Cache().clear();
  ASSERT_FALSE(storage_->Cache().Contains(Redis::StringCacheKey(ns_key)));

  std::string value;
  ASSERT_TRUE(string->Get(key_, &value).ok());
  EXPECT_EQ("read-through-value", value);
  EXPECT_TRUE(storage_->Cache().Contains(Redis::StringCacheKey(ns_key)));
}

TEST_F(RedisStringTest, MGetAndMSet) {
  string->MSet(pairs_);
  storage_->Cache().clear();
  std::vector<Slice> keys;
  std::vector<std::string> values;
  for (const auto &pair : pairs_) {
    keys.emplace_back(pair.key);
  }
  string->MGet(keys, &values);
  for (size_t i = 0; i < pairs_.size(); i++) {
    EXPECT_EQ(pairs_[i].value, values[i]);
  }
  for (const auto &pair : pairs_) {
    std::string ns_key;
    string->AppendNamespacePrefix(pair.key, &ns_key);
    EXPECT_TRUE(storage_->Cache().Contains(Redis::StringCacheKey(ns_key)));
  }
  for (size_t i = 0; i < pairs_.size(); i++) {
    string->Del(pairs_[i].key);
  }
}

TEST_F(RedisStringTest, MGetPrefersCachedValues) {
  ASSERT_TRUE(string->MSet(pairs_).ok());

  std::string ns_key;
  string->AppendNamespacePrefix(pairs_[0].key, &ns_key);
  storage_->Cache().Put(Redis::StringCacheKey(ns_key), "cached-mget-value");

  std::vector<Slice> keys;
  std::vector<std::string> values;
  for (const auto &pair : pairs_) {
    keys.emplace_back(pair.key);
  }

  auto statuses = string->MGet(keys, &values);
  ASSERT_EQ(pairs_.size(), values.size());
  ASSERT_EQ(pairs_.size(), statuses.size());
  EXPECT_TRUE(statuses[0].ok());
  EXPECT_EQ("cached-mget-value", values[0]);
  for (size_t i = 1; i < pairs_.size(); ++i) {
    EXPECT_TRUE(statuses[i].ok());
    EXPECT_EQ(pairs_[i].value, values[i]);
  }
}

TEST_F(RedisStringTest, GetExReturnsWrongTypeWhenKeyIsNotString) {
  Redis::Hash hash_db(storage_, "string_ns");
  int ret = 0;
  ASSERT_TRUE(hash_db.Set(key_, "field", "value", &ret).ok());

  std::string value;
  auto s = string->GetEx(key_, &value, 60);
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST_F(RedisStringTest, MSetWritesSingleWalBatch) {
  auto seq_before = storage_->LatestSeq();

  auto s = string->MSet(pairs_);
  EXPECT_TRUE(s.ok());

  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  auto status = storage_->GetWALIter(seq_before + 1, &iter);
  EXPECT_TRUE(status.IsOK());
  ASSERT_NE(iter, nullptr);
  ASSERT_TRUE(iter->Valid());

  auto batch = iter->GetBatch();
  EXPECT_EQ(seq_before + 1, batch.sequence);
  ASSERT_NE(batch.writeBatchPtr, nullptr);
  EXPECT_EQ(static_cast<int>(pairs_.size()), batch.writeBatchPtr->Count());

  iter->Next();
  EXPECT_FALSE(iter->Valid());

  for (const auto &pair : pairs_) {
    string->Del(pair.key);
  }
}

TEST_F(RedisStringTest, IncrByFloat) {
  double f;
  double max_float = std::numeric_limits<double>::max();
  string->IncrByFloat(key_, 1.0, &f);
  EXPECT_EQ(1.0, f);
  string->IncrByFloat(key_, max_float - 1, &f);
  EXPECT_EQ(max_float, f);
  string->IncrByFloat(key_, 1.2, &f);
  EXPECT_EQ(max_float, f);
  string->IncrByFloat(key_, -1 * max_float, &f);
  EXPECT_EQ(0, f);
  string->IncrByFloat(key_, -1 * max_float, &f);
  EXPECT_EQ(-1 * max_float, f);
  string->IncrByFloat(key_, -1.2, &f);
  EXPECT_EQ(-1 * max_float, f);
  // key hold value is not the number
  string->Set(key_, "abc");
  rocksdb::Status s = string->IncrByFloat(key_, 1.2, &f);
  EXPECT_TRUE(s.IsInvalidArgument());
  string->Del(key_);
}

TEST_F(RedisStringTest, IncrBy) {
  int64_t ret;
  string->IncrBy(key_, 1, &ret);
  EXPECT_EQ(1, ret);
  string->IncrBy(key_, INT64_MAX - 1, &ret);
  EXPECT_EQ(INT64_MAX, ret);
  rocksdb::Status s = string->IncrBy(key_, 1, &ret);
  EXPECT_TRUE(s.IsInvalidArgument());
  string->IncrBy(key_, INT64_MIN + 1, &ret);
  EXPECT_EQ(0, ret);
  string->IncrBy(key_, INT64_MIN, &ret);
  EXPECT_EQ(INT64_MIN, ret);
  s = string->IncrBy(key_, -1, &ret);
  EXPECT_TRUE(s.IsInvalidArgument());
  // key hold value is not the number
  string->Set(key_, "abc");
  s = string->IncrBy(key_, 1, &ret);
  EXPECT_TRUE(s.IsInvalidArgument());
  string->Del(key_);
}

TEST_F(RedisStringTest, GetEmptyValue) {
  const std::string key = "empty_value_key";
  auto s = string->Set(key, "");
  EXPECT_TRUE(s.ok());
  std::string value;
  s = string->Get(key, &value);
  EXPECT_TRUE(s.ok() && value.empty());
}

TEST_F(RedisStringTest, GetSet) {
  int ttl;
  int64_t now;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  std::vector<std::string> values = {"a", "b", "c", "d"};
  for (size_t i = 0; i < values.size(); i++) {
    std::string old_value;
    string->Expire(key_, static_cast<int>(now + 1000));
    string->GetSet(key_, values[i], &old_value);
    if (i != 0) {
      EXPECT_EQ(values[i - 1], old_value);
      string->TTL(key_, &ttl);
      EXPECT_TRUE(ttl == -1);
    } else {
      EXPECT_TRUE(old_value.empty());
    }
  }
  string->Del(key_);
}
TEST_F(RedisStringTest, GetDel) {
  for (size_t i = 0; i < pairs_.size(); i++) {
    string->Set(pairs_[i].key.ToString(), pairs_[i].value.ToString());
  }
  for (size_t i = 0; i < pairs_.size(); i++) {
    std::string got_value;
    string->GetDel(pairs_[i].key.ToString(), &got_value);
    EXPECT_EQ(pairs_[i].value, got_value);

    std::string second_got_value;
    auto s = string->GetDel(pairs_[i].key.ToString(), &second_got_value);
    EXPECT_TRUE(!s.ok() && s.IsNotFound());
  }
}

TEST_F(RedisStringTest, MSetXX) {
  int ret;
  string->SetXX(key_, "test-value", 3, &ret);
  EXPECT_EQ(ret, 0);
  string->Set(key_, "test-value");
  string->SetXX(key_, "test-value", 3, &ret);
  EXPECT_EQ(ret, 1);
  int ttl;
  string->TTL(key_, &ttl);
  EXPECT_TRUE(ttl >= 2 && ttl <= 3);
  string->Del(key_);
}

TEST_F(RedisStringTest, MSetNX) {
  int ret;
  string->MSetNX(pairs_, 0, &ret);
  EXPECT_EQ(1, ret);
  std::vector<Slice> keys;
  std::vector<std::string> values;
  for (const auto &pair : pairs_) {
    keys.emplace_back(pair.key);
  }
  string->MGet(keys, &values);
  for (size_t i = 0; i < pairs_.size(); i++) {
    EXPECT_EQ(pairs_[i].value, values[i]);
  }

  std::vector<StringPair> new_pairs{
      {"a", "1"}, {"b", "2"}, {"c", "3"}, {pairs_[0].key, pairs_[0].value}, {"d", "4"},
  };
  string->MSetNX(pairs_, 0, &ret);
  EXPECT_EQ(0, ret);

  for (size_t i = 0; i < pairs_.size(); i++) {
    string->Del(pairs_[i].key);
  }
}

TEST_F(RedisStringTest, MSetNXWithTTL) {
  int ret;
  string->SetNX(key_, "test-value", 3, &ret);
  int ttl;
  string->TTL(key_, &ttl);
  EXPECT_TRUE(ttl >= 2 && ttl <= 3);
  string->Del(key_);
}

TEST_F(RedisStringTest, SetEX) {
  string->SetEX(key_, "test-value", 3);
  int ttl;
  string->TTL(key_, &ttl);
  EXPECT_TRUE(ttl >= 2 && ttl <= 3);
  string->Del(key_);
}

TEST_F(RedisStringTest, SetRange) {
  int ret;
  string->Set(key_, "hello,world");
  string->SetRange(key_, 6, "redis", &ret);
  EXPECT_EQ(11, ret);
  std::string value;
  string->Get(key_, &value);
  EXPECT_EQ("hello,redis", value);

  string->SetRange(key_, 6, "test", &ret);
  EXPECT_EQ(11, ret);
  string->Get(key_, &value);
  EXPECT_EQ("hello,tests", value);

  string->SetRange(key_, 6, "redis-1234", &ret);
  string->Get(key_, &value);
  EXPECT_EQ("hello,redis-1234", value);

  string->SetRange(key_, 15, "1", &ret);
  EXPECT_EQ(16, ret);
  string->Get(key_, &value);
  EXPECT_EQ(16, value.size());
  string->Del(key_);
}

TEST_F(RedisStringTest, CAS) {
  int ret;
  std::string key = "cas_key", value = "cas_value", new_value = "new_value";

  auto status = string->Set(key, value);
  ASSERT_TRUE(status.ok());

  status = string->CAS("non_exist_key", value, new_value, 10, &ret);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(-1, ret);

  status = string->CAS(key, "cas_value_err", new_value, 10, &ret);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(0, ret);

  status = string->CAS(key, value, new_value, 10, &ret);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(1, ret);

  std::string current_value;
  status = string->Get(key, &current_value);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(new_value, current_value);

  int ttl;
  string->TTL(key, &ttl);
  EXPECT_TRUE(ttl >= 9 && ttl <= 10);

  string->Del(key);
}

TEST_F(RedisStringTest, CAD) {
  int ret;
  std::string key = "cas_key", value = "cas_value";

  auto status = string->Set(key, value);
  ASSERT_TRUE(status.ok());

  status = string->CAD("non_exist_key", value, &ret);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(-1, ret);

  status = string->CAD(key, "cas_value_err", &ret);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(0, ret);

  status = string->CAD(key, value, &ret);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(1, ret);

  std::string current_value;
  status = string->Get(key, &current_value);
  ASSERT_TRUE(status.IsNotFound());

  string->Del(key);
}
