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

#include <algorithm>
#include <memory>
#include <random>
#include <string>

#include "common/encoding.h"
#include "parse_util.h"
#include "test_base.h"
#include "types/redis_hash.h"

namespace {

std::string EncodeHashFieldValuesForTest(const std::vector<FieldValue> &field_values) {
  std::string encoded;
  PutFixed32(&encoded, static_cast<uint32_t>(field_values.size()));
  for (const auto &fv : field_values) {
    PutFixed32(&encoded, static_cast<uint32_t>(fv.field.size()));
    encoded.append(fv.field);
    PutFixed32(&encoded, static_cast<uint32_t>(fv.value.size()));
    encoded.append(fv.value);
  }
  return encoded;
}

}  // namespace

class RedisHashTest : public TestBase {
 protected:
  explicit RedisHashTest() : TestBase() { hash = std::make_unique<Redis::Hash>(storage_, "hash_ns"); }
  ~RedisHashTest() = default;
  void SetUp() override {
    key_ = "test_hash->key";
    fields_ = {"test-hash-key-1", "test-hash-key-2", "test-hash-key-3"};
    values_ = {"hash-test-value-1", "hash-test-value-2", "hash-test-value-3"};
  }
  void TearDown() override {}

 protected:
  std::unique_ptr<Redis::Hash> hash;
};

TEST_F(RedisHashTest, GetAndSet) {
  int ret;
  for (size_t i = 0; i < fields_.size(); i++) {
    rocksdb::Status s = hash->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string got;
    rocksdb::Status s = hash->Get(key_, fields_[i], &got);
    EXPECT_EQ(values_[i], got);
  }
  rocksdb::Status s = hash->Delete(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  hash->Del(key_);
}

TEST_F(RedisHashTest, MGetAndMSet) {
  int ret;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < fields_.size(); i++) {
    fvs.emplace_back(FieldValue{fields_[i].ToString(), values_[i].ToString()});
  }
  rocksdb::Status s = hash->MSet(key_, fvs, false, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fvs.size()) == ret);
  s = hash->MSet(key_, fvs, false, &ret);
  EXPECT_EQ(ret, 0);
  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  s = hash->MGet(key_, fields_, &values, &statuses);
  for (size_t i = 0; i < fields_.size(); i++) {
    EXPECT_EQ(values[i], values_[i].ToString());
  }
  s = hash->Delete(key_, fields_, &ret);
  EXPECT_EQ(static_cast<int>(fields_.size()), ret);
  hash->Del(key_);
}

TEST_F(RedisHashTest, GetAndMGetPreferStableFieldCacheKeys) {
  int ret;
  ASSERT_TRUE(hash->Set(key_, fields_[0], values_[0], &ret).ok());
  ASSERT_TRUE(hash->Set(key_, fields_[1], values_[1], &ret).ok());

  std::string ns_key;
  hash->AppendNamespacePrefix(key_, &ns_key);
  storage_->Cache().Put(Redis::HashCacheKey(ns_key, fields_[0]), "cached-hash-value-0");
  storage_->Cache().Put(Redis::HashCacheKey(ns_key, fields_[1]), "cached-hash-value-1");

  std::string value;
  ASSERT_TRUE(hash->Get(key_, fields_[0], &value).ok());
  EXPECT_EQ("cached-hash-value-0", value);

  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  ASSERT_TRUE(hash->MGet(key_, {fields_[0], fields_[1]}, &values, &statuses).ok());
  ASSERT_EQ(2U, values.size());
  ASSERT_EQ(2U, statuses.size());
  EXPECT_TRUE(statuses[0].ok());
  EXPECT_TRUE(statuses[1].ok());
  EXPECT_EQ("cached-hash-value-0", values[0]);
  EXPECT_EQ("cached-hash-value-1", values[1]);
}

TEST_F(RedisHashTest, GetAllPrefersFullHashCache) {
  int ret;
  ASSERT_TRUE(hash->MSet(key_, {{fields_[0].ToString(), values_[0].ToString()},
                                {fields_[1].ToString(), values_[1].ToString()}},
                         false, &ret)
                  .ok());

  std::string ns_key;
  hash->AppendNamespacePrefix(key_, &ns_key);
  const std::vector<FieldValue> cached_values = {
      {fields_[0].ToString(), "cached-value-0"},
      {fields_[1].ToString(), "cached-value-1"},
  };
  storage_->Cache().Put(Redis::HashAllCacheKey(ns_key), EncodeHashFieldValuesForTest(cached_values));

  std::vector<FieldValue> field_values;
  ASSERT_TRUE(hash->GetAll(key_, &field_values).ok());
  ASSERT_EQ(cached_values.size(), field_values.size());
  EXPECT_EQ(cached_values[0].field, field_values[0].field);
  EXPECT_EQ(cached_values[0].value, field_values[0].value);
  EXPECT_EQ(cached_values[1].field, field_values[1].field);
  EXPECT_EQ(cached_values[1].value, field_values[1].value);

  field_values.clear();
  ASSERT_TRUE(hash->GetAll(key_, &field_values, HashFetchType::kOnlyKey).ok());
  ASSERT_EQ(cached_values.size(), field_values.size());
  EXPECT_EQ(cached_values[0].field, field_values[0].field);
  EXPECT_TRUE(field_values[0].value.empty());
  EXPECT_EQ(cached_values[1].field, field_values[1].field);
  EXPECT_TRUE(field_values[1].value.empty());

  field_values.clear();
  ASSERT_TRUE(hash->GetAll(key_, &field_values, HashFetchType::kOnlyValue).ok());
  ASSERT_EQ(cached_values.size(), field_values.size());
  EXPECT_TRUE(field_values[0].field.empty());
  EXPECT_EQ(cached_values[0].value, field_values[0].value);
  EXPECT_TRUE(field_values[1].field.empty());
  EXPECT_EQ(cached_values[1].value, field_values[1].value);
}

TEST_F(RedisHashTest, DelClearsStableFieldCacheKeys) {
  int ret;
  ASSERT_TRUE(hash->Set(key_, fields_[0], values_[0], &ret).ok());

  std::string ns_key;
  hash->AppendNamespacePrefix(key_, &ns_key);
  const auto cache_key = Redis::HashCacheKey(ns_key, fields_[0]);
  storage_->Cache().Put(cache_key, "stale-hash-value");
  ASSERT_TRUE(storage_->Cache().Contains(cache_key));

  ASSERT_TRUE(hash->Del(key_).ok());
  EXPECT_FALSE(storage_->Cache().Contains(cache_key));

  ASSERT_TRUE(hash->Set(key_, fields_[0], "fresh-hash-value", &ret).ok());
  std::string value;
  ASSERT_TRUE(hash->Get(key_, fields_[0], &value).ok());
  EXPECT_EQ("fresh-hash-value", value);
}

TEST_F(RedisHashTest, SetNX) {
  int ret;
  Slice field("foo");
  rocksdb::Status s = hash->Set(key_, field, "bar", &ret);
  EXPECT_TRUE(s.ok() && ret == 1);
  s = hash->Set(key_, field, "bar", &ret);
  EXPECT_TRUE(s.ok() && ret == 0);
  std::vector<Slice> fields = {field};
  s = hash->Delete(key_, fields, &ret);
  EXPECT_EQ(fields.size(), ret);
  hash->Del(key_);
}

TEST_F(RedisHashTest, HGetAll) {
  int ret;
  for (size_t i = 0; i < fields_.size(); i++) {
    rocksdb::Status s = hash->Set(key_, fields_[i], values_[i], &ret);
    EXPECT_TRUE(s.ok() && ret == 1);
  }
  std::vector<FieldValue> fvs;
  rocksdb::Status s = hash->GetAll(key_, &fvs);
  EXPECT_TRUE(s.ok() && fvs.size() == fields_.size());
  s = hash->Delete(key_, fields_, &ret);
  EXPECT_TRUE(s.ok() && static_cast<int>(fields_.size()) == ret);
  hash->Del(key_);
}

TEST_F(RedisHashTest, HIncr) {
  int64_t value;
  Slice field("hash-incrby-invalid-field");
  for (int i = 0; i < 32; i++) {
    rocksdb::Status s = hash->IncrBy(key_, field, 1, &value);
    EXPECT_TRUE(s.ok());
  }
  std::string bytes;
  hash->Get(key_, field, &bytes);
  auto parseResult = ParseInt<int64_t>(bytes, 10);
  if (!parseResult) {
    FAIL();
  }
  EXPECT_EQ(32, *parseResult);
  hash->Del(key_);
}

TEST_F(RedisHashTest, HIncrInvalid) {
  int ret;
  int64_t value;
  Slice field("hash-incrby-invalid-field");
  rocksdb::Status s = hash->IncrBy(key_, field, 1, &value);
  EXPECT_TRUE(s.ok() && value == 1);

  s = hash->IncrBy(key_, field, LLONG_MAX, &value);
  EXPECT_TRUE(s.IsInvalidArgument());
  hash->Set(key_, field, "abc", &ret);
  s = hash->IncrBy(key_, field, 1, &value);
  EXPECT_TRUE(s.IsInvalidArgument());

  hash->Set(key_, field, "-1", &ret);
  s = hash->IncrBy(key_, field, -1, &value);
  EXPECT_TRUE(s.ok());
  s = hash->IncrBy(key_, field, LLONG_MIN, &value);
  EXPECT_TRUE(s.IsInvalidArgument());

  hash->Del(key_);
}

TEST_F(RedisHashTest, HIncrByFloat) {
  double value;
  Slice field("hash-incrbyfloat-invalid-field");
  for (int i = 0; i < 32; i++) {
    rocksdb::Status s = hash->IncrByFloat(key_, field, 1.2, &value);
    EXPECT_TRUE(s.ok());
  }
  std::string bytes;
  hash->Get(key_, field, &bytes);
  value = std::stof(bytes);
  EXPECT_FLOAT_EQ(32 * 1.2, value);
  hash->Del(key_);
}

TEST_F(RedisHashTest, HRange) {
  int ret;
  std::vector<FieldValue> fvs;
  for (size_t i = 0; i < 4; i++) {
    fvs.emplace_back(FieldValue{"key" + std::to_string(i), "value" + std::to_string(i)});
  }
  for (size_t i = 0; i < 26; i++) {
    fvs.emplace_back(FieldValue{std::to_string(char(i + 'a')), std::to_string(char(i + 'a'))});
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::vector<FieldValue> tmp(fvs);
  for (size_t i = 0; i < 100; i++) {
    std::shuffle(tmp.begin(), tmp.end(), g);
    rocksdb::Status s = hash->MSet(key_, tmp, false, &ret);
    EXPECT_TRUE(s.ok() && static_cast<int>(tmp.size()) == ret);
    s = hash->MSet(key_, fvs, false, &ret);
    EXPECT_EQ(ret, 0);
    std::vector<FieldValue> result;
    s = hash->Range(key_, "key0", "key4", INT_MAX, &result);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(4, result.size());
    EXPECT_EQ("key0", result[0].field);
    EXPECT_EQ("key1", result[1].field);
    EXPECT_EQ("key2", result[2].field);
    hash->Del(key_);
  }
}

TEST_F(RedisHashTest, HRangeNonExistingKey) {
  std::vector<FieldValue> result;
  auto s = hash->Range("non-existing-key", "any-start-key", "any-end-key", 10, &result);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(result.size(), 0);
}
