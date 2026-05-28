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

#ifdef BRPC_FOUND

#include <brpc/redis.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "common/encoding.h"
#include "commands/redis_cmd.h"
#include "core_command_executor/core_command_executor.h"
#include "storage/redis_metadata.h"
#include "test_base.h"
#include "types/redis_hash.h"
#include "types/redis_zset.h"

namespace {

std::string ToLower(std::string s) {
  for (auto &c : s) {
    c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
  }
  return s;
}

uint64_t GetCommandFlags(const std::string &command_name) {
  auto commands = Redis::GetOriginalCommands();
  auto it = commands->find(ToLower(command_name));
  CHECK(it != commands->end());
  return it->second->flags;
}

std::string ComposeRemoteKeyForTest(const std::string &ns, const std::string &user_key, bool slot_id_encoded) {
  std::string remote_key;
  ComposeNamespaceKey(ns, user_key, &remote_key, slot_id_encoded);
  return remote_key;
}

std::string ZSetRangeByScoreCacheKeyForTest(const std::string &ns_key, uint64_t epoch, const ZRangeSpec &spec) {
  std::string cache_key;
  cache_key.push_back('z');
  cache_key.push_back('r');
  PutFixed32(&cache_key, static_cast<uint32_t>(ns_key.size()));
  cache_key.append(ns_key);
  PutFixed64(&cache_key, epoch);
  PutDouble(&cache_key, spec.min);
  PutDouble(&cache_key, spec.max);
  PutFixed8(&cache_key, static_cast<uint8_t>(spec.minex));
  PutFixed8(&cache_key, static_cast<uint8_t>(spec.maxex));
  PutFixed8(&cache_key, static_cast<uint8_t>(spec.reversed));
  PutFixed32(&cache_key, static_cast<uint32_t>(spec.offset + 1));
  PutFixed32(&cache_key, static_cast<uint32_t>(spec.count + 1));
  return cache_key;
}

std::string EncodeMemberScoresForTest(const std::vector<MemberScore> &mscores, uint32_t expire = 0) {
  std::string encoded;
  PutFixed32(&encoded, expire);
  PutFixed32(&encoded, static_cast<uint32_t>(mscores.size()));
  for (const auto &ms : mscores) {
    PutFixed32(&encoded, static_cast<uint32_t>(ms.member.size()));
    encoded.append(ms.member);
    PutDouble(&encoded, ms.score);
  }
  return encoded;
}

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

void PrimeHashFieldCache(Engine::Storage *storage, const std::string &ns, const std::string &user_key,
                         const std::string &field, const std::string &cached_value) {
  Redis::Hash hash_db(storage, ns);
  std::string ns_key;
  hash_db.AppendNamespacePrefix(user_key, &ns_key);
  storage->Cache().Put(Redis::HashCacheKey(ns_key, field), cached_value);
}

void PrimeHashAllCache(Engine::Storage *storage, const std::string &ns, const std::string &user_key,
                       const std::vector<FieldValue> &field_values) {
  Redis::Hash hash_db(storage, ns);
  std::string ns_key;
  hash_db.AppendNamespacePrefix(user_key, &ns_key);
  storage->Cache().Put(Redis::HashAllCacheKey(ns_key), EncodeHashFieldValuesForTest(field_values));
}

void PrimeZSetRangeByScoreCache(Engine::Storage *storage, const std::string &ns, const std::string &user_key,
                                const ZRangeSpec &spec, const std::vector<MemberScore> &cached_mscores,
                                uint64_t epoch = 0, uint32_t expire = 0) {
  Redis::ZSet zset_db(storage, ns);
  std::string ns_key;
  zset_db.AppendNamespacePrefix(user_key, &ns_key);

  std::string epoch_value;
  PutFixed64(&epoch_value, epoch);
  storage->Cache().Put(Redis::ZSetEpochCacheKey(ns_key), epoch_value);
  storage->Cache().Put(ZSetRangeByScoreCacheKeyForTest(ns_key, epoch, spec),
                       EncodeMemberScoresForTest(cached_mscores, expire));
}

}  // namespace

class CoreCommandExecutorTest : public TestBase {
 public:
  CoreCommandExecutorTest() : executor_(std::make_unique<kvrocks_datanode::CoreCommandExecutor>(storage_, *config_)) {}

 protected:
  StatusOr<kvrocks_datanode::CoreCommandResult> Execute(std::vector<std::string> args_without_flags) const {
    args_without_flags.emplace_back(std::to_string(GetCommandFlags(args_without_flags.front())));

    std::vector<butil::StringPiece> components;
    components.reserve(args_without_flags.size());
    for (const auto &arg : args_without_flags) {
      components.emplace_back(arg);
    }

    brpc::RedisRequest request;
    CHECK(request.AddCommandByComponents(components.data(), components.size()));

    auto parsed = executor_->ParseFlags(request);
    CHECK(parsed.IsOK()) << parsed.ToStatus().Msg();
    return executor_->Execute();
  }

  std::string RemoteKey(const std::string &user_key) const {
    return ComposeRemoteKeyForTest(ns_, user_key, config_->slot_id_encoded);
  }

  std::unique_ptr<kvrocks_datanode::CoreCommandExecutor> executor_;
  const std::string ns_ = "ycsb-unit";
};

TEST_F(CoreCommandExecutorTest, HMSetReturnsOk) {
  auto result = Execute({"HMSET", RemoteKey("user:hmset"), "field0", "value0", "field1", "value1"});
  ASSERT_TRUE(result.IsOK()) << result.ToStatus().Msg();
  EXPECT_EQ(result->reply_kind, kvrocks_datanode::CoreReplyKind::kStatus);
  EXPECT_EQ(result->status, "OK");
}

TEST_F(CoreCommandExecutorTest, HMGetReturnsFieldValuesAndNilForMisses) {
  Redis::Hash hash_db(storage_, ns_);
  int ret = 0;
  std::vector<FieldValue> field_values = {{"field0", "value0"}, {"field1", "value1"}};
  ASSERT_TRUE(hash_db.MSet("user:hmget", field_values, false, &ret).ok());

  auto result = Execute({"HMGET", RemoteKey("user:hmget"), "field0", "missing", "field1"});
  ASSERT_TRUE(result.IsOK()) << result.ToStatus().Msg();
  EXPECT_EQ(result->reply_kind, kvrocks_datanode::CoreReplyKind::kArray);
  EXPECT_EQ(result->values, (std::vector<std::string>{"value0", "", "value1"}));
  EXPECT_EQ(result->founds, (std::vector<bool>{true, false, true}));
}

TEST_F(CoreCommandExecutorTest, HMGetPrefersCachedFieldValues) {
  Redis::Hash hash_db(storage_, ns_);
  int ret = 0;
  std::vector<FieldValue> field_values = {{"field0", "value0"}, {"field1", "value1"}};
  ASSERT_TRUE(hash_db.MSet("user:hmget-cache", field_values, false, &ret).ok());

  PrimeHashFieldCache(storage_, ns_, "user:hmget-cache", "field0", "cached-field0");

  auto result = Execute({"HMGET", RemoteKey("user:hmget-cache"), "field0", "field1"});
  ASSERT_TRUE(result.IsOK()) << result.ToStatus().Msg();
  EXPECT_EQ(result->reply_kind, kvrocks_datanode::CoreReplyKind::kArray);
  EXPECT_EQ(result->values, (std::vector<std::string>{"cached-field0", "value1"}));
  EXPECT_EQ(result->founds, (std::vector<bool>{true, true}));
}

TEST_F(CoreCommandExecutorTest, HGetAllReturnsFlattenedFieldValuePairs) {
  Redis::Hash hash_db(storage_, ns_);
  int ret = 0;
  std::vector<FieldValue> field_values = {{"field0", "value0"}, {"field1", "value1"}};
  ASSERT_TRUE(hash_db.MSet("user:hgetall", field_values, false, &ret).ok());

  auto result = Execute({"HGETALL", RemoteKey("user:hgetall")});
  ASSERT_TRUE(result.IsOK()) << result.ToStatus().Msg();
  EXPECT_EQ(result->reply_kind, kvrocks_datanode::CoreReplyKind::kArray);
  EXPECT_EQ(result->values, (std::vector<std::string>{"field0", "value0", "field1", "value1"}));
  EXPECT_EQ(result->founds, (std::vector<bool>{true, true, true, true}));
}

TEST_F(CoreCommandExecutorTest, HGetAllPrefersCachedFieldValues) {
  Redis::Hash hash_db(storage_, ns_);
  int ret = 0;
  std::vector<FieldValue> field_values = {{"field0", "value0"}, {"field1", "value1"}};
  ASSERT_TRUE(hash_db.MSet("user:hgetall-cache", field_values, false, &ret).ok());

  PrimeHashFieldCache(storage_, ns_, "user:hgetall-cache", "field1", "cached-field1");

  auto result = Execute({"HGETALL", RemoteKey("user:hgetall-cache")});
  ASSERT_TRUE(result.IsOK()) << result.ToStatus().Msg();
  EXPECT_EQ(result->reply_kind, kvrocks_datanode::CoreReplyKind::kArray);
  EXPECT_EQ(result->values, (std::vector<std::string>{"field0", "value0", "field1", "cached-field1"}));
  EXPECT_EQ(result->founds, (std::vector<bool>{true, true, true, true}));
}

TEST_F(CoreCommandExecutorTest, HGetAllPrefersFullHashCache) {
  Redis::Hash hash_db(storage_, ns_);
  int ret = 0;
  std::vector<FieldValue> field_values = {{"field0", "value0"}, {"field1", "value1"}};
  ASSERT_TRUE(hash_db.MSet("user:hgetall-full-cache", field_values, false, &ret).ok());

  PrimeHashAllCache(storage_, ns_, "user:hgetall-full-cache",
                    {{"field0", "cached-value0"}, {"field1", "cached-value1"}});

  auto result = Execute({"HGETALL", RemoteKey("user:hgetall-full-cache")});
  ASSERT_TRUE(result.IsOK()) << result.ToStatus().Msg();
  EXPECT_EQ(result->reply_kind, kvrocks_datanode::CoreReplyKind::kArray);
  EXPECT_EQ(result->values, (std::vector<std::string>{"field0", "cached-value0", "field1", "cached-value1"}));
  EXPECT_EQ(result->founds, (std::vector<bool>{true, true, true, true}));
}

TEST_F(CoreCommandExecutorTest, DelReturnsDeletedKeyCount) {
  Redis::Hash hash_db(storage_, ns_);
  int ret = 0;
  std::vector<FieldValue> field_values = {{"field0", "value0"}};
  ASSERT_TRUE(hash_db.MSet("user:del", field_values, false, &ret).ok());

  auto result = Execute({"DEL", RemoteKey("user:del"), RemoteKey("user:missing")});
  ASSERT_TRUE(result.IsOK()) << result.ToStatus().Msg();
  EXPECT_EQ(result->reply_kind, kvrocks_datanode::CoreReplyKind::kInteger);
  EXPECT_EQ(result->integer, 1);
}

TEST_F(CoreCommandExecutorTest, ZAddReturnsInsertedMemberCount) {
  auto result = Execute({"ZADD", RemoteKey("_indices:zadd"), "1", "user1", "2", "user2"});
  ASSERT_TRUE(result.IsOK()) << result.ToStatus().Msg();
  EXPECT_EQ(result->reply_kind, kvrocks_datanode::CoreReplyKind::kInteger);
  EXPECT_EQ(result->integer, 2);
}

TEST_F(CoreCommandExecutorTest, ZRangeByScoreReturnsMembersInScoreOrder) {
  Redis::ZSet zset_db(storage_, ns_);
  std::vector<MemberScore> member_scores = {{"user1", 1.0}, {"user2", 2.0}, {"user3", 3.0}};
  int ret = 0;
  ASSERT_TRUE(zset_db.Add("_indices:zrange", ZAddFlags::Default(), &member_scores, &ret).ok());

  auto result = Execute({"ZRANGEBYSCORE", RemoteKey("_indices:zrange"), "1", "+inf", "LIMIT", "0", "2"});
  ASSERT_TRUE(result.IsOK()) << result.ToStatus().Msg();
  EXPECT_EQ(result->reply_kind, kvrocks_datanode::CoreReplyKind::kArray);
  EXPECT_EQ(result->values, (std::vector<std::string>{"user1", "user2"}));
  EXPECT_EQ(result->founds, (std::vector<bool>{true, true}));
}

TEST_F(CoreCommandExecutorTest, ZRangeByScorePrefersCachedRangeResult) {
  Redis::ZSet zset_db(storage_, ns_);
  std::vector<MemberScore> member_scores = {{"user1", 1.0}, {"user2", 2.0}, {"user3", 3.0}};
  int ret = 0;
  ASSERT_TRUE(zset_db.Add("_indices:zrange-cache", ZAddFlags::Default(), &member_scores, &ret).ok());

  ZRangeSpec spec;
  spec.min = 1.0;
  spec.max = kMaxScore;
  spec.offset = 0;
  spec.count = 2;
  PrimeZSetRangeByScoreCache(storage_, ns_, "_indices:zrange-cache", spec, {{"cached-user", 1.5}});

  auto result = Execute({"ZRANGEBYSCORE", RemoteKey("_indices:zrange-cache"), "1", "+inf", "LIMIT", "0", "2"});
  ASSERT_TRUE(result.IsOK()) << result.ToStatus().Msg();
  EXPECT_EQ(result->reply_kind, kvrocks_datanode::CoreReplyKind::kArray);
  EXPECT_EQ(result->values, (std::vector<std::string>{"cached-user"}));
  EXPECT_EQ(result->founds, (std::vector<bool>{true}));
}

TEST_F(CoreCommandExecutorTest, ZRangeByScoreIgnoresExpiredCachedRangeResult) {
  Redis::ZSet zset_db(storage_, ns_);
  std::vector<MemberScore> member_scores = {{"user1", 1.0}, {"user2", 2.0}, {"user3", 3.0}};
  int ret = 0;
  ASSERT_TRUE(zset_db.Add("_indices:zrange-expired-cache", ZAddFlags::Default(), &member_scores, &ret).ok());

  ZRangeSpec spec;
  spec.min = 1.0;
  spec.max = kMaxScore;
  spec.offset = 0;
  spec.count = 2;
  PrimeZSetRangeByScoreCache(storage_, ns_, "_indices:zrange-expired-cache", spec, {{"stale-user", 9.0}}, 0, 1);

  auto result = Execute({"ZRANGEBYSCORE", RemoteKey("_indices:zrange-expired-cache"), "1", "+inf", "LIMIT", "0", "2"});
  ASSERT_TRUE(result.IsOK()) << result.ToStatus().Msg();
  EXPECT_EQ(result->reply_kind, kvrocks_datanode::CoreReplyKind::kArray);
  EXPECT_EQ(result->values, (std::vector<std::string>{"user1", "user2"}));
  EXPECT_EQ(result->founds, (std::vector<bool>{true, true}));
}

TEST_F(CoreCommandExecutorTest, ZRemReturnsRemovedMemberCount) {
  Redis::ZSet zset_db(storage_, ns_);
  std::vector<MemberScore> member_scores = {{"user1", 1.0}, {"user2", 2.0}};
  int ret = 0;
  ASSERT_TRUE(zset_db.Add("_indices:zrem", ZAddFlags::Default(), &member_scores, &ret).ok());

  auto result = Execute({"ZREM", RemoteKey("_indices:zrem"), "user1", "missing"});
  ASSERT_TRUE(result.IsOK()) << result.ToStatus().Msg();
  EXPECT_EQ(result->reply_kind, kvrocks_datanode::CoreReplyKind::kInteger);
  EXPECT_EQ(result->integer, 1);
}

TEST_F(CoreCommandExecutorTest, ZAddInvalidatesCachedRangeResult) {
  Redis::ZSet zset_db(storage_, ns_);
  std::vector<MemberScore> member_scores = {{"user1", 1.0}, {"user3", 3.0}};
  int ret = 0;
  ASSERT_TRUE(zset_db.Add("_indices:zrange-invalidate-add", ZAddFlags::Default(), &member_scores, &ret).ok());

  ZRangeSpec spec;
  spec.min = 1.0;
  spec.max = kMaxScore;
  spec.offset = 0;
  spec.count = 3;
  PrimeZSetRangeByScoreCache(storage_, ns_, "_indices:zrange-invalidate-add", spec, {{"stale-user", 9.0}});

  auto add_result = Execute({"ZADD", RemoteKey("_indices:zrange-invalidate-add"), "2", "user2"});
  ASSERT_TRUE(add_result.IsOK()) << add_result.ToStatus().Msg();
  EXPECT_EQ(add_result->integer, 1);

  auto range_result =
      Execute({"ZRANGEBYSCORE", RemoteKey("_indices:zrange-invalidate-add"), "1", "+inf", "LIMIT", "0", "3"});
  ASSERT_TRUE(range_result.IsOK()) << range_result.ToStatus().Msg();
  EXPECT_EQ(range_result->values, (std::vector<std::string>{"user1", "user2", "user3"}));
}

TEST_F(CoreCommandExecutorTest, ZRemInvalidatesCachedRangeResult) {
  Redis::ZSet zset_db(storage_, ns_);
  std::vector<MemberScore> member_scores = {{"user1", 1.0}, {"user2", 2.0}, {"user3", 3.0}};
  int ret = 0;
  ASSERT_TRUE(zset_db.Add("_indices:zrange-invalidate-rem", ZAddFlags::Default(), &member_scores, &ret).ok());

  ZRangeSpec spec;
  spec.min = 1.0;
  spec.max = kMaxScore;
  spec.offset = 0;
  spec.count = 3;
  PrimeZSetRangeByScoreCache(storage_, ns_, "_indices:zrange-invalidate-rem", spec, {{"stale-user", 9.0}});

  auto rem_result = Execute({"ZREM", RemoteKey("_indices:zrange-invalidate-rem"), "user2"});
  ASSERT_TRUE(rem_result.IsOK()) << rem_result.ToStatus().Msg();
  EXPECT_EQ(rem_result->integer, 1);

  auto range_result =
      Execute({"ZRANGEBYSCORE", RemoteKey("_indices:zrange-invalidate-rem"), "1", "+inf", "LIMIT", "0", "3"});
  ASSERT_TRUE(range_result.IsOK()) << range_result.ToStatus().Msg();
  EXPECT_EQ(range_result->values, (std::vector<std::string>{"user1", "user3"}));
}

#endif  // BRPC_FOUND
