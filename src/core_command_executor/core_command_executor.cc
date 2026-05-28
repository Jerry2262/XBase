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

#include "core_command_executor/core_command_executor.h"

#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <cmath>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "commands/redis_cmd.h"
#include "common/parse_util.h"
#include "common/util.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "types/redis_hash.h"
#include "types/redis_list.h"
#include "types/redis_string.h"
#include "types/redis_zset.h"

namespace kvrocks_datanode {

namespace {

struct RedisCommandSpec {
  CoreCommandKind kind = CoreCommandKind::kUnknown;
  size_t min_arity_with_flags = 0;
};

// 统一构造请求参数错误，便于上层区分“请求非法”和“执行失败”。
Status MakeInvalidRequest(const std::string &msg) { return {Status::RedisInvalidCmd, msg}; }

Status MakeParseError(const std::string &msg) { return {Status::RedisParseErr, msg}; }

Status MakeUnknownCommand(const std::string &msg) { return {Status::RedisUnknownCmd, msg}; }

// 将底层 rocksdb/storage 错误映射为可对外返回的执行错误。
Status MakeExecutionError(const rocksdb::Status &status) { return {Status::RedisExecErr, status.ToString()}; }

// 从 brpc::RedisRequest 中取出首条命令参数。
// brpc::RedisRequest 公开接口没有直接暴露参数数组，因此这里复用 brpc 自带 parser。
StatusOr<std::vector<std::string>> GetCommand(const brpc::RedisRequest &request) {
  butil::IOBuf buffer;
  if (!request.SerializeTo(&buffer)) {
    return MakeParseError("failed to serialize redis request");
  }
  if (buffer.empty()) {
    return MakeParseError("redis request is empty");
  }

  brpc::RedisCommandParser parser;
  butil::Arena arena;
  std::vector<butil::StringPiece> parsed_args;
  auto err = parser.Consume(buffer, &parsed_args, &arena);
  if (err != brpc::PARSE_OK) {
    return MakeParseError("invalid redis request");
  }
  if (parsed_args.empty()) {
    return MakeParseError("redis request is empty");
  }

  std::vector<std::string> command_args;
  command_args.reserve(parsed_args.size());
  for (const auto &arg : parsed_args) {
    command_args.emplace_back(arg.data(), arg.size());
  }
  return command_args;
}

// 根据命令名查到执行器内的命令类型以及“真实参数 + flags”最小参数个数。
StatusOr<RedisCommandSpec> GetCommandSpec(std::string_view command_name) {
  const std::string name = Util::ToLower(std::string(command_name));
  if (name == "del") return RedisCommandSpec{CoreCommandKind::kDel, 3};
  if (name == "get") return RedisCommandSpec{CoreCommandKind::kGet, 3};
  if (name == "set") return RedisCommandSpec{CoreCommandKind::kSet, 4};
  if (name == "mget") return RedisCommandSpec{CoreCommandKind::kMGet, 3};
  if (name == "mset") return RedisCommandSpec{CoreCommandKind::kMSet, 4};
  if (name == "hget") return RedisCommandSpec{CoreCommandKind::kHGet, 4};
  if (name == "hmget") return RedisCommandSpec{CoreCommandKind::kHMGet, 4};
  if (name == "hset") return RedisCommandSpec{CoreCommandKind::kHSet, 5};
  if (name == "hmset") return RedisCommandSpec{CoreCommandKind::kHMSet, 5};
  if (name == "hgetall") return RedisCommandSpec{CoreCommandKind::kHGetAll, 3};
  if (name == "lindex") return RedisCommandSpec{CoreCommandKind::kLIndex, 4};
  if (name == "lset") return RedisCommandSpec{CoreCommandKind::kLSet, 5};
  if (name == "zadd") return RedisCommandSpec{CoreCommandKind::kZAdd, 5};
  if (name == "zrangebyscore") return RedisCommandSpec{CoreCommandKind::kZRangeByScore, 5};
  if (name == "zrem") return RedisCommandSpec{CoreCommandKind::kZRem, 4};
  return MakeUnknownCommand("unsupported core command: " + name);
}

// 统一校验参数下限。这里的参数总数包含尾部 flags。
Status ValidateMinArity(const std::vector<std::string> &command_args, size_t min_arity_with_flags) {
  if (command_args.size() < min_arity_with_flags) {
    return MakeParseError("wrong number of arguments");
  }
  return Status::OK();
}

// 把最后一个参数解析为十进制 uint64 flags。
StatusOr<uint64_t> ParseTailFlags(const std::vector<std::string> &command_args) {
  if (command_args.empty()) {
    return MakeParseError("wrong number of arguments");
  }

  auto parsed = ParseInt<uint64_t>(command_args.back(), 10);
  if (!parsed.IsOK()) {
    return MakeParseError("invalid flags");
  }
  return parsed.GetValue();
}

// 从命令 flags 推导 core 线程池使用的任务类型。
kvrocks::CoreTaskType InferTaskType(uint64_t flags) {
  if ((flags & Redis::kCmdExclusive) != 0) {
    return kvrocks::CoreTaskType::kExclusive;
  }
  if ((flags & Redis::kCmdWrite) != 0) {
    return kvrocks::CoreTaskType::kWrite;
  }
  return kvrocks::CoreTaskType::kReadOnly;
}

Status ValidateCommandArguments(CoreCommandKind kind, const std::vector<std::string> &command_args) {
  switch (kind) {
    case CoreCommandKind::kHMSet:
      if ((command_args.size() % 2) == 0) {
        return MakeParseError("wrong number of arguments");
      }
      return Status::OK();
    case CoreCommandKind::kHGetAll:
      if (command_args.size() != 3) {
        return MakeParseError("wrong number of arguments");
      }
      return Status::OK();
    default:
      return Status::OK();
  }
}

Status ParseZAddFlags(const std::vector<std::string> &command_args, size_t *index, ZAddFlags *flags) {
  if (index == nullptr || flags == nullptr) {
    return MakeInvalidRequest("zadd flag parser requires output");
  }

  for (size_t i = *index; i + 1 < command_args.size(); ++i) {
    const auto option = Util::ToLower(command_args[i]);
    if (option == "xx") {
      flags->SetFlag(kZSetXX);
      *index += 1;
    } else if (option == "nx") {
      flags->SetFlag(kZSetNX);
      *index += 1;
    } else if (option == "ch") {
      flags->SetFlag(kZSetCH);
      *index += 1;
    } else if (option == "lt") {
      flags->SetFlag(kZSetLT);
      *index += 1;
    } else if (option == "gt") {
      flags->SetFlag(kZSetGT);
      *index += 1;
    } else if (option == "incr") {
      flags->SetFlag(kZSetIncr);
      *index += 1;
    } else {
      break;
    }
  }

  if (flags->HasNX() && flags->HasXX()) {
    return MakeParseError("XX and NX options at the same time are not compatible");
  }
  if ((flags->HasLT() && flags->HasGT()) || (flags->HasLT() && flags->HasNX()) ||
      (flags->HasGT() && flags->HasNX())) {
    return MakeParseError("GT, LT, and/or NX options at the same time are not compatible");
  }
  return Status::OK();
}

Status ParseZRangeByScoreArgs(const std::vector<std::string> &command_args, ZRangeSpec *spec, bool *with_scores) {
  if (spec == nullptr || with_scores == nullptr) {
    return MakeInvalidRequest("zrangebyscore parser requires output");
  }

  auto s = Redis::ZSet::ParseRangeSpec(command_args[2], command_args[3], spec);
  if (!s.IsOK()) {
    return MakeParseError(s.Msg());
  }

  size_t i = 4;
  while (i + 1 < command_args.size()) {
    const auto option = Util::ToLower(command_args[i]);
    if (option == "withscores") {
      *with_scores = true;
      ++i;
      continue;
    }
    if (option == "limit" && i + 2 < command_args.size() - 1) {
      auto offset = ParseInt<int>(command_args[i + 1], 10);
      auto count = ParseInt<int>(command_args[i + 2], 10);
      if (!offset.IsOK() || !count.IsOK()) {
        return MakeParseError("value is not an integer or out of range");
      }
      spec->offset = offset.GetValue();
      spec->count = count.GetValue();
      i += 3;
      continue;
    }
    return MakeParseError("syntax error");
  }

  return Status::OK();
}

}  // namespace

CoreCommandExecutor::CoreCommandExecutor(Engine::Storage *storage, const Config &config)
    : storage_(storage), slot_id_encoded_(config.slot_id_encoded) {}

StatusOr<ParsedCommandFlags> CoreCommandExecutor::ParseFlags(const brpc::RedisRequest &request) const {
  // 先清空旧缓存，确保本次解析失败时不会误留下上一条命令。
  ClearParsedRequest();

  // 当前只从请求中的首条命令提取 flags，忽略pipeline 里的后续命令。
  auto command_request = ParseRequest(request);
  if (!command_request.IsOK()) return command_request.ToStatus();

  const uint64_t flags = command_request->flags;
  SaveParsedRequest(std::move(*command_request));
  return ParsedCommandFlags{flags, InferTaskType(flags)};
}

StatusOr<CoreCommandResult> CoreCommandExecutor::Execute() const {
  if (storage_ == nullptr) {
    return MakeInvalidRequest("storage is required");
  }

  // 默认调用链里，core 线程池会先调用 ParseFlags(request) 完成解析并缓存结果，
  // 因此这里直接消费缓存的 command_request，不再重复接收 request。
  auto command_request = TakeParsedRequest();
  if (!command_request.IsOK()) return command_request.ToStatus();

  switch (command_request->kind) {
    case CoreCommandKind::kDel:
      return ExecuteKeyWrite(command_request->kind, command_request->command_args);
    case CoreCommandKind::kGet:
    case CoreCommandKind::kMGet:
      return ExecuteStringRead(command_request->kind, command_request->command_args);
    case CoreCommandKind::kSet:
    case CoreCommandKind::kMSet:
      return ExecuteStringWrite(command_request->kind, command_request->command_args);
    case CoreCommandKind::kHGet:
    case CoreCommandKind::kHMGet:
    case CoreCommandKind::kHGetAll:
      return ExecuteHashRead(command_request->kind, command_request->command_args);
    case CoreCommandKind::kHSet:
    case CoreCommandKind::kHMSet:
      return ExecuteHashWrite(command_request->kind, command_request->command_args);
    case CoreCommandKind::kLIndex:
      return ExecuteListRead(command_request->kind, command_request->command_args);
    case CoreCommandKind::kLSet:
      return ExecuteListWrite(command_request->kind, command_request->command_args);
    case CoreCommandKind::kZRangeByScore:
      return ExecuteZSetRead(command_request->kind, command_request->command_args);
    case CoreCommandKind::kZAdd:
    case CoreCommandKind::kZRem:
      return ExecuteZSetWrite(command_request->kind, command_request->command_args);
    case CoreCommandKind::kUnknown:
      return MakeInvalidRequest("unknown core command kind");
  }

  return MakeInvalidRequest("unsupported core command kind");
}

StatusOr<CoreCommandExecutor::CoreCommandRequest> CoreCommandExecutor::ParseRequest(
    const brpc::RedisRequest &request) const {
  // 统一做命令名识别、最小参数校验和 flags 解析，避免执行阶段重复判断。
  auto command_args = GetCommand(request);
  if (!command_args.IsOK()) return command_args.ToStatus();

  auto spec = GetCommandSpec(command_args->front());
  if (!spec.IsOK()) return spec.ToStatus();

  auto validate_status = ValidateMinArity(*command_args, spec->min_arity_with_flags);
  if (!validate_status.IsOK()) return validate_status;

  auto flags = ParseTailFlags(*command_args);
  if (!flags.IsOK()) return flags.ToStatus();

  auto command_validate_status = ValidateCommandArguments(spec->kind, *command_args);
  if (!command_validate_status.IsOK()) return command_validate_status;

  return CoreCommandRequest(std::move(*command_args), spec->kind, flags.GetValue());
}

void CoreCommandExecutor::ClearParsedRequest() const {
  std::lock_guard<std::mutex> lock(parsed_request_mutex_);
  parsed_request_.reset();
}

void CoreCommandExecutor::SaveParsedRequest(CoreCommandRequest request) const {
  // ParseFlags 和 Execute 之间通过一个简单缓存传递解析结果。
  // 只有成功解析的请求才会进入缓存。
  std::lock_guard<std::mutex> lock(parsed_request_mutex_);
  parsed_request_ = std::make_unique<CoreCommandRequest>(std::move(request));
}

StatusOr<CoreCommandExecutor::CoreCommandRequest> CoreCommandExecutor::TakeParsedRequest() const {
  // Execute 取走后立刻清空，避免同一份解析结果被重复消费。
  // 如果最近一次 ParseFlags 失败，缓存已经被清空，这里会直接报错。
  std::lock_guard<std::mutex> lock(parsed_request_mutex_);
  if (parsed_request_ == nullptr) {
    return MakeInvalidRequest("parsed core command request is required");
  }

  CoreCommandRequest request = std::move(*parsed_request_);
  parsed_request_.reset();
  return request;
}

Status CoreCommandExecutor::DecodeRemoteKey(const std::string &remote_key, DecodedRemoteKey *decoded_key) const {
  if (decoded_key == nullptr) {
    return MakeInvalidRequest("decoded key output is required");
  }
  if (remote_key.empty()) {
    return MakeInvalidRequest("remote key is required");
  }

  // 与 proxy 的 ComposeRemoteKey 相反，这里负责把 namespace 编码后的 key 还原回来。
  ExtractNamespaceKey(remote_key, &decoded_key->namespace_name, &decoded_key->user_key, slot_id_encoded_);
  if (decoded_key->namespace_name.empty()) {
    return MakeInvalidRequest("remote key namespace is empty");
  }

  return Status::OK();
}

StatusOr<CoreCommandResult> CoreCommandExecutor::ExecuteStringRead(CoreCommandKind kind,
                                                                   const std::vector<std::string> &command_args) const {
  switch (kind) {
    case CoreCommandKind::kGet: {
      if (command_args.size() != 3) return MakeParseError("wrong number of arguments");

      DecodedRemoteKey decoded_key;
      auto decode_status = DecodeRemoteKey(command_args[1], &decoded_key);
      if (!decode_status.IsOK()) return decode_status;

      Redis::String string_db(storage_, decoded_key.namespace_name);
      std::string value;
      auto s = string_db.Get(decoded_key.user_key, &value);
      if (s.ok()) return CoreCommandResult::String(std::move(value));
      if (s.IsNotFound()) return CoreCommandResult::Nil();
      return MakeExecutionError(s);
    }
    case CoreCommandKind::kMGet: {
      // MGET 在执行器侧逐 key 读取，再封装为数组结果返回给协议层。
      if (command_args.size() < 3) {
        return MakeParseError("wrong number of arguments");
      }

      std::vector<DecodedRemoteKey> decoded_keys;
      decoded_keys.reserve(command_args.size() - 2);
      for (size_t i = 1; i + 1 < command_args.size(); ++i) {
        DecodedRemoteKey decoded_key;
        auto s = DecodeRemoteKey(command_args[i], &decoded_key);
        if (!s.IsOK()) return s;
        decoded_keys.emplace_back(std::move(decoded_key));
      }

      std::vector<std::string> values;
      std::vector<bool> founds;
      values.reserve(decoded_keys.size());
      founds.reserve(decoded_keys.size());
      for (const auto &decoded_key : decoded_keys) {
        // 每个 key 都按自身 namespace 路由到对应的 Redis::String 视图。
        Redis::String string_db(storage_, decoded_key.namespace_name);
        std::string value;
        auto s = string_db.Get(decoded_key.user_key, &value);
        if (s.ok()) {
          values.emplace_back(std::move(value));
          founds.emplace_back(true);
          continue;
        }
        if (s.IsNotFound()) {
          values.emplace_back("");
          founds.emplace_back(false);
          continue;
        }
        return MakeExecutionError(s);
      }

      return CoreCommandResult::Array(std::move(values), std::move(founds));
    }
    default:
      return MakeInvalidRequest("unexpected string read command kind");
  }
}

StatusOr<CoreCommandResult> CoreCommandExecutor::ExecuteStringWrite(CoreCommandKind kind,
                                                                    const std::vector<std::string> &command_args) const {
  switch (kind) {
    case CoreCommandKind::kSet: {
      if (command_args.size() != 4) return MakeParseError("wrong number of arguments");

      DecodedRemoteKey decoded_key;
      auto decode_status = DecodeRemoteKey(command_args[1], &decoded_key);
      if (!decode_status.IsOK()) return decode_status;

      Redis::String string_db(storage_, decoded_key.namespace_name);
      auto s = string_db.Set(decoded_key.user_key, command_args[2]);
      if (!s.ok()) return MakeExecutionError(s);
      return CoreCommandResult::Status("OK");
    }
    case CoreCommandKind::kMSet: {
      // MSET 要求所有 key 属于同一 namespace，便于复用同一个 Redis::String 实例。
      if (command_args.size() < 4 || (command_args.size() % 2) != 0) {
        return MakeParseError("wrong number of arguments");
      }

      std::vector<DecodedRemoteKey> decoded_keys;
      decoded_keys.reserve((command_args.size() - 2) / 2);
      for (size_t i = 1; i + 2 < command_args.size(); i += 2) {
        DecodedRemoteKey decoded_key;
        auto s = DecodeRemoteKey(command_args[i], &decoded_key);
        if (!s.IsOK()) return s;
        decoded_keys.emplace_back(std::move(decoded_key));
      }

      std::string namespace_name = decoded_keys[0].namespace_name;
      for (const auto &decoded_key : decoded_keys) {
        if (decoded_key.namespace_name != namespace_name) {
          return MakeInvalidRequest("multi-key request must target the same namespace");
        }
      }

      std::vector<StringPair> key_values;
      key_values.reserve(decoded_keys.size());
      for (size_t i = 0; i < decoded_keys.size(); ++i) {
        key_values.emplace_back(StringPair{decoded_keys[i].user_key, command_args[2 * i + 2]});
      }

      Redis::String string_db(storage_, namespace_name);
      auto s = string_db.MSet(key_values);
      if (!s.ok()) return MakeExecutionError(s);
      return CoreCommandResult::Status("OK");
    }
    default:
      return MakeInvalidRequest("unexpected string write command kind");
  }
}

StatusOr<CoreCommandResult> CoreCommandExecutor::ExecuteKeyWrite(CoreCommandKind kind,
                                                                 const std::vector<std::string> &command_args) const {
  if (kind != CoreCommandKind::kDel) {
    return MakeInvalidRequest("unexpected key write command kind");
  }
  if (command_args.size() < 3) return MakeParseError("wrong number of arguments");

  int deleted = 0;
  for (size_t i = 1; i + 1 < command_args.size(); ++i) {
    DecodedRemoteKey decoded_key;
    auto decode_status = DecodeRemoteKey(command_args[i], &decoded_key);
    if (!decode_status.IsOK()) return decode_status;

    Redis::Database redis_db(storage_, decoded_key.namespace_name);
    auto s = redis_db.Del(decoded_key.user_key);
    if (s.ok()) {
      ++deleted;
      continue;
    }
    if (s.IsNotFound()) {
      continue;
    }
    return MakeExecutionError(s);
  }

  return CoreCommandResult::Integer(deleted);
}

StatusOr<CoreCommandResult> CoreCommandExecutor::ExecuteHashRead(CoreCommandKind kind,
                                                                 const std::vector<std::string> &command_args) const {
  DecodedRemoteKey decoded_key;
  auto decode_status = DecodeRemoteKey(command_args[1], &decoded_key);
  if (!decode_status.IsOK()) return decode_status;

  Redis::Hash hash_db(storage_, decoded_key.namespace_name);
  switch (kind) {
    case CoreCommandKind::kHGet: {
      if (command_args.size() != 4) return MakeParseError("wrong number of arguments");

      // HGET: 先解码远端 key，再在对应 namespace 下访问 hash field。
      std::string value;
      auto s = hash_db.Get(decoded_key.user_key, command_args[2], &value);
      if (s.ok()) return CoreCommandResult::String(std::move(value));
      if (s.IsNotFound()) return CoreCommandResult::Nil();
      return MakeExecutionError(s);
    }
    case CoreCommandKind::kHMGet: {
      if (command_args.size() < 4) return MakeParseError("wrong number of arguments");

      std::vector<Slice> fields;
      fields.reserve(command_args.size() - 3);
      for (size_t i = 2; i + 1 < command_args.size(); ++i) {
        fields.emplace_back(command_args[i]);
      }

      std::vector<std::string> values;
      std::vector<rocksdb::Status> statuses;
      auto s = hash_db.MGet(decoded_key.user_key, fields, &values, &statuses);
      if (!s.ok() && !s.IsNotFound()) return MakeExecutionError(s);

      std::vector<bool> founds;
      if (s.IsNotFound()) {
        values.resize(fields.size(), "");
        founds.assign(fields.size(), false);
      } else {
        founds.reserve(statuses.size());
        for (const auto &status : statuses) {
          founds.emplace_back(status.ok());
        }
      }
      return CoreCommandResult::Array(std::move(values), std::move(founds));
    }
    case CoreCommandKind::kHGetAll: {
      if (command_args.size() != 3) return MakeParseError("wrong number of arguments");

      std::vector<FieldValue> field_values;
      auto s = hash_db.GetAll(decoded_key.user_key, &field_values);
      if (!s.ok()) return MakeExecutionError(s);

      std::vector<std::string> values;
      std::vector<bool> founds;
      values.reserve(field_values.size() * 2);
      founds.reserve(field_values.size() * 2);
      for (const auto &field_value : field_values) {
        values.emplace_back(field_value.field);
        founds.emplace_back(true);
        values.emplace_back(field_value.value);
        founds.emplace_back(true);
      }
      return CoreCommandResult::Array(std::move(values), std::move(founds));
    }
    default:
      return MakeInvalidRequest("unexpected hash read command kind");
  }
}

StatusOr<CoreCommandResult> CoreCommandExecutor::ExecuteHashWrite(CoreCommandKind kind,
                                                                  const std::vector<std::string> &command_args) const {
  DecodedRemoteKey decoded_key;
  auto decode_status = DecodeRemoteKey(command_args[1], &decoded_key);
  if (!decode_status.IsOK()) return decode_status;

  Redis::Hash hash_db(storage_, decoded_key.namespace_name);
  switch (kind) {
    case CoreCommandKind::kHSet: {
      if (command_args.size() != 5) return MakeParseError("wrong number of arguments");

      // HSET 返回新增 field 数量，因此这里封装为整数结果。
      int ret = 0;
      auto s = hash_db.Set(decoded_key.user_key, command_args[2], command_args[3], &ret);
      if (!s.ok()) return MakeExecutionError(s);
      return CoreCommandResult::Integer(ret);
    }
    case CoreCommandKind::kHMSet: {
      if (command_args.size() < 5 || (command_args.size() % 2) == 0) {
        return MakeParseError("wrong number of arguments");
      }

      std::vector<FieldValue> field_values;
      field_values.reserve((command_args.size() - 3) / 2);
      for (size_t i = 2; i + 2 < command_args.size(); i += 2) {
        field_values.emplace_back(FieldValue{command_args[i], command_args[i + 1]});
      }

      int ret = 0;
      auto s = hash_db.MSet(decoded_key.user_key, field_values, false, &ret);
      if (!s.ok()) return MakeExecutionError(s);
      return CoreCommandResult::Status("OK");
    }
    default:
      return MakeInvalidRequest("unexpected hash write command kind");
  }
}

StatusOr<CoreCommandResult> CoreCommandExecutor::ExecuteListRead(CoreCommandKind kind,
                                                                 const std::vector<std::string> &command_args) const {
  if (kind != CoreCommandKind::kLIndex) {
    return MakeInvalidRequest("unexpected list read command kind");
  }
  if (command_args.size() != 4) return MakeParseError("wrong number of arguments");

  // LINDEX: 读取 list 指定下标，未命中时返回 nil。
  auto index = ParseInt<int>(command_args[2], 10);
  if (!index.IsOK()) return MakeParseError("value is not an integer or out of range");

  DecodedRemoteKey decoded_key;
  auto decode_status = DecodeRemoteKey(command_args[1], &decoded_key);
  if (!decode_status.IsOK()) return decode_status;

  Redis::List list_db(storage_, decoded_key.namespace_name);
  std::string value;
  auto s = list_db.Index(decoded_key.user_key, index.GetValue(), &value);
  if (s.ok()) return CoreCommandResult::String(std::move(value));
  if (s.IsNotFound()) return CoreCommandResult::Nil();
  return MakeExecutionError(s);
}

StatusOr<CoreCommandResult> CoreCommandExecutor::ExecuteListWrite(CoreCommandKind kind,
                                                                  const std::vector<std::string> &command_args) const {
  if (kind != CoreCommandKind::kLSet) {
    return MakeInvalidRequest("unexpected list write command kind");
  }
  if (command_args.size() != 5) return MakeParseError("wrong number of arguments");

  // LSET 成功时保持与 Redis 语义一致，返回状态字符串 "OK"。
  auto index = ParseInt<int>(command_args[2], 10);
  if (!index.IsOK()) return MakeParseError("value is not an integer or out of range");

  DecodedRemoteKey decoded_key;
  auto decode_status = DecodeRemoteKey(command_args[1], &decoded_key);
  if (!decode_status.IsOK()) return decode_status;

  Redis::List list_db(storage_, decoded_key.namespace_name);
  auto s = list_db.Set(decoded_key.user_key, index.GetValue(), command_args[3]);
  if (!s.ok()) return MakeExecutionError(s);

  // TODO(@core-command-executor): 如果后续协议适配层需要区分简单字符串状态
  // 和更强类型的状态元数据，可以在这里扩展更丰富的状态载荷。
  return CoreCommandResult::Status("OK");
}

StatusOr<CoreCommandResult> CoreCommandExecutor::ExecuteZSetRead(CoreCommandKind kind,
                                                                 const std::vector<std::string> &command_args) const {
  if (kind != CoreCommandKind::kZRangeByScore) {
    return MakeInvalidRequest("unexpected zset read command kind");
  }
  if (command_args.size() < 5) return MakeParseError("wrong number of arguments");

  ZRangeSpec spec;
  bool with_scores = false;
  auto parse_status = ParseZRangeByScoreArgs(command_args, &spec, &with_scores);
  if (!parse_status.IsOK()) return parse_status;

  DecodedRemoteKey decoded_key;
  auto decode_status = DecodeRemoteKey(command_args[1], &decoded_key);
  if (!decode_status.IsOK()) return decode_status;

  Redis::ZSet zset_db(storage_, decoded_key.namespace_name);
  std::vector<MemberScore> member_scores;
  int size = 0;
  auto s = zset_db.RangeByScore(decoded_key.user_key, spec, &member_scores, &size);
  if (!s.ok()) return MakeExecutionError(s);

  std::vector<std::string> values;
  std::vector<bool> founds;
  values.reserve(with_scores ? member_scores.size() * 2 : member_scores.size());
  founds.reserve(values.capacity());
  for (const auto &member_score : member_scores) {
    values.emplace_back(member_score.member);
    founds.emplace_back(true);
    if (with_scores) {
      values.emplace_back(Util::Float2String(member_score.score));
      founds.emplace_back(true);
    }
  }
  return CoreCommandResult::Array(std::move(values), std::move(founds));
}

StatusOr<CoreCommandResult> CoreCommandExecutor::ExecuteZSetWrite(CoreCommandKind kind,
                                                                  const std::vector<std::string> &command_args) const {
  if (kind == CoreCommandKind::kZAdd) {
    if (command_args.size() < 5) return MakeParseError("wrong number of arguments");

    size_t value_index = 2;
    ZAddFlags flags = ZAddFlags::Default();
    auto parse_status = ParseZAddFlags(command_args, &value_index, &flags);
    if (!parse_status.IsOK()) return parse_status;

    const size_t pairs = command_args.size() - value_index - 1;
    if ((flags.HasIncr() && pairs != 2) || pairs == 0 || (pairs % 2) != 0) {
      return MakeParseError(flags.HasIncr() ? "INCR option supports a single increment-element pair" : "syntax error");
    }

    std::vector<MemberScore> member_scores;
    member_scores.reserve(pairs / 2);
    try {
      for (size_t i = value_index; i + 2 < command_args.size(); i += 2) {
        double score = std::stod(command_args[i]);
        if (std::isnan(score)) {
          return MakeParseError("score is not a valid float");
        }
        member_scores.emplace_back(MemberScore{command_args[i + 1], score});
      }
    } catch (const std::exception &) {
      return MakeParseError("value is not a float");
    }

    DecodedRemoteKey decoded_key;
    auto decode_status = DecodeRemoteKey(command_args[1], &decoded_key);
    if (!decode_status.IsOK()) return decode_status;

    Redis::ZSet zset_db(storage_, decoded_key.namespace_name);
    int ret = 0;
    const double old_score = member_scores[0].score;
    auto s = zset_db.Add(decoded_key.user_key, flags, &member_scores, &ret);
    if (!s.ok()) return MakeExecutionError(s);

    if (!flags.HasIncr()) {
      return CoreCommandResult::Integer(ret);
    }

    const double new_score = member_scores[0].score;
    if ((flags.HasNX() || flags.HasXX() || flags.HasLT() || flags.HasGT()) && old_score == new_score && ret == 0) {
      return CoreCommandResult::Nil();
    }
    return CoreCommandResult::String(Util::Float2String(new_score));
  }

  if (kind == CoreCommandKind::kZRem) {
    if (command_args.size() < 4) return MakeParseError("wrong number of arguments");

    DecodedRemoteKey decoded_key;
    auto decode_status = DecodeRemoteKey(command_args[1], &decoded_key);
    if (!decode_status.IsOK()) return decode_status;

    std::vector<Slice> members;
    members.reserve(command_args.size() - 3);
    for (size_t i = 2; i + 1 < command_args.size(); ++i) {
      members.emplace_back(command_args[i]);
    }

    Redis::ZSet zset_db(storage_, decoded_key.namespace_name);
    int removed = 0;
    auto s = zset_db.Remove(decoded_key.user_key, members, &removed);
    if (!s.ok()) return MakeExecutionError(s);
    return CoreCommandResult::Integer(removed);
  }

  return MakeInvalidRequest("unexpected zset write command kind");
}

}  // namespace kvrocks_datanode
