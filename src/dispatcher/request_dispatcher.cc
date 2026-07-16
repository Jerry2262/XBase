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

#include "dispatcher/request_dispatcher.h"

#include <brpc/redis.h>
#include <butil/strings/string_piece.h>

#include <array>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "brpc_client/brpc_client.h"
#include "cluster/redis_slot.h"
#include "config/config.h"
#include "storage/redis_metadata.h"
namespace Dispatcher {

namespace {

// 将 brpc 的 RedisReply 数据区转成普通 std::string，方便上层统一处理。
std::string ReplyToString(const brpc::RedisReply &reply) {
  auto data = reply.data();
  return std::string(data.data(), data.size());
}

StatusOr<MultiRequestResult> ParseArrayResponse(const brpc::RedisResponse &response, const std::string &command_name);

// 解析单值读取类命令的返回结果，例如 GET/HGET/LINDEX。
// nil 表示未命中，其余非 error 的字符串结果写入 RequestResult::value。
StatusOr<RequestResult> ParseGetResponse(const brpc::RedisResponse &response) {
  if (response.reply_size() != 1) {
    return Status(Status::RedisExecErr, "unexpected GET response size");
  }

  const auto &reply = response.reply(0);
  if (reply.is_nil()) return RequestResult{};
  if (reply.is_error()) {
    return Status(Status::RedisExecErr, reply.error_message());
  }

  RequestResult result;
  result.found = true;
  result.value = ReplyToString(reply);
  return result;
}

// 解析整数型返回结果，例如 HSET 返回新增字段数。
StatusOr<RequestResult> ParseIntegerResponse(const brpc::RedisResponse &response, const std::string &command_name) {
  if (response.reply_size() != 1) {
    return Status(Status::RedisExecErr, "unexpected " + command_name + " response size");
  }

  const auto &reply = response.reply(0);
  if (reply.is_error()) {
    return Status(Status::RedisExecErr, reply.error_message());
  }
  if (!reply.is_integer()) {
    return Status(Status::RedisExecErr, "unexpected " + command_name + " response type");
  }

  RequestResult result;
  result.integer = reply.integer();
  return result;
}

StatusOr<RequestResult> ParseSingleResponse(const brpc::RedisResponse &response, const std::string &command_name) {
  if (response.reply_size() != 1) {
    return Status(Status::RedisExecErr, "unexpected " + command_name + " response size");
  }

  const auto &reply = response.reply(0);
  if (reply.is_error()) {
    return Status(Status::RedisExecErr, reply.error_message());
  }

  RequestResult result;
  if (reply.is_nil()) {
    return result;
  }
  if (reply.is_integer()) {
    result.integer = reply.integer();
    return result;
  }

  result.found = true;
  result.value = ReplyToString(reply);
  return result;
}

// 解析状态型返回结果，例如 SET/LSET/MSET 这类返回 "OK" 的命令。
StatusOr<std::string> ParseStatusResponse(const brpc::RedisResponse &response, const std::string &command_name) {
  if (response.reply_size() != 1) {
    return Status(Status::RedisExecErr, "unexpected " + command_name + " response size");
  }

  const auto &reply = response.reply(0);
  if (reply.is_error()) {
    return Status(Status::RedisExecErr, reply.error_message());
  }
  if (reply.is_nil()) {
    return Status(Status::RedisExecErr, "unexpected nil " + command_name + " response");
  }

  auto status = ReplyToString(reply);
  if (status != "OK") {
    return Status(Status::RedisExecErr, "unexpected " + command_name + " response: " + status);
  }
  return status;
}

StatusOr<RequestResult> ParseSetResponse(const brpc::RedisResponse &response, const std::string &command_name) {
  auto status = ParseStatusResponse(response, command_name);
  if (!status.IsOK()) return status.ToStatus();

  RequestResult result;
  result.status = std::move(*status);
  return result;
}

// 解析 MGET 返回结果。每个 reply 都会映射到 founds/values 中同一位置。
StatusOr<MultiRequestResult> ParseMGetResponse(const brpc::RedisResponse &response) {
  return ParseArrayResponse(response, "MGET");
}

// 解析 MSET 返回结果，预期只返回一个整体状态。
StatusOr<MultiRequestResult> ParseMSetResponse(const brpc::RedisResponse &response) {
  auto status = ParseStatusResponse(response, "MSET");
  if (!status.IsOK()) return status.ToStatus();

  MultiRequestResult result;
  result.status = std::move(*status);
  return result;
}

StatusOr<MultiRequestResult> ParseArrayResponse(const brpc::RedisResponse &response, const std::string &command_name) {
  if (response.reply_size() != 1) {
    return Status(Status::RedisExecErr, "unexpected " + command_name + " response size");
  }

  const auto &array_reply = response.reply(0);
  if (array_reply.is_error()) {
    return Status(Status::RedisExecErr, array_reply.error_message());
  }
  if (!array_reply.is_array()) {
    return Status(Status::RedisExecErr, "unexpected " + command_name + " response type");
  }

  MultiRequestResult result;
  result.founds.reserve(array_reply.size());
  result.values.reserve(array_reply.size());
  for (size_t i = 0; i < array_reply.size(); ++i) {
    const auto &reply = array_reply[i];
    if (reply.is_error()) {
      return Status(Status::RedisExecErr, reply.error_message());
    }
    result.founds.emplace_back(!reply.is_nil());
    result.values.emplace_back(reply.is_nil() ? std::string() : ReplyToString(reply));
  }
  return result;
}

Status InvalidRequest(const char *command_name, const char *message) {
  return Status(Status::RedisExecErr, std::string(command_name) + " request " + message);
}

// proxy 与 datanode 之间约定：把 Commander 原始 flags 作为命令末尾的内部十进制参数透传。
std::string EncodeCommandFlags(uint64_t command_flags) { return std::to_string(command_flags); }

template <typename Result, typename Callback, typename Parser>
BrpcClient::ResponseCallback MakeAsyncResponseCallback(Callback callback, Parser parser) {
  return [callback = std::move(callback), parser = std::move(parser)](Status status,
                                                                      const brpc::RedisResponse &response) mutable {
    if (!status.IsOK()) {
      callback(StatusOr<Result>(std::move(status)));
      return;
    }
    callback(parser(response));
  };
}

}  // namespace

// 初始化远端 Redis brpc 通道，后续所有命令分发都复用该连接配置。
RequestDispatcher::RequestDispatcher(const Config &config) : slot_id_encoded_(config.slot_id_encoded) {
  init_result_ = brpc_client_.Init(config.storage_backend_addrs.c_str(), config.storage_rpc_connection_type.c_str(),
                                   config.storage_rpc_timeout_ms, config.storage_rpc_max_retry);
}

Status RequestDispatcher::ExecuteRedisCommand(const brpc::RedisRequest &redis_request,
                                              brpc::RedisResponse *redis_response) const {
  if (redis_response == nullptr) {
    return Status(Status::RedisExecErr, "redis response output is required");
  }
  return brpc_client_.RequestSync(redis_request, redis_response);
}

Status RequestDispatcher::ExecuteRedisCommandAsync(brpc::RedisRequest redis_request,
                                                   BrpcClient::ResponseCallback callback) const {
  return brpc_client_.RequestAsync(std::move(redis_request), std::move(callback));
}

// 处理所有单 key 的读请求，根据数据结构类型翻译成不同 Redis 命令。
StatusOr<RequestResult> RequestDispatcher::DispatchRequest(const GetRequestCommand &request) const {
  if (request.kind != RequestKind::kGet) {
    return InvalidRequest("GET", "kind mismatch");
  }

  switch (request.structure) {
    case DataStructureType::kString: {
      const auto *string_request = dynamic_cast<const StringGetRequest *>(&request);
      if (!string_request) return InvalidRequest("GET", "does not match string request payload");

      brpc::RedisRequest redis_request;
      // 远端实际访问的 key 需要带 namespace，集群模式下还可能附带 slot 编码。
      auto remote_key = ComposeRemoteKey(string_request->ns, string_request->key);
      auto command_flags = EncodeCommandFlags(string_request->command_flags);
      if (!redis_request.AddCommand("GET %b %s", remote_key.data(), remote_key.size(), command_flags.c_str())) {
        return Status(Status::RedisExecErr, "failed to build GET request");
      }

      brpc::RedisResponse redis_response;
      auto s = ExecuteRedisCommand(redis_request, &redis_response);
      if (!s.IsOK()) return s;
      return ParseGetResponse(redis_response);
    }
    case DataStructureType::kHash: {
      const auto *hash_request = dynamic_cast<const HashGetRequest *>(&request);
      if (!hash_request) return InvalidRequest("GET", "does not match hash request payload");

      brpc::RedisRequest redis_request;
      auto remote_key = ComposeRemoteKey(hash_request->ns, hash_request->key);
      auto command_flags = EncodeCommandFlags(hash_request->command_flags);
      if (!redis_request.AddCommand("HGET %b %b %s", remote_key.data(), remote_key.size(), hash_request->field.data(),
                                    hash_request->field.size(), command_flags.c_str())) {
        return Status(Status::RedisExecErr, "failed to build HGET request");
      }

      brpc::RedisResponse redis_response;
      auto s = ExecuteRedisCommand(redis_request, &redis_response);
      if (!s.IsOK()) return s;
      return ParseGetResponse(redis_response);
    }
    case DataStructureType::kList: {
      const auto *list_request = dynamic_cast<const ListGetRequest *>(&request);
      if (!list_request) return InvalidRequest("GET", "does not match list request payload");

      brpc::RedisRequest redis_request;
      auto remote_key = ComposeRemoteKey(list_request->ns, list_request->key);
      auto command_flags = EncodeCommandFlags(list_request->command_flags);
      if (!redis_request.AddCommand("LINDEX %b %d %s", remote_key.data(), remote_key.size(), list_request->index,
                                    command_flags.c_str())) {
        return Status(Status::RedisExecErr, "failed to build LINDEX request");
      }

      brpc::RedisResponse redis_response;
      auto s = ExecuteRedisCommand(redis_request, &redis_response);
      if (!s.IsOK()) return s;
      return ParseGetResponse(redis_response);
    }
  }

  return Status(Status::RedisExecErr, "unsupported GET request structure");
}

Status RequestDispatcher::DispatchRequestAsync(const GetRequestCommand &request, RequestResultCallback callback) const {
  if (request.kind != RequestKind::kGet) return InvalidRequest("GET", "kind mismatch");

  brpc::RedisRequest redis_request;
  switch (request.structure) {
    case DataStructureType::kString: {
      const auto *string_request = dynamic_cast<const StringGetRequest *>(&request);
      if (!string_request) return InvalidRequest("GET", "does not match string request payload");
      auto remote_key = ComposeRemoteKey(string_request->ns, string_request->key);
      auto command_flags = EncodeCommandFlags(string_request->command_flags);
      if (!redis_request.AddCommand("GET %b %s", remote_key.data(), remote_key.size(), command_flags.c_str())) {
        return Status(Status::RedisExecErr, "failed to build GET request");
      }
      break;
    }
    case DataStructureType::kHash: {
      const auto *hash_request = dynamic_cast<const HashGetRequest *>(&request);
      if (!hash_request) return InvalidRequest("GET", "does not match hash request payload");
      auto remote_key = ComposeRemoteKey(hash_request->ns, hash_request->key);
      auto command_flags = EncodeCommandFlags(hash_request->command_flags);
      if (!redis_request.AddCommand("HGET %b %b %s", remote_key.data(), remote_key.size(), hash_request->field.data(),
                                    hash_request->field.size(), command_flags.c_str())) {
        return Status(Status::RedisExecErr, "failed to build HGET request");
      }
      break;
    }
    case DataStructureType::kList: {
      const auto *list_request = dynamic_cast<const ListGetRequest *>(&request);
      if (!list_request) return InvalidRequest("GET", "does not match list request payload");
      auto remote_key = ComposeRemoteKey(list_request->ns, list_request->key);
      auto command_flags = EncodeCommandFlags(list_request->command_flags);
      if (!redis_request.AddCommand("LINDEX %b %d %s", remote_key.data(), remote_key.size(), list_request->index,
                                    command_flags.c_str())) {
        return Status(Status::RedisExecErr, "failed to build LINDEX request");
      }
      break;
    }
  }
  return ExecuteRedisCommandAsync(
      std::move(redis_request),
      MakeAsyncResponseCallback<RequestResult>(
          std::move(callback), [](const brpc::RedisResponse &response) { return ParseGetResponse(response); }));
}

// 处理所有单 key 写请求，根据数据结构类型翻译成 SET/HSET/LSET。
StatusOr<RequestResult> RequestDispatcher::DispatchRequest(const SetRequestCommand &request) const {
  if (request.kind != RequestKind::kSet) {
    return InvalidRequest("SET", "kind mismatch");
  }

  switch (request.structure) {
    case DataStructureType::kString: {
      const auto *string_request = dynamic_cast<const StringSetRequest *>(&request);
      if (!string_request) return InvalidRequest("SET", "does not match string request payload");

      brpc::RedisRequest redis_request;
      auto remote_key = ComposeRemoteKey(string_request->ns, string_request->key);
      auto command_flags = EncodeCommandFlags(string_request->command_flags);
      if (!redis_request.AddCommand("SET %b %b %s", remote_key.data(), remote_key.size(), string_request->value.data(),
                                    string_request->value.size(), command_flags.c_str())) {
        return Status(Status::RedisExecErr, "failed to build SET request");
      }

      brpc::RedisResponse redis_response;
      auto s = ExecuteRedisCommand(redis_request, &redis_response);
      if (!s.IsOK()) return s;
      return ParseSetResponse(redis_response, "SET");
    }
    case DataStructureType::kHash: {
      const auto *hash_request = dynamic_cast<const HashSetRequest *>(&request);
      if (!hash_request) return InvalidRequest("SET", "does not match hash request payload");

      brpc::RedisRequest redis_request;
      auto remote_key = ComposeRemoteKey(hash_request->ns, hash_request->key);
      auto command_flags = EncodeCommandFlags(hash_request->command_flags);
      // HSET 需要携带 3 段二进制参数再追加 flags，超出 AddCommand 的模板参数上限，因此改用组件方式拼装。
      std::array<butil::StringPiece, 5> components = {
          butil::StringPiece("HSET"), butil::StringPiece(remote_key), butil::StringPiece(hash_request->field),
          butil::StringPiece(hash_request->value), butil::StringPiece(command_flags)};
      if (!redis_request.AddCommandByComponents(components.data(), components.size())) {
        return Status(Status::RedisExecErr, "failed to build HSET request");
      }

      brpc::RedisResponse redis_response;
      auto s = ExecuteRedisCommand(redis_request, &redis_response);
      if (!s.IsOK()) return s;
      return ParseIntegerResponse(redis_response, "HSET");
    }
    case DataStructureType::kList: {
      const auto *list_request = dynamic_cast<const ListSetRequest *>(&request);
      if (!list_request) return InvalidRequest("SET", "does not match list request payload");

      brpc::RedisRequest redis_request;
      auto remote_key = ComposeRemoteKey(list_request->ns, list_request->key);
      auto command_flags = EncodeCommandFlags(list_request->command_flags);
      if (!redis_request.AddCommand("LSET %b %d %b %s", remote_key.data(), remote_key.size(), list_request->index,
                                    list_request->value.data(), list_request->value.size(), command_flags.c_str())) {
        return Status(Status::RedisExecErr, "failed to build LSET request");
      }

      brpc::RedisResponse redis_response;
      auto s = ExecuteRedisCommand(redis_request, &redis_response);
      if (!s.IsOK()) return s;
      return ParseSetResponse(redis_response, "LSET");
    }
  }

  return Status(Status::RedisExecErr, "unsupported SET request structure");
}

Status RequestDispatcher::DispatchRequestAsync(const SetRequestCommand &request, RequestResultCallback callback) const {
  if (request.kind != RequestKind::kSet) return InvalidRequest("SET", "kind mismatch");

  brpc::RedisRequest redis_request;
  std::string response_command;
  bool integer_response = false;
  switch (request.structure) {
    case DataStructureType::kString: {
      const auto *string_request = dynamic_cast<const StringSetRequest *>(&request);
      if (!string_request) return InvalidRequest("SET", "does not match string request payload");
      auto remote_key = ComposeRemoteKey(string_request->ns, string_request->key);
      auto command_flags = EncodeCommandFlags(string_request->command_flags);
      if (!redis_request.AddCommand("SET %b %b %s", remote_key.data(), remote_key.size(), string_request->value.data(),
                                    string_request->value.size(), command_flags.c_str())) {
        return Status(Status::RedisExecErr, "failed to build SET request");
      }
      response_command = "SET";
      break;
    }
    case DataStructureType::kHash: {
      const auto *hash_request = dynamic_cast<const HashSetRequest *>(&request);
      if (!hash_request) return InvalidRequest("SET", "does not match hash request payload");
      auto remote_key = ComposeRemoteKey(hash_request->ns, hash_request->key);
      auto command_flags = EncodeCommandFlags(hash_request->command_flags);
      std::array<butil::StringPiece, 5> components = {
          butil::StringPiece("HSET"), butil::StringPiece(remote_key), butil::StringPiece(hash_request->field),
          butil::StringPiece(hash_request->value), butil::StringPiece(command_flags)};
      if (!redis_request.AddCommandByComponents(components.data(), components.size())) {
        return Status(Status::RedisExecErr, "failed to build HSET request");
      }
      response_command = "HSET";
      integer_response = true;
      break;
    }
    case DataStructureType::kList: {
      const auto *list_request = dynamic_cast<const ListSetRequest *>(&request);
      if (!list_request) return InvalidRequest("SET", "does not match list request payload");
      auto remote_key = ComposeRemoteKey(list_request->ns, list_request->key);
      auto command_flags = EncodeCommandFlags(list_request->command_flags);
      if (!redis_request.AddCommand("LSET %b %d %b %s", remote_key.data(), remote_key.size(), list_request->index,
                                    list_request->value.data(), list_request->value.size(), command_flags.c_str())) {
        return Status(Status::RedisExecErr, "failed to build LSET request");
      }
      response_command = "LSET";
      break;
    }
  }
  return ExecuteRedisCommandAsync(std::move(redis_request),
                                  MakeAsyncResponseCallback<RequestResult>(
                                      std::move(callback), [response_command = std::move(response_command),
                                                            integer_response](const brpc::RedisResponse &response) {
                                        return integer_response ? ParseIntegerResponse(response, response_command)
                                                                : ParseSetResponse(response, response_command);
                                      }));
}

// 处理批量读取请求，对多个 key 统一拼装一条 MGET。
StatusOr<MultiRequestResult> RequestDispatcher::DispatchRequest(const MultiGetRequest &request) const {
  if (request.keys.empty()) {
    return Status(Status::RedisExecErr, "MGET requires at least one key");
  }

  brpc::RedisRequest redis_request;
  std::vector<std::string> remote_keys;
  std::vector<butil::StringPiece> components;
  remote_keys.reserve(request.keys.size());
  components.reserve(request.keys.size() + 2);
  components.emplace_back("MGET");
  for (const auto &key : request.keys) {
    remote_keys.emplace_back(ComposeRemoteKey(request.ns, key));
    components.emplace_back(remote_keys.back());
  }
  auto command_flags = EncodeCommandFlags(request.command_flags);
  components.emplace_back(command_flags);
  if (!redis_request.AddCommandByComponents(components.data(), components.size())) {
    return Status(Status::RedisExecErr, "failed to build MGET request");
  }

  brpc::RedisResponse redis_response;
  auto s = ExecuteRedisCommand(redis_request, &redis_response);
  if (!s.IsOK()) return s;
  return ParseMGetResponse(redis_response);
}

Status RequestDispatcher::DispatchRequestAsync(const MultiGetRequest &request,
                                               MultiRequestResultCallback callback) const {
  if (request.keys.empty()) return Status(Status::RedisExecErr, "MGET requires at least one key");

  brpc::RedisRequest redis_request;
  std::vector<std::string> remote_keys;
  std::vector<butil::StringPiece> components;
  remote_keys.reserve(request.keys.size());
  components.reserve(request.keys.size() + 2);
  components.emplace_back("MGET");
  for (const auto &key : request.keys) {
    remote_keys.emplace_back(ComposeRemoteKey(request.ns, key));
    components.emplace_back(remote_keys.back());
  }
  auto command_flags = EncodeCommandFlags(request.command_flags);
  components.emplace_back(command_flags);
  if (!redis_request.AddCommandByComponents(components.data(), components.size())) {
    return Status(Status::RedisExecErr, "failed to build MGET request");
  }
  return ExecuteRedisCommandAsync(
      std::move(redis_request),
      MakeAsyncResponseCallback<MultiRequestResult>(
          std::move(callback), [](const brpc::RedisResponse &response) { return ParseMGetResponse(response); }));
}

// 处理批量写入请求，对多个 key/value 统一拼装一条 MSET。
StatusOr<MultiRequestResult> RequestDispatcher::DispatchRequest(const MultiSetRequest &request) const {
  if (request.key_values.empty()) {
    return Status(Status::RedisExecErr, "MSET requires at least one key-value pair");
  }

  brpc::RedisRequest redis_request;
  std::vector<std::string> remote_keys;
  std::vector<butil::StringPiece> components;
  remote_keys.reserve(request.key_values.size());
  components.reserve(request.key_values.size() * 2 + 2);
  components.emplace_back("MSET");
  for (const auto &key_value : request.key_values) {
    remote_keys.emplace_back(ComposeRemoteKey(request.ns, key_value.first));
    components.emplace_back(remote_keys.back());
    components.emplace_back(key_value.second);
  }
  auto command_flags = EncodeCommandFlags(request.command_flags);
  components.emplace_back(command_flags);
  if (!redis_request.AddCommandByComponents(components.data(), components.size())) {
    return Status(Status::RedisExecErr, "failed to build MSET request");
  }

  brpc::RedisResponse redis_response;
  auto s = ExecuteRedisCommand(redis_request, &redis_response);
  if (!s.IsOK()) return s;
  return ParseMSetResponse(redis_response);
}

Status RequestDispatcher::DispatchRequestAsync(const MultiSetRequest &request,
                                               MultiRequestResultCallback callback) const {
  if (request.key_values.empty()) {
    return Status(Status::RedisExecErr, "MSET requires at least one key-value pair");
  }

  brpc::RedisRequest redis_request;
  std::vector<std::string> remote_keys;
  std::vector<butil::StringPiece> components;
  remote_keys.reserve(request.key_values.size());
  components.reserve(request.key_values.size() * 2 + 2);
  components.emplace_back("MSET");
  for (const auto &key_value : request.key_values) {
    remote_keys.emplace_back(ComposeRemoteKey(request.ns, key_value.first));
    components.emplace_back(remote_keys.back());
    components.emplace_back(key_value.second);
  }
  auto command_flags = EncodeCommandFlags(request.command_flags);
  components.emplace_back(command_flags);
  if (!redis_request.AddCommandByComponents(components.data(), components.size())) {
    return Status(Status::RedisExecErr, "failed to build MSET request");
  }
  return ExecuteRedisCommandAsync(
      std::move(redis_request),
      MakeAsyncResponseCallback<MultiRequestResult>(
          std::move(callback), [](const brpc::RedisResponse &response) { return ParseMSetResponse(response); }));
}

Status RequestDispatcher::BuildRedisCommandRequest(const std::string &command_name, const std::string &ns,
                                                   const std::vector<std::string> &args,
                                                   const std::vector<size_t> &key_arg_indexes, uint64_t command_flags,
                                                   brpc::RedisRequest *redis_request) const {
  if (redis_request == nullptr) {
    return Status(Status::RedisExecErr, "redis request output is required");
  }

  std::vector<std::string> encoded_args = args;
  for (size_t index : key_arg_indexes) {
    if (index >= encoded_args.size()) {
      return Status(Status::RedisExecErr, "invalid key argument index");
    }
    encoded_args[index] = ComposeRemoteKey(ns, encoded_args[index]);
  }

  auto encoded_flags = EncodeCommandFlags(command_flags);
  std::vector<butil::StringPiece> components;
  components.reserve(encoded_args.size() + 2);
  components.emplace_back(command_name);
  for (const auto &arg : encoded_args) {
    components.emplace_back(arg);
  }
  components.emplace_back(encoded_flags);
  if (!redis_request->AddCommandByComponents(components.data(), components.size())) {
    return Status(Status::RedisExecErr, "failed to build " + command_name + " request");
  }
  return Status::OK();
}

StatusOr<RequestResult> RequestDispatcher::DispatchIntegerCommand(const std::string &command_name,
                                                                  const std::string &ns,
                                                                  const std::vector<std::string> &args,
                                                                  const std::vector<size_t> &key_arg_indexes,
                                                                  uint64_t command_flags) const {
  brpc::RedisRequest redis_request;
  auto build_status = BuildRedisCommandRequest(command_name, ns, args, key_arg_indexes, command_flags, &redis_request);
  if (!build_status.IsOK()) return build_status;

  brpc::RedisResponse redis_response;
  auto s = ExecuteRedisCommand(redis_request, &redis_response);
  if (!s.IsOK()) return s;
  return ParseIntegerResponse(redis_response, command_name);
}

Status RequestDispatcher::DispatchIntegerCommandAsync(const std::string &command_name, const std::string &ns,
                                                      const std::vector<std::string> &args,
                                                      const std::vector<size_t> &key_arg_indexes,
                                                      uint64_t command_flags, RequestResultCallback callback) const {
  brpc::RedisRequest redis_request;
  auto build_status = BuildRedisCommandRequest(command_name, ns, args, key_arg_indexes, command_flags, &redis_request);
  if (!build_status.IsOK()) return build_status;
  return ExecuteRedisCommandAsync(std::move(redis_request),
                                  MakeAsyncResponseCallback<RequestResult>(
                                      std::move(callback), [command_name](const brpc::RedisResponse &response) {
                                        return ParseIntegerResponse(response, command_name);
                                      }));
}

StatusOr<RequestResult> RequestDispatcher::DispatchSingleCommand(const std::string &command_name, const std::string &ns,
                                                                 const std::vector<std::string> &args,
                                                                 const std::vector<size_t> &key_arg_indexes,
                                                                 uint64_t command_flags) const {
  brpc::RedisRequest redis_request;
  auto build_status = BuildRedisCommandRequest(command_name, ns, args, key_arg_indexes, command_flags, &redis_request);
  if (!build_status.IsOK()) return build_status;

  brpc::RedisResponse redis_response;
  auto s = ExecuteRedisCommand(redis_request, &redis_response);
  if (!s.IsOK()) return s;
  return ParseSingleResponse(redis_response, command_name);
}

Status RequestDispatcher::DispatchSingleCommandAsync(const std::string &command_name, const std::string &ns,
                                                     const std::vector<std::string> &args,
                                                     const std::vector<size_t> &key_arg_indexes, uint64_t command_flags,
                                                     RequestResultCallback callback) const {
  brpc::RedisRequest redis_request;
  auto build_status = BuildRedisCommandRequest(command_name, ns, args, key_arg_indexes, command_flags, &redis_request);
  if (!build_status.IsOK()) return build_status;
  return ExecuteRedisCommandAsync(std::move(redis_request),
                                  MakeAsyncResponseCallback<RequestResult>(
                                      std::move(callback), [command_name](const brpc::RedisResponse &response) {
                                        return ParseSingleResponse(response, command_name);
                                      }));
}

StatusOr<RequestResult> RequestDispatcher::DispatchStatusCommand(const std::string &command_name, const std::string &ns,
                                                                 const std::vector<std::string> &args,
                                                                 const std::vector<size_t> &key_arg_indexes,
                                                                 uint64_t command_flags) const {
  brpc::RedisRequest redis_request;
  auto build_status = BuildRedisCommandRequest(command_name, ns, args, key_arg_indexes, command_flags, &redis_request);
  if (!build_status.IsOK()) return build_status;

  brpc::RedisResponse redis_response;
  auto s = ExecuteRedisCommand(redis_request, &redis_response);
  if (!s.IsOK()) return s;
  return ParseSetResponse(redis_response, command_name);
}

Status RequestDispatcher::DispatchStatusCommandAsync(const std::string &command_name, const std::string &ns,
                                                     const std::vector<std::string> &args,
                                                     const std::vector<size_t> &key_arg_indexes, uint64_t command_flags,
                                                     RequestResultCallback callback) const {
  brpc::RedisRequest redis_request;
  auto build_status = BuildRedisCommandRequest(command_name, ns, args, key_arg_indexes, command_flags, &redis_request);
  if (!build_status.IsOK()) return build_status;
  return ExecuteRedisCommandAsync(
      std::move(redis_request),
      [command_name, callback = std::move(callback)](Status status, const brpc::RedisResponse &response) mutable {
        if (!status.IsOK()) {
          callback(StatusOr<RequestResult>(std::move(status)));
          return;
        }
        callback(ParseSetResponse(response, command_name));
      });
}

StatusOr<MultiRequestResult> RequestDispatcher::DispatchArrayCommand(const std::string &command_name,
                                                                     const std::string &ns,
                                                                     const std::vector<std::string> &args,
                                                                     const std::vector<size_t> &key_arg_indexes,
                                                                     uint64_t command_flags) const {
  brpc::RedisRequest redis_request;
  auto build_status = BuildRedisCommandRequest(command_name, ns, args, key_arg_indexes, command_flags, &redis_request);
  if (!build_status.IsOK()) return build_status;

  brpc::RedisResponse redis_response;
  auto s = ExecuteRedisCommand(redis_request, &redis_response);
  if (!s.IsOK()) return s;
  return ParseArrayResponse(redis_response, command_name);
}

Status RequestDispatcher::DispatchArrayCommandAsync(const std::string &command_name, const std::string &ns,
                                                    const std::vector<std::string> &args,
                                                    const std::vector<size_t> &key_arg_indexes, uint64_t command_flags,
                                                    MultiRequestResultCallback callback) const {
  brpc::RedisRequest redis_request;
  auto build_status = BuildRedisCommandRequest(command_name, ns, args, key_arg_indexes, command_flags, &redis_request);
  if (!build_status.IsOK()) return build_status;
  return ExecuteRedisCommandAsync(
      std::move(redis_request),
      [command_name, callback = std::move(callback)](Status status, const brpc::RedisResponse &response) mutable {
        if (!status.IsOK()) {
          callback(StatusOr<MultiRequestResult>(std::move(status)));
          return;
        }
        callback(ParseArrayResponse(response, command_name));
      });
}

// 统一封装远端 key 编码逻辑，避免上层命令感知 namespace/slot 细节。
std::string RequestDispatcher::ComposeRemoteKey(const std::string &ns, const std::string &key) const {
  std::string ns_key;
  ComposeNamespaceKey(ns, key, &ns_key, slot_id_encoded_);
  return ns_key;
}

}  // namespace Dispatcher
