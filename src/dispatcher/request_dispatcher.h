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

#pragma once

#include <brpc/redis.h>

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "brpc_client/brpc_client.h"
#include "common/status.h"

struct Config;

namespace Dispatcher {

// 远端请求对应的数据结构类型。dispatcher 会据此选择具体的 Redis 命令。
enum class DataStructureType {
  kString,
  kHash,
  kList,
};

// 请求的大类。目前只区分读(Get)和写(Set)两类。
enum class RequestKind {
  kGet,
  kSet,
};

// 所有远端请求的公共基类，封装了命令分发所需的最小公共信息。
class RequestCommand {
 public:
  virtual ~RequestCommand() = default;

  RequestKind kind;             // 请求类型，决定走读取路径还是写入路径。
  DataStructureType structure;  // 目标数据结构类型，决定拼装哪种 Redis 命令。
  std::string ns;               // 逻辑命名空间，会和 key 一起编码成远端真实 key。
  std::string key;              // 业务 key，尚未附加 namespace/slot 信息。
  uint64_t command_flags = 0;   // Commander 原始 flags，作为内部参数透传给 datanode。

 protected:
  RequestCommand(RequestKind kind, DataStructureType structure, std::string ns, std::string key, uint64_t command_flags)
      : kind(kind), structure(structure), ns(std::move(ns)), key(std::move(key)), command_flags(command_flags) {}
};

class GetRequestCommand : public RequestCommand {
 public:
  GetRequestCommand(DataStructureType structure, std::string ns, std::string key, uint64_t command_flags)
      : RequestCommand(RequestKind::kGet, structure, std::move(ns), std::move(key), command_flags) {}
  ~GetRequestCommand() override = default;
};

// 所有写请求的公共基类。
class SetRequestCommand : public RequestCommand {
 public:
  SetRequestCommand(DataStructureType structure, std::string ns, std::string key, uint64_t command_flags)
      : RequestCommand(RequestKind::kSet, structure, std::move(ns), std::move(key), command_flags) {}
  ~SetRequestCommand() override = default;
};

// String GET 请求，只需要 namespace 和 key。
class StringGetRequest : public GetRequestCommand {
 public:
  StringGetRequest(std::string ns, std::string key, uint64_t command_flags)
      : GetRequestCommand(DataStructureType::kString, std::move(ns), std::move(key), command_flags) {}
};

// String SET 请求，额外携带待写入的 value。
class StringSetRequest : public SetRequestCommand {
 public:
  StringSetRequest(std::string ns, std::string key, std::string value, uint64_t command_flags)
      : SetRequestCommand(DataStructureType::kString, std::move(ns), std::move(key), command_flags),
        value(std::move(value)) {}

  std::string value;  // 写入远端 string key 的值。
};

// Hash 读取请求，额外指定 field。
class HashGetRequest : public GetRequestCommand {
 public:
  HashGetRequest(std::string ns, std::string key, std::string field, uint64_t command_flags)
      : GetRequestCommand(DataStructureType::kHash, std::move(ns), std::move(key), command_flags),
        field(std::move(field)) {}

  std::string field;  // hash 中需要访问的 field。
};

// Hash 写入请求，指定 field 和对应 value。
class HashSetRequest : public SetRequestCommand {
 public:
  HashSetRequest(std::string ns, std::string key, std::string field, std::string value, uint64_t command_flags)
      : SetRequestCommand(DataStructureType::kHash, std::move(ns), std::move(key), command_flags),
        field(std::move(field)),
        value(std::move(value)) {}

  std::string field;  // hash 中待写入的 field。
  std::string value;  // field 对应的新值。
};

// List 读取请求，使用 index 访问指定元素。
class ListGetRequest : public GetRequestCommand {
 public:
  ListGetRequest(std::string ns, std::string key, int index, uint64_t command_flags)
      : GetRequestCommand(DataStructureType::kList, std::move(ns), std::move(key), command_flags), index(index) {}

  int index = 0;  // list 下标，对应 LINDEX 的 index 参数。
};

// List 写请求，使用 index 覆盖指定位置元素。
class ListSetRequest : public SetRequestCommand {
 public:
  ListSetRequest(std::string ns, std::string key, int index, std::string value, uint64_t command_flags)
      : SetRequestCommand(DataStructureType::kList, std::move(ns), std::move(key), command_flags),
        index(index),
        value(std::move(value)) {}

  int index = 0;      // list 下标，对应 LSET 的 index 参数。
  std::string value;  // 需要写入该位置的新值。
};

// 单个请求的统一结果对象，屏蔽底层 brpc/RESP 响应细节，供上层命令直接消费。
struct RequestResult {
  bool found = false;   // 读请求是否命中。对 GET/HGET/LINDEX 这类命令最有意义。
  int64_t integer = 0;  // 整数型返回值，例如 HSET 返回新增 field 数量。
  std::string value;    // 字符串型返回值，例如 GET/HGET 返回的 value。
  std::string status;   // 状态型返回值，例如 SET/LSET 返回的 "OK"。
};

// 多 key 读取请求，对应 MGET。
struct MultiGetRequest {
  std::string ns;                 // 所有 key 所属的逻辑命名空间。
  std::vector<std::string> keys;  // 需要批量读取的 key 列表。
  uint64_t command_flags = 0;     // Commander 原始 flags，追加到 MGET 命令末尾透传给 datanode。
};

// 多 key 写入请求，对应 MSET。
struct MultiSetRequest {
  std::string ns;                                               // 所有 key 所属的逻辑命名空间。
  std::vector<std::pair<std::string, std::string>> key_values;  // 待批量写入的 key/value 列表。
  uint64_t command_flags = 0;  // Commander 原始 flags，追加到 MSET 命令末尾透传给 datanode。
};

// 批量请求的统一结果对象，与单请求结果分开，避免 value/found 字段语义混淆。
struct MultiRequestResult {
  std::vector<bool> founds;         // 每个 key 是否命中，和 values 一一对应。
  std::vector<std::string> values;  // 每个 key 的返回值；未命中位置通常是空串。
  std::string status;               // 批量写请求的整体状态，例如 MSET 返回的 "OK"。
};

// 统一的远端请求分发器。
// 负责将结构化请求对象翻译成具体 Redis 命令，通过 brpc 发往后端，再解析为统一结果。
class RequestDispatcher {
 public:
  using RequestResultCallback = std::function<void(StatusOr<RequestResult>)>;
  using MultiRequestResultCallback = std::function<void(StatusOr<MultiRequestResult>)>;

  explicit RequestDispatcher(const Config &config, event_base *event_base = nullptr);

  // brpc 初始化结果。0 表示成功，非 0 表示初始化失败。
  int init_result() const { return init_result_; }
  StatusOr<RequestResult> DispatchRequest(const GetRequestCommand &request) const;
  StatusOr<RequestResult> DispatchRequest(const SetRequestCommand &request) const;
  StatusOr<MultiRequestResult> DispatchRequest(const MultiGetRequest &request) const;
  StatusOr<MultiRequestResult> DispatchRequest(const MultiSetRequest &request) const;
  Status DispatchRequestAsync(const GetRequestCommand &request, RequestResultCallback callback) const;
  Status DispatchRequestAsync(const SetRequestCommand &request, RequestResultCallback callback) const;
  Status DispatchRequestAsync(const MultiGetRequest &request, MultiRequestResultCallback callback) const;
  Status DispatchRequestAsync(const MultiSetRequest &request, MultiRequestResultCallback callback) const;
  StatusOr<RequestResult> DispatchIntegerCommand(const std::string &command_name, const std::string &ns,
                                                 const std::vector<std::string> &args,
                                                 const std::vector<size_t> &key_arg_indexes,
                                                 uint64_t command_flags) const;
  Status DispatchIntegerCommandAsync(const std::string &command_name, const std::string &ns,
                                     const std::vector<std::string> &args, const std::vector<size_t> &key_arg_indexes,
                                     uint64_t command_flags, RequestResultCallback callback) const;
  StatusOr<RequestResult> DispatchSingleCommand(const std::string &command_name, const std::string &ns,
                                                const std::vector<std::string> &args,
                                                const std::vector<size_t> &key_arg_indexes,
                                                uint64_t command_flags) const;
  Status DispatchSingleCommandAsync(const std::string &command_name, const std::string &ns,
                                    const std::vector<std::string> &args, const std::vector<size_t> &key_arg_indexes,
                                    uint64_t command_flags, RequestResultCallback callback) const;
  StatusOr<RequestResult> DispatchStatusCommand(const std::string &command_name, const std::string &ns,
                                                const std::vector<std::string> &args,
                                                const std::vector<size_t> &key_arg_indexes,
                                                uint64_t command_flags) const;
  Status DispatchStatusCommandAsync(const std::string &command_name, const std::string &ns,
                                    const std::vector<std::string> &args, const std::vector<size_t> &key_arg_indexes,
                                    uint64_t command_flags, RequestResultCallback callback) const;
  StatusOr<MultiRequestResult> DispatchArrayCommand(const std::string &command_name, const std::string &ns,
                                                    const std::vector<std::string> &args,
                                                    const std::vector<size_t> &key_arg_indexes,
                                                    uint64_t command_flags) const;
  Status DispatchArrayCommandAsync(const std::string &command_name, const std::string &ns,
                                   const std::vector<std::string> &args, const std::vector<size_t> &key_arg_indexes,
                                   uint64_t command_flags, MultiRequestResultCallback callback) const;

 private:
  Status BuildRedisCommandRequest(const std::string &command_name, const std::string &ns,
                                  const std::vector<std::string> &args, const std::vector<size_t> &key_arg_indexes,
                                  uint64_t command_flags, brpc::RedisRequest *redis_request) const;
  Status ExecuteRedisCommand(const brpc::RedisRequest &redis_request, brpc::RedisResponse *redis_response) const;
  Status ExecuteRedisCommandAsync(brpc::RedisRequest redis_request, BrpcClient::ResponseCallback callback) const;
  // 将 namespace + key 编码成后端实际使用的 key，必要时带上 slot 信息。
  std::string ComposeRemoteKey(const std::string &ns, const std::string &key) const;

  int init_result_ = -1;          // brpc channel 初始化结果。
  bool slot_id_encoded_ = false;  // 是否在 key 编码中附带 slot id，需与后端存储的编码规则保持一致。
  mutable BrpcClient brpc_client_;
};

}  // namespace Dispatcher
