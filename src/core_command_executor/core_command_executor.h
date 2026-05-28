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

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <brpc/redis.h>

#include "common/core_task.h"
#include "common/status.h"
#include "config/config.h"
#include "core_command_executor/core_command_result.h"

namespace Engine {
class Storage;
}

namespace kvrocks_datanode {

// 核心命令执行器当前支持的命令种类。
enum class CoreCommandKind {
  kUnknown = 0,
  kDel,
  kGet,
  kSet,
  kMGet,
  kMSet,
  kHGet,
  kHMGet,
  kHSet,
  kHMSet,
  kHGetAll,
  kLIndex,
  kLSet,
  kZAdd,
  kZRangeByScore,
  kZRem,
};

// ParseFlags 的返回结果。
// 上层线程池会先调用它决定任务该按只读、写入还是独占方式调度。
struct ParsedCommandFlags {
  uint64_t flags = 0;
  kvrocks::CoreTaskType task_type = kvrocks::CoreTaskType::kReadOnly;
};

// 在 kvrocks 服务端直接执行核心命令的入口。
// 它直接解析 brpc::RedisRequest，解码远端 key 后落到本地存储引擎对象上执行。
class CoreCommandExecutor {
 public:
  CoreCommandExecutor(Engine::Storage *storage, const Config &config);

  // 解析请求尾部 flags，并把结果缓存下来供 Execute 复用。
  // 如果解析失败，会先清空旧缓存，保证 Execute 不会误执行上一条命令。
  StatusOr<ParsedCommandFlags> ParseFlags(const brpc::RedisRequest &request) const;
  // 执行最近一次 ParseFlags/ParseRequest 解析出的命令。
  // 默认调用链里，core 线程池会先调用 ParseFlags(request)，它会顺手把解析出的
  // command_request 缓存下来，因此这里执行阶段不需要再次传入 request。
  // 这里只有在最近一次解析成功且结果尚未被消费时才会执行。
  StatusOr<CoreCommandResult> Execute() const;

 private:
  // 执行器内部缓存的标准化命令表示。
  // 这里保留原始参数数组、规范化命令类型和尾部 flags，方便 ParseFlags 与 Execute 解耦。
  class CoreCommandRequest {
   public:
    CoreCommandRequest(std::vector<std::string> args, CoreCommandKind command_kind, uint64_t command_flags)
        : command_args(std::move(args)), kind(command_kind), flags(command_flags) {}

    std::vector<std::string> command_args;      // 原始命令参数，最后一个元素固定是 flags。
    CoreCommandKind kind = CoreCommandKind::kUnknown;  // 规范化后的命令类型。
    uint64_t flags = 0;                         // 从尾参解析出的调度 flags。
  };

  // 从远端 key 中拆出 namespace 和用户真实 key，保持与 proxy 侧 ComposeRemoteKey 对称。
  struct DecodedRemoteKey {
    std::string namespace_name;
    std::string user_key;
  };

  // 把 RedisRequest 解析成执行器内部命令对象。
  StatusOr<CoreCommandRequest> ParseRequest(const brpc::RedisRequest &request) const;
  // 清空最近一次缓存的解析结果。解析失败时也会调用，保证缓存 fail-closed。
  void ClearParsedRequest() const;
  // 保存最近一次解析结果，供后续 Execute 直接消费。
  // 缓存是单槽覆盖模型，新的成功解析结果会替换旧结果。
  void SaveParsedRequest(CoreCommandRequest request) const;
  // 取走并清空最近一次缓存的解析结果，避免同一条命令被重复执行。
  StatusOr<CoreCommandRequest> TakeParsedRequest() const;
  // 把 proxy 侧传来的 remote key 还原成 namespace + user key。
  Status DecodeRemoteKey(const std::string &remote_key, DecodedRemoteKey *decoded_key) const;

  // 按数据结构和读写属性拆开的具体执行函数，command_args 的最后一个参数固定视为 flags。
  StatusOr<CoreCommandResult> ExecuteStringRead(CoreCommandKind kind,
                                                const std::vector<std::string> &command_args) const;
  StatusOr<CoreCommandResult> ExecuteStringWrite(CoreCommandKind kind,
                                                 const std::vector<std::string> &command_args) const;
  StatusOr<CoreCommandResult> ExecuteKeyWrite(CoreCommandKind kind, const std::vector<std::string> &command_args) const;
  StatusOr<CoreCommandResult> ExecuteHashRead(CoreCommandKind kind, const std::vector<std::string> &command_args) const;
  StatusOr<CoreCommandResult> ExecuteHashWrite(CoreCommandKind kind,
                                               const std::vector<std::string> &command_args) const;
  StatusOr<CoreCommandResult> ExecuteListRead(CoreCommandKind kind, const std::vector<std::string> &command_args) const;
  StatusOr<CoreCommandResult> ExecuteListWrite(CoreCommandKind kind,
                                               const std::vector<std::string> &command_args) const;
  StatusOr<CoreCommandResult> ExecuteZSetRead(CoreCommandKind kind, const std::vector<std::string> &command_args) const;
  StatusOr<CoreCommandResult> ExecuteZSetWrite(CoreCommandKind kind,
                                               const std::vector<std::string> &command_args) const;

  Engine::Storage *storage_ = nullptr;  // 本地存储引擎句柄，所有核心命令最终都直接访问它。
  bool slot_id_encoded_ = false;        // 远端 key 是否带 slot 编码，需与 key 编解码规则保持一致。
  mutable std::mutex parsed_request_mutex_;                // 保护单槽缓存，协调 ParseFlags 与 Execute 的交接。
  mutable std::unique_ptr<CoreCommandRequest> parsed_request_;  // 最近一次成功解析且尚未消费的命令。
};

}  // namespace kvrocks_datanode
