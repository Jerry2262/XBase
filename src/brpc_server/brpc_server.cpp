// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// A brpc based redis-server using CoreCommandExecutor for unified proxy->datanode requests.

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <brpc/controller.h>
#include <brpc/redis.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <glog/logging.h>

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/core_task.h"
#include "common/status.h"
#include "config/config.h"
#include "core_command_executor/core_command_executor.h"
#include "core_command_executor/core_command_result.h"
#include "server/core_thread_pool.h"
#include "storage/storage.h"

namespace {

Engine::Storage *g_storage = nullptr;
Config *g_config = nullptr;
kvrocks::CoreThreadPool *g_core_thread_pool = nullptr;

struct TaskResult {
  bool ready = false;
  Status status = {Status::NotOK, "core task did not finish"};
  kvrocks_datanode::CoreCommandResult result;
  bthread_mutex_t mu;
  bthread_cond_t cond;

  TaskResult() {
    bthread_mutex_init(&mu, nullptr);
    bthread_cond_init(&cond, nullptr);
  }

  ~TaskResult() {
    bthread_mutex_destroy(&mu);
    bthread_cond_destroy(&cond);
  }

  void SetResult(kvrocks_datanode::CoreCommandResult result_value) {
    bthread_mutex_lock(&mu);
    result = std::move(result_value);
    bthread_mutex_unlock(&mu);
  }

  void Complete(Status status_value) {
    bthread_mutex_lock(&mu);
    status = std::move(status_value);
    ready = true;
    bthread_cond_broadcast(&cond);
    bthread_mutex_unlock(&mu);
  }

  void Wait() {
    bthread_mutex_lock(&mu);
    while (!ready) {
      bthread_cond_wait(&cond, &mu);
    }
    bthread_mutex_unlock(&mu);
  }
};

void EncodeCoreResultToRedisReply(const kvrocks_datanode::CoreCommandResult &result, brpc::RedisReply *output) {
  switch (result.reply_kind) {
    case kvrocks_datanode::CoreReplyKind::kNil:
      output->SetNullString();
      return;
    case kvrocks_datanode::CoreReplyKind::kString:
      output->SetString(result.value);
      return;
    case kvrocks_datanode::CoreReplyKind::kInteger:
      output->SetInteger(result.integer);
      return;
    case kvrocks_datanode::CoreReplyKind::kStatus:
      output->SetStatus(result.status);
      return;
    case kvrocks_datanode::CoreReplyKind::kArray:
      output->SetArray(static_cast<int>(result.values.size()));
      for (size_t i = 0; i < result.values.size(); ++i) {
        if (i < result.founds.size() && !result.founds[i]) {
          (*output)[i].SetNullString();
        } else {
          (*output)[i].SetString(result.values[i]);
        }
      }
      return;
  }
}

bool BuildRedisRequestFromArgs(const std::vector<butil::StringPiece> &args, brpc::RedisRequest *request) {
  if (request == nullptr || args.empty()) {
    return false;
  }
  return request->AddCommandByComponents(args.data(), args.size());
}

class CoreRedisCommandHandler : public brpc::RedisCommandHandler {
 public:
  CoreRedisCommandHandler(Engine::Storage *storage, Config *config, kvrocks::CoreThreadPool *core_thread_pool)
      : storage_(storage), config_(config), core_thread_pool_(core_thread_pool) {}

  brpc::RedisCommandHandlerResult Run(brpc::RedisConnContext *ctx, const std::vector<butil::StringPiece> &args,
                                      brpc::RedisReply *output, bool /*flush_batched*/) override {
    if (storage_ == nullptr || config_ == nullptr) {
      output->FormatError("brpc redis server is not initialized");
      return brpc::REDIS_CMD_HANDLED;
    }
    if (args.empty()) {
      output->FormatError("empty command");
      return brpc::REDIS_CMD_HANDLED;
    }

    brpc::RedisRequest request;
    if (!BuildRedisRequestFromArgs(args, &request)) {
      output->FormatError("failed to build redis request");
      return brpc::REDIS_CMD_HANDLED;
    }

    auto executor = std::make_shared<kvrocks_datanode::CoreCommandExecutor>(storage_, *config_);
    auto parsed_flags = executor->ParseFlags(request);
    if (!parsed_flags.IsOK()) {
      output->FormatError("%s", parsed_flags.Msg().c_str());
      return brpc::REDIS_CMD_HANDLED;
    }

    if (core_thread_pool_ == nullptr || !core_thread_pool_->IsInitialized()) {
      auto result = executor->Execute();
      if (!result.IsOK()) {
        output->FormatError("%s", result.Msg().c_str());
        return brpc::REDIS_CMD_HANDLED;
      }
      EncodeCoreResultToRedisReply(*result, output);
      return brpc::REDIS_CMD_HANDLED;
    }

    auto task_result = std::make_shared<TaskResult>();
    auto task = std::make_unique<kvrocks::CoreTask>();
    task->SetName("brpc-" + std::string(args[0].data(), args[0].size()));
    task->SetType(parsed_flags->task_type);
    task->SetExecutor([executor, task_result](kvrocks::CoreTask &, std::string *) -> Status {
      auto result = executor->Execute();
      if (!result.IsOK()) {
        return result.ToStatus();
      }
      task_result->SetResult(std::move(result.GetValue()));
      return Status::OK();
    });
    task->SetDoneCallback([task_result](const kvrocks::CoreTask &, const Status &status, const std::string &) {
      task_result->Complete(status);
    });

    auto submit_status = core_thread_pool_->Submit(std::move(task));
    if (!submit_status.IsOK()) {
      output->FormatError("%s", submit_status.Msg().c_str());
      return brpc::REDIS_CMD_HANDLED;
    }

    task_result->Wait();
    if (!task_result->status.IsOK()) {
      output->FormatError("%s", task_result->status.Msg().c_str());
      return brpc::REDIS_CMD_HANDLED;
    }

    EncodeCoreResultToRedisReply(task_result->result, output);
    return brpc::REDIS_CMD_HANDLED;
  }

 private:
  Engine::Storage *storage_ = nullptr;
  Config *config_ = nullptr;
  kvrocks::CoreThreadPool *core_thread_pool_ = nullptr;
};

std::mutex g_brpc_mu;
std::unique_ptr<brpc::Server> g_brpc_server;
std::unique_ptr<brpc::RedisService> g_redis_service;
std::unique_ptr<CoreRedisCommandHandler> g_core_handler;

}  // namespace

Status brpc_server_init(Engine::Storage *storage, Config *config, kvrocks::CoreThreadPool *core_thread_pool) {
  std::lock_guard<std::mutex> guard(g_brpc_mu);

  g_storage = storage;
  g_config = config;
  g_core_thread_pool = core_thread_pool;
  g_redis_service = std::make_unique<brpc::RedisService>();
  g_core_handler = std::make_unique<CoreRedisCommandHandler>(storage, config, core_thread_pool);

  const char *commands[] = {"del",        "get",          "set",     "mget",   "mset",         "hget",
                            "hmget",      "hset",         "hmset",   "hgetall","lindex",       "lset",
                            "zadd",       "zrangebyscore","zrem"};
  for (const auto *command : commands) {
    g_redis_service->AddCommandHandler(command, g_core_handler.get());
  }
  return Status::OK();
}

Status brpc_server_start() {
  std::lock_guard<std::mutex> guard(g_brpc_mu);
  if (!g_redis_service || !g_config) {
    return {Status::NotOK, "brpc server is not initialized"};
  }

  g_brpc_server = std::make_unique<brpc::Server>();

  brpc::ServerOptions options;
  options.redis_service = g_redis_service.get();
  if (g_config->datanode_brpc_num_threads > 0) {
    options.num_threads = g_config->datanode_brpc_num_threads;
  }

  const std::string bind = g_config->binds.empty() ? "0.0.0.0" : g_config->binds.front();
  const int port = g_config->GetDatanodeListenPort();
  butil::EndPoint endpoint;
  if (butil::str2endpoint(bind.c_str(), port, &endpoint) != 0) {
    LOG(ERROR) << "Invalid brpc redis server bind address " << bind << ":" << port;
    return {Status::NotOK, "invalid brpc redis server bind address"};
  }
  if (g_brpc_server->Start(endpoint, &options) != 0) {
    LOG(ERROR) << "Failed to start brpc redis server on " << endpoint;
    return {Status::NotOK, "failed to start brpc redis server"};
  }

  LOG(INFO) << "Brpc server started on " << endpoint;
  return Status::OK();
}

void brpc_server_stop() {
  std::lock_guard<std::mutex> guard(g_brpc_mu);
  if (!g_brpc_server) return;

  g_brpc_server->Stop(0);
  g_brpc_server->Join();
  g_brpc_server.reset();
  g_core_handler.reset();
  g_redis_service.reset();
  g_core_thread_pool = nullptr;
  g_config = nullptr;
  g_storage = nullptr;
}
