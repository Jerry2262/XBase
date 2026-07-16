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

// A brpc based command-line interface to talk with redis-server

#include "brpc_client.h"

#include <memory>
#include <utility>

DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "127.0.0.1:6379", "IP Address of server");
DEFINE_int32(timeout_ms, 1000, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");

namespace {

struct AsyncRedisCall {
  AsyncRedisCall(brpc::RedisRequest redis_request, BrpcClient::ResponseCallback response_callback)
      : request(std::move(redis_request)), callback(std::move(response_callback)) {}

  brpc::RedisRequest request;
  brpc::RedisResponse response;
  brpc::Controller controller;
  BrpcClient::ResponseCallback callback;
};

void OnRedisResponse(AsyncRedisCall *raw_call) {
  std::unique_ptr<AsyncRedisCall> call(raw_call);
  Status status = Status::OK();
  if (call->controller.Failed()) {
    status = Status(Status::RedisExecErr, call->controller.ErrorText());
  }
  call->callback(std::move(status), call->response);
}

}  // namespace

int BrpcClient::Init(const char *server, const char *connection_type, int timeout_ms, int max_retry) {
  brpc::ChannelOptions options;
  options.protocol = brpc::PROTOCOL_REDIS;
  options.connection_type = connection_type;
  options.timeout_ms = timeout_ms;
  options.max_retry = max_retry;
  if (channel_.Init(server, &options) != 0) {
    LOG(ERROR) << "Fail to initialize brpc channel";
    return -1;
  }

  return 0;
}

Status BrpcClient::RequestSync(const brpc::RedisRequest &request, brpc::RedisResponse *response) {
  brpc::Controller cntl;
  channel_.CallMethod(nullptr, &cntl, &request, response, nullptr);
  if (cntl.Failed()) {
    return Status(Status::RedisExecErr, cntl.ErrorText());
  }
  return Status::OK();
}

Status BrpcClient::RequestAsync(brpc::RedisRequest request, ResponseCallback callback) {
  if (!callback) {
    return Status(Status::RedisExecErr, "redis response callback is required");
  }
  auto call = std::make_unique<AsyncRedisCall>(std::move(request), std::move(callback));
  auto *done = brpc::NewCallback(&OnRedisResponse, call.get());
  channel_.CallMethod(nullptr, &call->controller, &call->request, &call->response, done);
  call.release();
  return Status::OK();
}
