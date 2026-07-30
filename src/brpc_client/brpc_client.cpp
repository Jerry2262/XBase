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
  call->callback(std::move(status), call->response, 0);
}

struct AsyncRedisBatch {
  brpc::RedisRequest request;
  brpc::RedisResponse response;
  brpc::Controller controller;
  std::vector<BrpcClient::ResponseCallback> callbacks;
};

void OnRedisBatchResponse(AsyncRedisBatch *raw_batch) {
  std::unique_ptr<AsyncRedisBatch> batch(raw_batch);
  if (batch->controller.Failed()) {
    for (auto &callback : batch->callbacks) {
      callback(Status(Status::RedisExecErr, batch->controller.ErrorText()), batch->response, 0);
    }
    return;
  }
  if (batch->response.reply_size() != static_cast<int>(batch->callbacks.size())) {
    for (auto &callback : batch->callbacks) {
      callback(Status(Status::RedisExecErr, "unexpected batched response size"), batch->response, 0);
    }
    return;
  }
  for (size_t i = 0; i < batch->callbacks.size(); ++i) {
    batch->callbacks[i](Status::OK(), batch->response, i);
  }
}

}  // namespace

BrpcClient::~BrpcClient() {
  if (flush_event_ != nullptr) event_free(flush_event_);
}

int BrpcClient::Init(const char *server, const char *connection_type, int timeout_ms, int max_retry, int batch_size,
                     event_base *event_base) {
  brpc::ChannelOptions options;
  options.protocol = brpc::PROTOCOL_REDIS;
  options.connection_type = connection_type;
  options.timeout_ms = timeout_ms;
  options.max_retry = max_retry;
  if (channel_.Init(server, &options) != 0) {
    LOG(ERROR) << "Fail to initialize brpc channel";
    return -1;
  }

  batch_size_ = batch_size;
  if (batch_size_ > 1) {
    flush_event_ = event_new(event_base, -1, 0, FlushBatch, this);
    if (flush_event_ == nullptr) {
      LOG(ERROR) << "Fail to initialize storage RPC batch event";
      return -1;
    }
    pending_.reserve(batch_size_);
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
  PendingCall call{std::move(request), std::move(callback)};
  if (flush_event_ == nullptr || call.request.command_size() != 1) {
    SendOne(std::move(call));
    return Status::OK();
  }
  if (pending_.empty()) event_active(flush_event_, EV_TIMEOUT, 0);
  pending_.emplace_back(std::move(call));
  if (pending_.size() == static_cast<size_t>(batch_size_)) FlushPending();
  return Status::OK();
}

void BrpcClient::FlushBatch(evutil_socket_t, short, void *ctx) { static_cast<BrpcClient *>(ctx)->FlushPending(); }

void BrpcClient::FlushPending() {
  if (pending_.empty()) return;
  std::vector<PendingCall> calls;
  calls.swap(pending_);
  pending_.reserve(batch_size_);
  SendBatch(std::move(calls));
}

void BrpcClient::SendOne(PendingCall pending) {
  auto call = std::make_unique<AsyncRedisCall>(std::move(pending.request), std::move(pending.callback));
  auto *done = brpc::NewCallback(&OnRedisResponse, call.get());
  channel_.CallMethod(nullptr, &call->controller, &call->request, &call->response, done);
  call.release();
}

void BrpcClient::SendBatch(std::vector<PendingCall> calls) {
  if (calls.size() == 1) {
    SendOne(std::move(calls.front()));
    return;
  }

  auto batch = std::make_unique<AsyncRedisBatch>();
  batch->callbacks.reserve(calls.size());
  for (auto &call : calls) {
    batch->request.MergeFrom(call.request);
    batch->callbacks.emplace_back(std::move(call.callback));
  }
  auto *done = brpc::NewCallback(&OnRedisBatchResponse, batch.get());
  channel_.CallMethod(nullptr, &batch->controller, &batch->request, &batch->response, done);
  batch.release();
}
