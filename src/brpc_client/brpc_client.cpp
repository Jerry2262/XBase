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

DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "127.0.0.1:6379", "IP Address of server");
DEFINE_int32(timeout_ms, 1000, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)"); 

namespace brpc {
const char* logo();
}

// A Channel represents a communication line to a Server. Notice that 
// Channel is thread-safe and can be shared by all threads in your program.
brpc::Channel g_brpc_channel;

// 异步回调函数
static void OnRedisResponse(RedisCallContext* ctx) {
    if (ctx->cntl.Failed()) {
        LOG(ERROR) << "Redis request failed: " << ctx->cntl.ErrorText();
    } else {
        LOG(INFO) << "Redis response: " << ctx->response;
    }
    ctx->done.store(true, std::memory_order_release);
}

// 发送单个异步Redis请求
bool brpc_request_async( const char* command, RedisCallContext* ctx) {
    brpc::RedisRequest request;
    if (!request.AddCommand(command)) {
        LOG(ERROR) << "Fail to add command: " << command;
        ctx->done.store(true, std::memory_order_release);
        return false;
    }
    
    ctx->Reset();
    
    // 使用NewCallback创建回调函数
    google::protobuf::Closure* done = brpc::NewCallback(
        &OnRedisResponse, ctx);
    
    // 发起异步调用
    g_brpc_channel.CallMethod(NULL, &ctx->cntl, &request, &ctx->response, done);
    return true;
}

int brpc_client_init(const char* server, const char* connection_type, int timeout_ms, int max_retry) {
    printf("%s\n", brpc::logo());

    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_REDIS;
    options.connection_type = connection_type;
    options.timeout_ms = timeout_ms;
    options.max_retry = max_retry;
    if (g_brpc_channel.Init(server, &options) != 0) {
        LOG(ERROR) << "Fail to initialize g_brpc_channel";
        return -1;
    }

    return 0;
}

Status brpc_request_sync(const brpc::RedisRequest& request, brpc::RedisResponse* response) {
    brpc::Controller cntl;
    g_brpc_channel.CallMethod(NULL, &cntl, &request, response, NULL);
    if (cntl.Failed()) {
        return Status(Status::RedisExecErr, cntl.ErrorText());
    }
    return Status::OK();
}
