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

#ifndef KVROCKS_BRPC_CLIENT_H
#define KVROCKS_BRPC_CLIENT_H
#include <signal.h>
#include <stdio.h>
#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/channel.h>
#include <brpc/redis.h>

#include "common/status.h"

// 用于异步回调的上下文结构体
struct RedisCallContext {
    brpc::RedisResponse response;
    brpc::Controller cntl;
    std::atomic<bool> done{false};
    
    void Reset() {
        response.Clear();
        cntl.Reset();
        done.store(false, std::memory_order_relaxed);
    }
};

/**
 * Initialize brpc client
 * @return 0 on success, -1 on failure
 */
int brpc_client_init(const char* server, const char* connection_type, int timeout_ms, int max_retry);

/**
 * Send request to redis server via brpc asynchronously
 * @param command Redis command to execute
 * @param ctx Pointer to the RedisCallContext to store the response
 * @return true on success, false on failure
 */
bool brpc_request_async(const char* command, RedisCallContext* ctx);

Status brpc_request_sync(const brpc::RedisRequest& request, brpc::RedisResponse* response);

#endif // KVROCKS_BRPC_CLIENT_H
