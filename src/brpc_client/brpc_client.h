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
#include <brpc/channel.h>
#include <brpc/redis.h>

#include <functional>

#include "common/status.h"

class BrpcClient {
 public:
  using ResponseCallback = std::function<void(Status, const brpc::RedisResponse &)>;

  int Init(const char *server, const char *connection_type, int timeout_ms, int max_retry);
  Status RequestSync(const brpc::RedisRequest &request, brpc::RedisResponse *response);
  Status RequestAsync(brpc::RedisRequest request, ResponseCallback callback);

 private:
  brpc::Channel channel_;
};

#endif  // KVROCKS_BRPC_CLIENT_H
