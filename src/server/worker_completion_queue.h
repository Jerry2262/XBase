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
 */

#pragma once

#include <event2/event.h>
#include <event2/util.h>

#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "common/status.h"

struct ProxyCommandCompletion {
  int fd;
  uint64_t connection_id;
  Status status;
  std::string reply;
};

class WorkerCompletionQueue {
 public:
  using Handler = std::function<void(ProxyCommandCompletion)>;

  WorkerCompletionQueue(event_base *base, Handler handler);
  ~WorkerCompletionQueue();
  WorkerCompletionQueue(const WorkerCompletionQueue &) = delete;
  WorkerCompletionQueue &operator=(const WorkerCompletionQueue &) = delete;

  bool Post(ProxyCommandCompletion completion);
  void Stop();

 private:
  static void OnNotify(evutil_socket_t fd, int16_t events, void *ctx);
  void Drain();

  int notify_fd_ = -1;
  event *notify_event_ = nullptr;
  Handler handler_;
  std::mutex mu_;
  std::deque<ProxyCommandCompletion> pending_;
  bool stopped_ = false;
};

class WorkerCompletionQueueHandle {
 public:
  explicit WorkerCompletionQueueHandle(WorkerCompletionQueue *queue) : queue_(queue) {}

  bool Post(ProxyCommandCompletion completion);
  void Stop();

 private:
  std::mutex mu_;
  WorkerCompletionQueue *queue_;
};
