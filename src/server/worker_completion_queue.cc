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

#include "server/worker_completion_queue.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <stdexcept>
#include <utility>

WorkerCompletionQueue::WorkerCompletionQueue(event_base *base, Handler handler) : handler_(std::move(handler)) {
  if (base == nullptr || !handler_) {
    throw std::invalid_argument("worker completion queue requires an event base and handler");
  }

  notify_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (notify_fd_ < 0) {
    throw std::runtime_error("failed to create worker completion eventfd");
  }

  notify_event_ = event_new(base, notify_fd_, EV_READ | EV_PERSIST, OnNotify, this);
  if (notify_event_ == nullptr || event_add(notify_event_, nullptr) != 0) {
    if (notify_event_ != nullptr) event_free(notify_event_);
    close(notify_fd_);
    notify_event_ = nullptr;
    notify_fd_ = -1;
    throw std::runtime_error("failed to register worker completion event");
  }
}

WorkerCompletionQueue::~WorkerCompletionQueue() {
  Stop();
  if (notify_event_ != nullptr) event_free(notify_event_);
  if (notify_fd_ >= 0) close(notify_fd_);
}

bool WorkerCompletionQueue::Post(ProxyCommandCompletion completion) {
  bool should_notify = false;
  {
    std::lock_guard<std::mutex> guard(mu_);
    if (stopped_) return false;
    should_notify = pending_.empty();
    pending_.emplace_back(std::move(completion));
  }

  if (!should_notify) return true;

  const uint64_t value = 1;
  ssize_t bytes = 0;
  do {
    bytes = write(notify_fd_, &value, sizeof(value));
  } while (bytes < 0 && errno == EINTR);
  return bytes == static_cast<ssize_t>(sizeof(value)) || (bytes < 0 && errno == EAGAIN);
}

void WorkerCompletionQueue::Stop() {
  {
    std::lock_guard<std::mutex> guard(mu_);
    if (stopped_) return;
    stopped_ = true;
    pending_.clear();
  }
  if (notify_event_ != nullptr) event_del(notify_event_);
}

void WorkerCompletionQueue::OnNotify(evutil_socket_t, int16_t, void *ctx) {
  static_cast<WorkerCompletionQueue *>(ctx)->Drain();
}

void WorkerCompletionQueue::Drain() {
  uint64_t value = 0;
  for (;;) {
    ssize_t bytes = read(notify_fd_, &value, sizeof(value));
    if (bytes == static_cast<ssize_t>(sizeof(value))) continue;
    if (bytes < 0 && errno == EINTR) continue;
    break;
  }

  std::deque<ProxyCommandCompletion> pending;
  {
    std::lock_guard<std::mutex> guard(mu_);
    if (stopped_) return;
    pending.swap(pending_);
  }
  for (auto &completion : pending) {
    handler_(std::move(completion));
  }
}
