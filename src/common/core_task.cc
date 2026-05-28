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

#include "core_task.h"

namespace kvrocks {

namespace {

uint64_t DurationUs(std::chrono::steady_clock::time_point from, std::chrono::steady_clock::time_point to) {
  if (from == std::chrono::steady_clock::time_point{}) {
    return 0;
  }
  return std::chrono::duration_cast<std::chrono::microseconds>(to - from).count();
}

}  // namespace

CoreTask::CoreTask()
    : namespace_(kDefaultTaskNamespace), name_(kDefaultTaskName), submit_time_(std::chrono::steady_clock::now()) {}

CoreTask::CoreTask(std::vector<std::string> args)
    : namespace_(kDefaultTaskNamespace), name_(kDefaultTaskName), submit_time_(std::chrono::steady_clock::now()) {
  SetArgs(std::move(args));
}

void CoreTask::SetArgs(std::vector<std::string> args) {
  args_ = std::move(args);
  if (!args_.empty() && (name_.empty() || name_ == kDefaultTaskName)) {
    name_ = args_.front();
  }
}

void CoreTask::SetNamespace(std::string ns) {
  namespace_ = ns.empty() ? kDefaultTaskNamespace : std::move(ns);
}

void CoreTask::SetName(std::string name) {
  if (!name.empty()) {
    name_ = std::move(name);
    return;
  }

  if (!args_.empty()) {
    name_ = args_.front();
  } else {
    name_ = kDefaultTaskName;
  }
}

Status CoreTask::Execute(std::string *reply) {
  if (!executor_) {
    return {Status::NotOK, "core task executor is not set"};
  }

  std::string local_reply;
  if (!reply) {
    reply = &local_reply;
  }
  reply->clear();
  return executor_(*this, reply);
}

void CoreTask::ExecuteCallback(const Status &s, const std::string &reply) const {
  if (done_callback_) {
    done_callback_(*this, s, reply);
  }
}

uint64_t CoreTask::GetWaitingTimeUs() const {
  auto end = start_time_;
  if (end == std::chrono::steady_clock::time_point{}) {
    end = std::chrono::steady_clock::now();
  }
  return DurationUs(submit_time_, end);
}

uint64_t CoreTask::GetExecutionTimeUs() const {
  if (start_time_ == std::chrono::steady_clock::time_point{}) {
    return 0;
  }

  auto end = finish_time_;
  if (end == std::chrono::steady_clock::time_point{}) {
    end = std::chrono::steady_clock::now();
  }
  return DurationUs(start_time_, end);
}

uint64_t CoreTask::GetTurnaroundTimeUs() const {
  auto end = finish_time_;
  if (end == std::chrono::steady_clock::time_point{}) {
    end = std::chrono::steady_clock::now();
  }
  return DurationUs(submit_time_, end);
}

void CoreTask::NormalizeMetadata() {
  if (namespace_.empty()) {
    namespace_ = kDefaultTaskNamespace;
  }
  if (name_.empty()) {
    name_ = args_.empty() ? kDefaultTaskName : args_.front();
  }
}

}  // namespace kvrocks
