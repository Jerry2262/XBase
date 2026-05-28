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

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "status.h"

namespace kvrocks {

enum class CoreTaskType {
  kReadOnly = 0,
  kWrite = 1,
  kExclusive = 2,
};

class CoreTask {
 public:
  static constexpr const char *kDefaultTaskNamespace = "__namespace";
  static constexpr const char *kDefaultTaskName = "anonymous-core-task";

  using TaskID = uint64_t;
  using Executor = std::function<Status(CoreTask &, std::string *)>;
  using DoneCallback = std::function<void(const CoreTask &, const Status &, const std::string &)>;

  CoreTask();
  explicit CoreTask(std::vector<std::string> args);
  ~CoreTask() = default;

  CoreTask(const CoreTask &) = delete;
  CoreTask &operator=(const CoreTask &) = delete;
  CoreTask(CoreTask &&) noexcept = default;
  CoreTask &operator=(CoreTask &&) noexcept = default;

  void SetArgs(std::vector<std::string> args);
  const std::vector<std::string> &GetArgs() const { return args_; }
  std::vector<std::string> &MutableArgs() { return args_; }

  void SetTaskID(TaskID id) { task_id_.store(id, std::memory_order_relaxed); }
  TaskID GetTaskID() const { return task_id_.load(std::memory_order_relaxed); }

  void SetConnectionID(uint64_t conn_id) { conn_id_ = conn_id; }
  uint64_t GetConnectionID() const { return conn_id_; }

  void SetNamespace(std::string ns);
  const std::string &GetNamespace() const { return namespace_; }

  void SetName(std::string name);
  const std::string &GetName() const { return name_; }

  void SetExecutor(Executor executor) { executor_ = std::move(executor); }
  bool HasExecutor() const { return static_cast<bool>(executor_); }
  Status Execute(std::string *reply);

  void SetDoneCallback(DoneCallback cb) { done_callback_ = std::move(cb); }
  void ExecuteCallback(const Status &s, const std::string &reply) const;

  void SetType(CoreTaskType type) { type_ = type; }
  CoreTaskType GetType() const { return type_; }
  bool IsReadOnly() const { return type_ == CoreTaskType::kReadOnly; }
  bool IsWrite() const { return type_ == CoreTaskType::kWrite; }
  bool IsExclusive() const { return type_ == CoreTaskType::kExclusive; }

  void SetSubmitTime(std::chrono::steady_clock::time_point t) { submit_time_ = t; }
  std::chrono::steady_clock::time_point GetSubmitTime() const { return submit_time_; }
  void SetStartTime(std::chrono::steady_clock::time_point t) { start_time_ = t; }
  std::chrono::steady_clock::time_point GetStartTime() const { return start_time_; }
  void SetFinishTime(std::chrono::steady_clock::time_point t) { finish_time_ = t; }
  std::chrono::steady_clock::time_point GetFinishTime() const { return finish_time_; }
  uint64_t GetWaitingTimeUs() const;
  uint64_t GetExecutionTimeUs() const;
  uint64_t GetTurnaroundTimeUs() const;

  void SetPriority(int priority) { priority_ = priority; }
  int GetPriority() const { return priority_; }
  void NormalizeMetadata();

 private:
  std::atomic<TaskID> task_id_{0};
  uint64_t conn_id_ = 0;
  std::string namespace_;
  std::string name_;
  std::vector<std::string> args_;
  Executor executor_;
  DoneCallback done_callback_;
  std::chrono::steady_clock::time_point submit_time_;
  std::chrono::steady_clock::time_point start_time_{};
  std::chrono::steady_clock::time_point finish_time_{};
  CoreTaskType type_ = CoreTaskType::kReadOnly;
  int priority_ = 0;
};

}  // namespace kvrocks
