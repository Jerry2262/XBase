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
#include <cstddef>
#include <memory>
#include <string>
#include <thread>

#include "common/core_task.h"
#include "status.h"

namespace kvrocks {

class CoreThreadPool;

class CoreWorker {
 public:
  CoreWorker(CoreThreadPool *pool, size_t worker_id, std::string thread_name);
  ~CoreWorker() = default;

  CoreWorker(const CoreWorker &) = delete;
  CoreWorker &operator=(const CoreWorker &) = delete;

  void Run(std::thread::id tid);

  size_t GetID() const { return worker_id_; }
  const std::string &GetThreadName() const { return thread_name_; }
  bool IsRunning() const { return running_.load(std::memory_order_relaxed); }
  uint64_t GetProcessedTasks() const { return tasks_processed_.load(std::memory_order_relaxed); }
  uint64_t GetFailedTasks() const { return tasks_failed_.load(std::memory_order_relaxed); }

 private:
  void ProcessTask(std::unique_ptr<CoreTask> task);
  void HandleTaskResult(const CoreTask &task, const Status &status, const std::string &reply) const;

  CoreThreadPool *pool_ = nullptr;
  size_t worker_id_ = 0;
  std::string thread_name_;
  std::thread::id tid_;
  std::atomic<bool> running_{false};
  std::atomic<uint64_t> tasks_processed_{0};
  std::atomic<uint64_t> tasks_failed_{0};
};

class CoreWorkerThread {
 public:
  explicit CoreWorkerThread(std::unique_ptr<CoreWorker> worker) : worker_(std::move(worker)) {}
  ~CoreWorkerThread() = default;

  CoreWorkerThread(const CoreWorkerThread &) = delete;
  CoreWorkerThread &operator=(const CoreWorkerThread &) = delete;

  Status Start();
  void Join();

 private:
  std::thread thread_;
  std::unique_ptr<CoreWorker> worker_;
};

}  // namespace kvrocks
