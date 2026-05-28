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
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/core_task.h"
#include "status.h"

namespace kvrocks {

class CoreWorkerThread;
class CoreWorker;

class CoreThreadPool {
 public:
  struct StatsSnapshot {
    uint64_t total_submitted = 0;
    uint64_t total_completed = 0;
    uint64_t total_failed = 0;
    uint64_t total_rejected = 0;
    uint64_t total_dropped = 0;
    uint64_t total_wait_us = 0;
    uint64_t total_exec_us = 0;
    uint64_t in_flight = 0;
    uint64_t queue_peak = 0;
  };

  explicit CoreThreadPool(std::string worker_name_prefix = "core-worker");
  ~CoreThreadPool();

  CoreThreadPool(const CoreThreadPool &) = delete;
  CoreThreadPool &operator=(const CoreThreadPool &) = delete;

  Status Initialize(size_t num_workers, size_t max_queue_size = 10240);
  void Shutdown(bool drain = true);
  void Join();

  Status Submit(std::unique_ptr<CoreTask> task);
  Status SubmitBatch(std::vector<std::unique_ptr<CoreTask>> tasks);

  size_t QueueSize() const;
  size_t GetWorkerCount() const { return workers_.size(); }
  size_t GetIdleWorkerCount() const { return idle_workers_.load(std::memory_order_relaxed); }
  size_t GetMaxQueueSize() const { return max_queue_size_; }
  bool IsInitialized() const { return initialized_.load(std::memory_order_acquire); }
  bool IsShutdown() const { return shutdown_.load(std::memory_order_acquire); }
  bool IsAccepting() const { return accepting_.load(std::memory_order_acquire); }

  StatsSnapshot GetStats() const;

 private:
  friend class CoreWorkerThread;
  friend class CoreWorker;

  struct QueueEntry {
    uint64_t sequence = 0;
    int priority = 0;
    std::unique_ptr<CoreTask> task;
  };

  struct QueueEntryLess {
    bool operator()(const QueueEntry &lhs, const QueueEntry &rhs) const;
  };

  bool WaitAndPopTask(std::unique_ptr<CoreTask> *task);
  void OnTaskStarted(const CoreTask &task);
  void OnTaskFinished(const CoreTask &task, const Status &status);
  void CompleteDroppedTasks(std::vector<std::unique_ptr<CoreTask>> tasks, const Status &status);
  std::shared_mutex &GetExecutionMutex() { return execution_mutex_; }

  mutable std::mutex mutex_;
  std::condition_variable queue_cond_;
  std::vector<QueueEntry> task_queue_;
  std::vector<std::unique_ptr<CoreWorkerThread>> workers_;
  mutable std::shared_mutex execution_mutex_;

  std::string worker_name_prefix_;
  size_t max_queue_size_ = 10240;
  std::atomic<bool> initialized_{false};
  std::atomic<bool> accepting_{false};
  std::atomic<bool> shutdown_{false};
  std::atomic<bool> drain_on_shutdown_{true};
  std::atomic<size_t> idle_workers_{0};
  std::atomic<uint64_t> next_task_id_{1};
  std::atomic<uint64_t> next_sequence_{0};

  std::atomic<uint64_t> stats_total_submitted_{0};
  std::atomic<uint64_t> stats_total_completed_{0};
  std::atomic<uint64_t> stats_total_failed_{0};
  std::atomic<uint64_t> stats_total_rejected_{0};
  std::atomic<uint64_t> stats_total_dropped_{0};
  std::atomic<uint64_t> stats_total_wait_us_{0};
  std::atomic<uint64_t> stats_total_exec_us_{0};
  std::atomic<uint64_t> stats_in_flight_{0};
  std::atomic<uint64_t> stats_queue_peak_{0};
};

}  // namespace kvrocks
