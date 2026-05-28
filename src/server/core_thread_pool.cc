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

#include "core_thread_pool.h"

#include <algorithm>
#include <exception>
#include <utility>

#include <glog/logging.h>

#include "core_worker.h"

namespace kvrocks {

CoreThreadPool::CoreThreadPool(std::string worker_name_prefix) : worker_name_prefix_(std::move(worker_name_prefix)) {
  if (worker_name_prefix_.empty()) {
    worker_name_prefix_ = "core-worker";
  }
}

CoreThreadPool::~CoreThreadPool() {
  Shutdown(false);
  Join();
}

bool CoreThreadPool::QueueEntryLess::operator()(const QueueEntry &lhs, const QueueEntry &rhs) const {
  if (lhs.priority != rhs.priority) {
    return lhs.priority < rhs.priority;
  }
  return lhs.sequence > rhs.sequence;
}

Status CoreThreadPool::Initialize(size_t num_workers, size_t max_queue_size) {
  if (num_workers == 0) {
    return {Status::NotOK, "core thread pool requires at least one worker"};
  }
  if (max_queue_size == 0) {
    return {Status::NotOK, "core thread pool queue size must be positive"};
  }
  if (initialized_.exchange(true, std::memory_order_acq_rel)) {
    return {Status::NotOK, "core thread pool already initialized"};
  }

  max_queue_size_ = max_queue_size;
  accepting_.store(true, std::memory_order_release);
  shutdown_.store(false, std::memory_order_release);
  drain_on_shutdown_.store(true, std::memory_order_release);
  idle_workers_.store(0, std::memory_order_relaxed);
  task_queue_.clear();
  workers_.clear();

  workers_.reserve(num_workers);
  for (size_t i = 0; i < num_workers; ++i) {
    auto worker = std::make_unique<CoreWorker>(this, i, worker_name_prefix_);
    workers_.emplace_back(std::make_unique<CoreWorkerThread>(std::move(worker)));
  }

  for (const auto &worker : workers_) {
    auto s = worker->Start();
    if (!s.IsOK()) {
      LOG(ERROR) << "[core-pool] failed to initialize worker thread: " << s.Msg();
      Shutdown(false);
      Join();
      return s;
    }
  }

  LOG(INFO) << "[core-pool] Initialized with " << num_workers << " workers, max_queue_size=" << max_queue_size_;
  return Status::OK();
}

void CoreThreadPool::Shutdown(bool drain) {
  if (!initialized_.load(std::memory_order_acquire)) {
    return;
  }

  accepting_.store(false, std::memory_order_release);
  drain_on_shutdown_.store(drain, std::memory_order_release);
  shutdown_.store(true, std::memory_order_release);

  std::vector<std::unique_ptr<CoreTask>> dropped_tasks;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!drain) {
      dropped_tasks.reserve(task_queue_.size());
      while (!task_queue_.empty()) {
        std::pop_heap(task_queue_.begin(), task_queue_.end(), QueueEntryLess{});
        dropped_tasks.emplace_back(std::move(task_queue_.back().task));
        task_queue_.pop_back();
      }
    }
  }

  if (!dropped_tasks.empty()) {
    LOG(WARNING) << "[core-pool] dropping " << dropped_tasks.size()
                 << " queued tasks because thread pool is shutting down without draining";
    CompleteDroppedTasks(std::move(dropped_tasks), {Status::NotOK, "core thread pool shut down"});
  }

  queue_cond_.notify_all();
}

void CoreThreadPool::Join() {
  for (const auto &worker : workers_) {
    worker->Join();
  }

  if (!workers_.empty()) {
    auto stats = GetStats();
    LOG(INFO) << "[core-pool] All workers joined, submitted=" << stats.total_submitted
              << ", completed=" << stats.total_completed << ", failed=" << stats.total_failed
              << ", rejected=" << stats.total_rejected << ", dropped=" << stats.total_dropped
              << ", queue_peak=" << stats.queue_peak;
  }

  workers_.clear();
  initialized_.store(false, std::memory_order_release);
}

Status CoreThreadPool::Submit(std::unique_ptr<CoreTask> task) {
  if (!task) {
    stats_total_rejected_.fetch_add(1, std::memory_order_relaxed);
    return {Status::NotOK, "core task is null"};
  }
  task->NormalizeMetadata();
  if (!task->HasExecutor()) {
    stats_total_rejected_.fetch_add(1, std::memory_order_relaxed);
    return {Status::NotOK, "core task executor is not set"};
  }
  if (!accepting_.load(std::memory_order_acquire) || shutdown_.load(std::memory_order_acquire)) {
    stats_total_rejected_.fetch_add(1, std::memory_order_relaxed);
    return {Status::NotOK, "core thread pool is shutting down"};
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!accepting_.load(std::memory_order_relaxed) || shutdown_.load(std::memory_order_relaxed)) {
      stats_total_rejected_.fetch_add(1, std::memory_order_relaxed);
      return {Status::NotOK, "core thread pool is shutting down"};
    }
    if (task_queue_.size() >= max_queue_size_) {
      stats_total_rejected_.fetch_add(1, std::memory_order_relaxed);
      return {Status::NotOK, "core thread pool queue is full"};
    }

    task->SetTaskID(next_task_id_.fetch_add(1, std::memory_order_relaxed));
    task->SetSubmitTime(std::chrono::steady_clock::now());
    task_queue_.push_back(
        {next_sequence_.fetch_add(1, std::memory_order_relaxed), task->GetPriority(), std::move(task)});
    std::push_heap(task_queue_.begin(), task_queue_.end(), QueueEntryLess{});

    stats_total_submitted_.fetch_add(1, std::memory_order_relaxed);
    auto queue_size = static_cast<uint64_t>(task_queue_.size());
    auto peak = stats_queue_peak_.load(std::memory_order_relaxed);
    while (queue_size > peak &&
           !stats_queue_peak_.compare_exchange_weak(peak, queue_size, std::memory_order_relaxed)) {
    }
  }

  queue_cond_.notify_one();
  return Status::OK();
}

Status CoreThreadPool::SubmitBatch(std::vector<std::unique_ptr<CoreTask>> tasks) {
  if (tasks.empty()) {
    return Status::OK();
  }
  if (!accepting_.load(std::memory_order_acquire) || shutdown_.load(std::memory_order_acquire)) {
    stats_total_rejected_.fetch_add(tasks.size(), std::memory_order_relaxed);
    return {Status::NotOK, "core thread pool is shutting down"};
  }

  for (const auto &task : tasks) {
    if (!task) {
      stats_total_rejected_.fetch_add(tasks.size(), std::memory_order_relaxed);
      return {Status::NotOK, "core task batch contains null task"};
    }
    if (!task->HasExecutor()) {
      stats_total_rejected_.fetch_add(tasks.size(), std::memory_order_relaxed);
      return {Status::NotOK, "core task batch contains task without executor"};
    }
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!accepting_.load(std::memory_order_relaxed) || shutdown_.load(std::memory_order_relaxed)) {
      stats_total_rejected_.fetch_add(tasks.size(), std::memory_order_relaxed);
      return {Status::NotOK, "core thread pool is shutting down"};
    }
    if (task_queue_.size() + tasks.size() > max_queue_size_) {
      stats_total_rejected_.fetch_add(tasks.size(), std::memory_order_relaxed);
      return {Status::NotOK, "core thread pool queue would overflow"};
    }

    auto now = std::chrono::steady_clock::now();
    for (auto &task : tasks) {
      task->NormalizeMetadata();
      task->SetTaskID(next_task_id_.fetch_add(1, std::memory_order_relaxed));
      task->SetSubmitTime(now);
      task_queue_.push_back(
          {next_sequence_.fetch_add(1, std::memory_order_relaxed), task->GetPriority(), std::move(task)});
      std::push_heap(task_queue_.begin(), task_queue_.end(), QueueEntryLess{});
    }

    stats_total_submitted_.fetch_add(tasks.size(), std::memory_order_relaxed);
    auto queue_size = static_cast<uint64_t>(task_queue_.size());
    auto peak = stats_queue_peak_.load(std::memory_order_relaxed);
    while (queue_size > peak &&
           !stats_queue_peak_.compare_exchange_weak(peak, queue_size, std::memory_order_relaxed)) {
    }
  }

  queue_cond_.notify_all();
  return Status::OK();
}

size_t CoreThreadPool::QueueSize() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return task_queue_.size();
}

CoreThreadPool::StatsSnapshot CoreThreadPool::GetStats() const {
  return {
      stats_total_submitted_.load(std::memory_order_relaxed), stats_total_completed_.load(std::memory_order_relaxed),
      stats_total_failed_.load(std::memory_order_relaxed),    stats_total_rejected_.load(std::memory_order_relaxed),
      stats_total_dropped_.load(std::memory_order_relaxed),   stats_total_wait_us_.load(std::memory_order_relaxed),
      stats_total_exec_us_.load(std::memory_order_relaxed),   stats_in_flight_.load(std::memory_order_relaxed),
      stats_queue_peak_.load(std::memory_order_relaxed),
  };
}

bool CoreThreadPool::WaitAndPopTask(std::unique_ptr<CoreTask> *task) {
  std::unique_lock<std::mutex> lock(mutex_);

  idle_workers_.fetch_add(1, std::memory_order_relaxed);
  queue_cond_.wait(lock, [this]() { return shutdown_.load(std::memory_order_relaxed) || !task_queue_.empty(); });
  idle_workers_.fetch_sub(1, std::memory_order_relaxed);

  if (task_queue_.empty()) {
    return false;
  }

  std::pop_heap(task_queue_.begin(), task_queue_.end(), QueueEntryLess{});
  *task = std::move(task_queue_.back().task);
  task_queue_.pop_back();
  return true;
}

void CoreThreadPool::OnTaskStarted(const CoreTask &task) {
  stats_in_flight_.fetch_add(1, std::memory_order_relaxed);
  stats_total_wait_us_.fetch_add(task.GetWaitingTimeUs(), std::memory_order_relaxed);
}

void CoreThreadPool::OnTaskFinished(const CoreTask &task, const Status &status) {
  stats_in_flight_.fetch_sub(1, std::memory_order_relaxed);
  stats_total_exec_us_.fetch_add(task.GetExecutionTimeUs(), std::memory_order_relaxed);
  if (status.IsOK()) {
    stats_total_completed_.fetch_add(1, std::memory_order_relaxed);
  } else {
    stats_total_failed_.fetch_add(1, std::memory_order_relaxed);
  }
}

void CoreThreadPool::CompleteDroppedTasks(std::vector<std::unique_ptr<CoreTask>> tasks, const Status &status) {
  stats_total_dropped_.fetch_add(tasks.size(), std::memory_order_relaxed);
  for (auto &task : tasks) {
    if (!task) {
      continue;
    }
    try {
      task->SetFinishTime(std::chrono::steady_clock::now());
      task->ExecuteCallback(status, "");
    } catch (const std::exception &e) {
      LOG(ERROR) << "[core-pool] dropped task callback failed, task_id=" << task->GetTaskID()
                 << ", name=" << task->GetName() << ", namespace=" << task->GetNamespace()
                 << ", err=" << e.what();
    } catch (...) {
      LOG(ERROR) << "[core-pool] dropped task callback failed with unknown exception, task_id=" << task->GetTaskID()
                 << ", name=" << task->GetName() << ", namespace=" << task->GetNamespace();
    }
  }
}

}  // namespace kvrocks
