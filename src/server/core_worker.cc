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

#include "core_worker.h"

#include <exception>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include <glog/logging.h>

#include "core_thread_pool.h"
#include "util.h"

namespace kvrocks {

CoreWorker::CoreWorker(CoreThreadPool *pool, size_t worker_id, std::string thread_name)
    : pool_(pool), worker_id_(worker_id), thread_name_(std::move(thread_name)) {
  if (thread_name_.empty()) {
    thread_name_ = "core-worker";
  }
}

void CoreWorker::Run(std::thread::id tid) {
  tid_ = tid;
  running_.store(true, std::memory_order_relaxed);
  while (true) {
    std::unique_ptr<CoreTask> task;
    if (!pool_->WaitAndPopTask(&task)) {
      break;
    }
    ProcessTask(std::move(task));
  }

  running_.store(false, std::memory_order_relaxed);
}

void CoreWorker::ProcessTask(std::unique_ptr<CoreTask> task) {
  if (!task) {
    return;
  }

  task->SetStartTime(std::chrono::steady_clock::now());
  pool_->OnTaskStarted(*task);

  std::string reply;
  Status status;
  try {
    if (task->IsExclusive()) {
      std::unique_lock<std::shared_mutex> execution_guard(pool_->GetExecutionMutex());
      status = task->Execute(&reply);
    } else {
      std::shared_lock<std::shared_mutex> execution_guard(pool_->GetExecutionMutex());
      status = task->Execute(&reply);
    }
  } catch (const std::exception &e) {
    LOG(ERROR) << "[core-worker] task execution threw exception, task_id=" << task->GetTaskID()
               << ", name=" << task->GetName() << ", namespace=" << task->GetNamespace()
               << ", err=" << e.what();
    status = {Status::NotOK, std::string("uncaught core task exception: ") + e.what()};
  } catch (...) {
    LOG(ERROR) << "[core-worker] task execution threw unknown exception, task_id=" << task->GetTaskID()
               << ", name=" << task->GetName() << ", namespace=" << task->GetNamespace();
    status = {Status::NotOK, "uncaught core task exception"};
  }

  task->SetFinishTime(std::chrono::steady_clock::now());
  pool_->OnTaskFinished(*task, status);

  if (!status.IsOK()) {
    tasks_failed_.fetch_add(1, std::memory_order_relaxed);
  }
  tasks_processed_.fetch_add(1, std::memory_order_relaxed);

  HandleTaskResult(*task, status, reply);
}

void CoreWorker::HandleTaskResult(const CoreTask &task, const Status &status, const std::string &reply) const {
  try {
    task.ExecuteCallback(status, reply);
  } catch (const std::exception &e) {
    LOG(ERROR) << "[core-worker] task callback failed, task_id=" << task.GetTaskID()
               << ", name=" << task.GetName() << ", namespace=" << task.GetNamespace() << ", err=" << e.what();
  } catch (...) {
    LOG(ERROR) << "[core-worker] task callback failed with unknown exception, task_id=" << task.GetTaskID()
               << ", name=" << task.GetName() << ", namespace=" << task.GetNamespace();
  }
}

Status CoreWorkerThread::Start() {
  try {
    thread_ = std::thread([this]() {
      Util::ThreadSetName(worker_->GetThreadName().c_str());
      worker_->Run(std::this_thread::get_id());
    });
  } catch (const std::system_error &e) {
    LOG(ERROR) << "[core-worker] failed to start worker " << worker_->GetID()
               << ", thread_name=" << worker_->GetThreadName() << ", err=" << e.what();
    return {Status::NotOK, e.what()};
  }
  return Status::OK();
}

void CoreWorkerThread::Join() {
  if (thread_.joinable()) {
    thread_.join();
  }
}

}  // namespace kvrocks
