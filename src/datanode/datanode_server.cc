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

#include "datanode/datanode_server.h"

#include <glog/logging.h>
#include <rocksdb/convenience.h>

#include <thread>

#include "storage/compaction_checker.h"
#include "storage/storage.h"
#include "server/core_thread_pool.h"
#include "util.h"

Status brpc_server_init(Engine::Storage *storage, Config *config, kvrocks::CoreThreadPool *core_thread_pool);
Status brpc_server_start();
void brpc_server_stop();

DatanodeServer::DatanodeServer(Engine::Storage *storage, Config *config)
    : storage_(storage), config_(config), task_runner_(1, 10240) {}

Status DatanodeServer::Start() {
  stop_ = false;
  task_runner_.Start();

  auto s = InitCore();
  if (!s.IsOK()) {
    task_runner_.Stop();
    task_runner_.Join();
    return s;
  }

  s = InitBrpcRedisServer();
  if (!s.IsOK()) {
    ShutdownCore();
    if (core_thread_pool_) {
      core_thread_pool_->Join();
      core_thread_pool_.reset();
    }
    task_runner_.Stop();
    task_runner_.Join();
    return s;
  }

  s = StartBrpcRedisServer();
  if (!s.IsOK()) {
    ShutdownCore();
    if (core_thread_pool_) {
      core_thread_pool_->Join();
      core_thread_pool_.reset();
    }
    task_runner_.Stop();
    task_runner_.Join();
    return s;
  }

  StartCronThread();
  StartCompactionCheckerThread();
  LOG(INFO) << "Datanode is ready";
  return Status::OK();
}

void DatanodeServer::Stop() {
  stop_ = true;
  brpc_server_stop();
  ShutdownCore();
  if (storage_ && storage_->GetDB()) {
    rocksdb::CancelAllBackgroundWork(storage_->GetDB(), true);
  }
  task_runner_.Stop();
}

void DatanodeServer::Join() {
  if (core_thread_pool_) {
    core_thread_pool_->Join();
    core_thread_pool_.reset();
  }
  task_runner_.Join();
  if (cron_thread_.joinable()) cron_thread_.join();
  if (compaction_checker_thread_.joinable()) compaction_checker_thread_.join();
}

Status DatanodeServer::InitBrpcRedisServer() { return brpc_server_init(storage_, config_, core_thread_pool_.get()); }

Status DatanodeServer::StartBrpcRedisServer() { return brpc_server_start(); }

Status DatanodeServer::InitCore() {
  size_t workers = config_->core_threads > 0 ? static_cast<size_t>(config_->core_threads)
                                             : static_cast<size_t>(std::thread::hardware_concurrency());
  if (workers == 0) {
    workers = 4;
  }

  core_thread_pool_ = std::make_unique<kvrocks::CoreThreadPool>();
  auto s = core_thread_pool_->Initialize(workers, static_cast<size_t>(config_->core_queue_size));
  if (!s.IsOK()) {
    core_thread_pool_.reset();
    return s;
  }

  return Status::OK();
}

void DatanodeServer::ShutdownCore() {
  if (core_thread_pool_) {
    core_thread_pool_->Shutdown();
  }
}

void DatanodeServer::StartCronThread() {
  cron_thread_ = std::thread([this]() {
    Util::ThreadSetName("dn-cron");
    while (!stop_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  });
}

void DatanodeServer::StartCompactionCheckerThread() {
  compaction_checker_thread_ = std::thread([this]() {
    uint64_t counter = 0;
    int32_t last_compact_date = 0;
    Util::ThreadSetName("dn-compact");
    CompactionChecker compaction_checker(this->storage_);
    while (!stop_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      auto guard = storage_->ReadLockGuard();
      if (storage_->IsClosing()) continue;

      if (++counter % 600 == 0 && config_->compaction_checker_range.Enabled()) {
        auto now = static_cast<time_t>(Util::GetTimeStamp());
        std::tm local_time{};
        localtime_r(&now, &local_time);
        if (local_time.tm_hour >= config_->compaction_checker_range.Start &&
            local_time.tm_hour <= config_->compaction_checker_range.Stop) {
          std::vector<std::string> cf_names = {Engine::kMetadataColumnFamilyName, Engine::kSubkeyColumnFamilyName,
                                               Engine::kZSetScoreColumnFamilyName, Engine::kStreamColumnFamilyName};
          for (const auto &cf_name : cf_names) {
            compaction_checker.PickCompactionFiles(cf_name);
          }
        }
        if (now != 0 && last_compact_date != now / 86400) {
          last_compact_date = now / 86400;
          compaction_checker.CompactPropagateAndPubSubFiles();
        }
      }
    }
  });
}
