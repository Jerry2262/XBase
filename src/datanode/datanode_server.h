#pragma once

#include <atomic>
#include <memory>
#include <thread>

#include "config/config.h"
#include "server/core_thread_pool.h"
#include "task_runner.h"

namespace Engine {
class Storage;
}

class DatanodeServer {
 public:
  explicit DatanodeServer(Engine::Storage *storage, Config *config);
  ~DatanodeServer() = default;

  Status Start();
  void Stop();
  void Join();
  bool IsStopped() { return stop_; }
  kvrocks::CoreThreadPool *GetCoreThreadPool() { return core_thread_pool_.get(); }

 private:
  Status InitBrpcRedisServer();
  Status StartBrpcRedisServer();
  Status InitCore();
  void ShutdownCore();
  void StartCronThread();
  void StartCompactionCheckerThread();

  Engine::Storage *storage_ = nullptr;
  Config *config_ = nullptr;
  TaskRunner task_runner_;
  std::unique_ptr<kvrocks::CoreThreadPool> core_thread_pool_;
  std::atomic<bool> stop_{false};
  std::thread cron_thread_;
  std::thread compaction_checker_thread_;
};
