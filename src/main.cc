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

#include <bthread/bthread.h>
#include <event2/event.h>
#include <event2/thread.h>
#include <glog/logging.h>

#include <iostream>
#include <vector>

#include "app/app_util.h"
#include "config/config.h"
#include "datanode/datanode_server.h"
#include "scope_exit.h"
#include "server/server.h"
#include "server/tls_util.h"
#include "storage/storage.h"
#include "util.h"

namespace {

Status ApplyBrpcThreadConfig(const Config &config) {
  const int num_threads = config.RunsProxy() ? config.proxy_brpc_num_threads : config.datanode_brpc_num_threads;
  if (num_threads <= 0) {
    return Status::OK();
  }

  if (bthread_setconcurrency(num_threads) != 0) {
    return {Status::NotOK, "failed to set brpc worker threads to " + std::to_string(num_threads)};
  }
  return Status::OK();
}

Status ValidateServiceStartup(const Config &config) {
  if (config.binds.empty()) {
    return Status::OK();
  }

  std::vector<int> ports;
  if (config.RunsProxy()) {
    ports.emplace_back(config.port);
    if (config.tls_port > 0) ports.emplace_back(config.tls_port);
  }
  if (config.RunsDatanode()) {
    ports.emplace_back(config.GetDatanodeListenPort());
  }

  for (size_t i = 0; i < ports.size(); ++i) {
    for (size_t j = i + 1; j < ports.size(); ++j) {
      if (ports[i] == ports[j]) {
        return {Status::NotOK, "configured listen ports must be different when multiple services are enabled"};
      }
    }
  }

  for (int listen_port : ports) {
    if (Util::IsPortInUse(listen_port)) {
      return {Status::NotOK, "the specified port[" + std::to_string(listen_port) + "] is already in use"};
    }
  }

  return Status::OK();
}

int RunProxyService(Config *config) {
  auto s = ValidateServiceStartup(*config);
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to validate proxy startup config: " << s.Msg();
    return 1;
  }

  Server server(nullptr, config);
  AppUtil::SetServer(&server);
  auto cleanup = MakeScopeExit([] {
    AppUtil::ClearStopHandler();
    AppUtil::SetServer(nullptr);
  });

  AppUtil::RegisterStopHandler([&server]() { return server.IsStopped(); }, [&server]() { server.Stop(); });
  s = server.Start();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to start proxy service: " << s.Msg();
    return 1;
  }

  server.Join();
  return 0;
}

int RunDatanodeService(Config *config) {
  Engine::Storage storage(config);
  auto s = storage.Open();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to open storage: " << s.Msg();
    return 1;
  }

  DatanodeServer server(&storage, config);
  AppUtil::SetServer(nullptr);
  auto cleanup = MakeScopeExit([] { AppUtil::ClearStopHandler(); });

  AppUtil::RegisterStopHandler([&server]() { return server.IsStopped(); }, [&server]() { server.Stop(); });
  s = server.Start();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to start datanode service: " << s.Msg();
    return 1;
  }

  server.Join();
  return 0;
}

int RunAllInOneService(Config *config) {
  auto s = ValidateServiceStartup(*config);
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to validate all-in-one startup config: " << s.Msg();
    return 1;
  }

  Engine::Storage storage(config);
  s = storage.Open();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to open storage: " << s.Msg();
    return 1;
  }

  DatanodeServer datanode_server(&storage, config);
  Server proxy_server(nullptr, config);
  AppUtil::SetServer(&proxy_server);
  auto cleanup = MakeScopeExit([] {
    AppUtil::ClearStopHandler();
    AppUtil::SetServer(nullptr);
  });

  AppUtil::RegisterStopHandler([&proxy_server, &datanode_server]() {
                                 return proxy_server.IsStopped() && datanode_server.IsStopped();
                               },
                               [&proxy_server, &datanode_server]() {
                                 proxy_server.Stop();
                                 datanode_server.Stop();
                               });

  s = datanode_server.Start();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to start datanode service: " << s.Msg();
    return 1;
  }

  s = proxy_server.Start();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to start proxy service: " << s.Msg();
    datanode_server.Stop();
    datanode_server.Join();
    return 1;
  }

  proxy_server.Join();
  datanode_server.Join();
  return 0;
}

int RunConfiguredApp(int argc, char **argv) {
  auto opts = AppUtil::ParseCommandLineOptions(argc, argv);

  Config config;
  auto s = config.Load(opts);
  if (!s.IsOK()) {
    std::cout << "Failed to load config, err: " << s.Msg() << std::endl;
    return 1;
  }

  AppUtil::InitGoogleLog(&config);
  AppUtil::PrintVersion(LOG(INFO));
  LOG(INFO) << "Configured service type: " << config.GetServiceTypeName();

  s = ApplyBrpcThreadConfig(config);
  if (!s.IsOK()) {
    LOG(ERROR) << s.Msg();
    return 1;
  }

  bool is_supervised = AppUtil::IsSupervisedMode(config.supervised_mode);
  if (config.daemonize && !is_supervised) AppUtil::Daemonize();

  s = AppUtil::CreatePidFile(config.pidfile);
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to create pidfile: " << s.Msg();
    return 1;
  }
  auto pidfile_exit = MakeScopeExit([&config] { AppUtil::RemovePidFile(config.pidfile); });

#ifdef ENABLE_OPENSSL
  if (config.tls_port) {
    InitSSL();
  }
#endif

  if (config.IsProxy()) {
    return RunProxyService(&config);
  }
  if (config.IsDatanode()) {
    return RunDatanodeService(&config);
  }
  if (config.IsAllInOne()) {
    return RunAllInOneService(&config);
  }

  LOG(ERROR) << "Unsupported service type: " << config.service_type;
  return 1;
}

}  // namespace

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argc > 0 && argv[0] != nullptr ? argv[0] : "kvrocks");
  evthread_use_pthreads();
  AppUtil::SetupSignalHandlers();

  int exit_code = RunConfiguredApp(argc, argv);

  google::ShutdownGoogleLogging();
  libevent_global_shutdown();
  return exit_code;
}
