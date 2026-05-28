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

#include "proxy/proxy_app.h"

#include <event2/event.h>
#include <event2/thread.h>
#include <glog/logging.h>

#include <iostream>

#include "app/app_util.h"
#include "scope_exit.h"
#include "server/server.h"
#include "server/tls_util.h"

int ProxyApp::Run(int argc, char **argv) {
  google::InitGoogleLogging("kvrocks-proxy");
  evthread_use_pthreads();
  AppUtil::SetupSignalHandlers();

  auto opts = AppUtil::ParseCommandLineOptions(argc, argv);

  Config config;
  Status s = config.Load(opts);
  if (!s.IsOK()) {
    std::cout << "Failed to load config, err: " << s.Msg() << std::endl;
    return 1;
  }
  AppUtil::InitGoogleLog(&config);
  AppUtil::PrintVersion(LOG(INFO));

  if (!config.binds.empty()) {
    int ports[] = {config.port, config.tls_port, 0};
    for (int *port = ports; *port; ++port) {
      if (Util::IsPortInUse(*port)) {
        LOG(ERROR) << "Could not create server TCP since the specified port[" << *port << "] is already in use";
        return 1;
      }
    }
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

  Server server(nullptr, &config);
  AppUtil::SetServer(&server);
  AppUtil::RegisterStopHandler([&server]() { return server.IsStopped(); }, [&server]() { server.Stop(); });

  s = server.Start();
  if (!s.IsOK()) {
    AppUtil::ClearStopHandler();
    return 1;
  }
  server.Join();

  AppUtil::ClearStopHandler();
  google::ShutdownGoogleLogging();
  libevent_global_shutdown();
  return 0;
}
