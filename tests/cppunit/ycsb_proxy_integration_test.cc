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

#ifdef BRPC_FOUND

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "commands/redis_cmd.h"
#include "config/config.h"
#include "datanode/datanode_server.h"
#include "dispatcher/request_dispatcher.h"
#include "test_base.h"

namespace {

int PickFreePort() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  CHECK(fd >= 0);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  CHECK_EQ(bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)), 0);

  socklen_t len = sizeof(addr);
  CHECK_EQ(getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len), 0);
  const int port = ntohs(addr.sin_port);
  close(fd);
  return port;
}

int JavaHashCode(const std::string &value) {
  int32_t hash = 0;
  for (unsigned char c : value) {
    hash = hash * 31 + c;
  }
  return hash;
}

}  // namespace

class YcsbProxyIntegrationTest : public TestBase {
 protected:
  void SetUp() override {
    datanode_port_ = PickFreePort();
    config_->service_type = kServiceDatanode;
    config_->port = datanode_port_;
    config_->workers = 1;
    config_->core_threads = 1;
    config_->slot_id_encoded = false;

    datanode_server_ = std::make_unique<DatanodeServer>(storage_, config_);
    auto start_status = datanode_server_->Start();
    ASSERT_TRUE(start_status.IsOK()) << start_status.Msg();

    proxy_config_.service_type = kServiceProxy;
    proxy_config_.slot_id_encoded = false;
    proxy_config_.storage_backend_addrs = "127.0.0.1:" + std::to_string(datanode_port_);
    proxy_config_.storage_rpc_connection_type = "single";
    proxy_config_.storage_rpc_timeout_ms = 1000;
    proxy_config_.storage_rpc_max_retry = 1;
    dispatcher_ = std::make_unique<Dispatcher::RequestDispatcher>(proxy_config_);
    ASSERT_EQ(dispatcher_->init_result(), 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void TearDown() override {
    if (datanode_server_) {
      datanode_server_->Stop();
      datanode_server_->Join();
      datanode_server_.reset();
    }
    dispatcher_.reset();
  }

  const std::string ns_ = "ycsb-proxy";
  int datanode_port_ = 0;
  Config proxy_config_;
  std::unique_ptr<DatanodeServer> datanode_server_;
  std::unique_ptr<Dispatcher::RequestDispatcher> dispatcher_;
};

TEST_F(YcsbProxyIntegrationTest, SupportsYcsbReadWriteScanDeleteSequence) {
  const std::string user1 = "user0001";
  const std::string user2 = "user0002";
  const double score1 = JavaHashCode(user1);
  const double score2 = JavaHashCode(user2);
  const std::string first_key = score1 <= score2 ? user1 : user2;
  const std::string second_key = score1 <= score2 ? user2 : user1;
  const std::string first_score = std::to_string(score1 <= score2 ? score1 : score2);

  const auto begin = std::chrono::steady_clock::now();

  auto hmset_user1 = dispatcher_->DispatchStatusCommand(
      "HMSET", ns_, {user1, "field0", "value0", "field1", "value1"}, {0}, Redis::GetOriginalCommands()->at("hmset")->flags);
  ASSERT_TRUE(hmset_user1.IsOK()) << hmset_user1.ToStatus().Msg();
  EXPECT_EQ(hmset_user1->status, "OK");

  auto zadd_user1 = dispatcher_->DispatchSingleCommand(
      "ZADD", ns_, {"_indices", std::to_string(score1), user1}, {0}, Redis::GetOriginalCommands()->at("zadd")->flags);
  ASSERT_TRUE(zadd_user1.IsOK()) << zadd_user1.ToStatus().Msg();
  EXPECT_EQ(zadd_user1->integer, 1);

  auto hmget_user1 =
      dispatcher_->DispatchArrayCommand("HMGET", ns_, {user1, "field0", "field1", "missing"}, {0},
                                        Redis::GetOriginalCommands()->at("hmget")->flags);
  ASSERT_TRUE(hmget_user1.IsOK()) << hmget_user1.ToStatus().Msg();
  EXPECT_EQ(hmget_user1->values, (std::vector<std::string>{"value0", "value1", ""}));
  EXPECT_EQ(hmget_user1->founds, (std::vector<bool>{true, true, false}));

  auto hgetall_user1 =
      dispatcher_->DispatchArrayCommand("HGETALL", ns_, {user1}, {0}, Redis::GetOriginalCommands()->at("hgetall")->flags);
  ASSERT_TRUE(hgetall_user1.IsOK()) << hgetall_user1.ToStatus().Msg();
  EXPECT_EQ(hgetall_user1->values, (std::vector<std::string>{"field0", "value0", "field1", "value1"}));

  auto hmset_user2 = dispatcher_->DispatchStatusCommand(
      "HMSET", ns_, {user2, "field0", "value2", "field1", "value3"}, {0}, Redis::GetOriginalCommands()->at("hmset")->flags);
  ASSERT_TRUE(hmset_user2.IsOK()) << hmset_user2.ToStatus().Msg();

  auto zadd_user2 = dispatcher_->DispatchSingleCommand(
      "ZADD", ns_, {"_indices", std::to_string(score2), user2}, {0}, Redis::GetOriginalCommands()->at("zadd")->flags);
  ASSERT_TRUE(zadd_user2.IsOK()) << zadd_user2.ToStatus().Msg();
  EXPECT_EQ(zadd_user2->integer, 1);

  auto scan_result = dispatcher_->DispatchArrayCommand("ZRANGEBYSCORE", ns_,
                                                       {"_indices", first_score, "+inf", "LIMIT", "0", "2"}, {0},
                                                       Redis::GetOriginalCommands()->at("zrangebyscore")->flags);
  ASSERT_TRUE(scan_result.IsOK()) << scan_result.ToStatus().Msg();
  EXPECT_EQ(scan_result->values, (std::vector<std::string>{first_key, second_key}));

  auto rmw_read = dispatcher_->DispatchArrayCommand("HMGET", ns_, {first_key, "field1"}, {0},
                                                    Redis::GetOriginalCommands()->at("hmget")->flags);
  ASSERT_TRUE(rmw_read.IsOK()) << rmw_read.ToStatus().Msg();
  ASSERT_EQ(rmw_read->values.size(), 1);

  auto rmw_write = dispatcher_->DispatchStatusCommand(
      "HMSET", ns_, {first_key, "field1", "updated"}, {0}, Redis::GetOriginalCommands()->at("hmset")->flags);
  ASSERT_TRUE(rmw_write.IsOK()) << rmw_write.ToStatus().Msg();

  auto updated_record =
      dispatcher_->DispatchArrayCommand("HGETALL", ns_, {first_key}, {0}, Redis::GetOriginalCommands()->at("hgetall")->flags);
  ASSERT_TRUE(updated_record.IsOK()) << updated_record.ToStatus().Msg();
  EXPECT_NE(std::find(updated_record->values.begin(), updated_record->values.end(), "updated"),
            updated_record->values.end());

  auto delete_result = dispatcher_->DispatchIntegerCommand("DEL", ns_, {first_key}, {0},
                                                           Redis::GetOriginalCommands()->at("del")->flags);
  ASSERT_TRUE(delete_result.IsOK()) << delete_result.ToStatus().Msg();
  EXPECT_EQ(delete_result->integer, 1);

  auto zrem_result = dispatcher_->DispatchIntegerCommand("ZREM", ns_, {"_indices", first_key}, {0},
                                                         Redis::GetOriginalCommands()->at("zrem")->flags);
  ASSERT_TRUE(zrem_result.IsOK()) << zrem_result.ToStatus().Msg();
  EXPECT_EQ(zrem_result->integer, 1);

  auto post_delete = dispatcher_->DispatchArrayCommand("HMGET", ns_, {first_key, "field0"}, {0},
                                                       Redis::GetOriginalCommands()->at("hmget")->flags);
  ASSERT_TRUE(post_delete.IsOK()) << post_delete.ToStatus().Msg();
  EXPECT_EQ(post_delete->founds, (std::vector<bool>{false}));

  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count();
  RecordProperty("ycsb_sequence_ms", static_cast<int>(elapsed_ms));
  EXPECT_GE(elapsed_ms, 0);
}

#endif  // BRPC_FOUND
