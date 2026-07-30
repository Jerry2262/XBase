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

#include <arpa/inet.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <future>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "commands/redis_cmd.h"
#include "config/config.h"
#include "datanode/datanode_server.h"
#include "dispatcher/request_dispatcher.h"
#include "server/server.h"
#include "server/worker.h"
#include "test_base.h"
#include "types/redis_list.h"

class WorkerCompletionQueueHandle;

namespace {

static_assert(std::is_same_v<decltype(std::declval<Worker &>().NewCompletionHandle()),
                             std::shared_ptr<WorkerCompletionQueueHandle>>);

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

int ConnectLoopback(int port) {
  for (int attempt = 0; attempt < 50; ++attempt) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0) return fd;

    close(fd);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return -1;
}

bool SendAll(int fd, const std::string &data) {
  size_t offset = 0;
  while (offset < data.size()) {
    const auto bytes = send(fd, data.data() + offset, data.size() - offset, MSG_NOSIGNAL);
    if (bytes <= 0) return false;
    offset += static_cast<size_t>(bytes);
  }
  return true;
}

std::string ReceiveExactly(int fd, size_t expected_size, int timeout_ms) {
  timeval timeout{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  std::string result;
  result.resize(expected_size);
  size_t offset = 0;
  while (offset < expected_size) {
    const auto bytes = recv(fd, result.data() + offset, expected_size - offset, 0);
    if (bytes <= 0) break;
    offset += static_cast<size_t>(bytes);
  }
  result.resize(offset);
  return result;
}

template <typename Result, typename Start>
StatusOr<Result> AwaitAsync(Start start) {
  auto promise = std::make_shared<std::promise<StatusOr<Result>>>();
  auto future = promise->get_future();
  auto submit = start([promise](StatusOr<Result> result) mutable { promise->set_value(std::move(result)); });
  if (!submit.IsOK()) return submit;
  if (future.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
    return Status(Status::RedisExecErr, "async dispatcher callback timed out");
  }
  return future.get();
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
    proxy_config_.binds = {"127.0.0.1"};
    proxy_config_.port = PickFreePort();
    proxy_config_.workers = 1;
    proxy_config_.slot_id_encoded = false;
    proxy_config_.storage_backend_addrs = "127.0.0.1:" + std::to_string(datanode_port_);
    proxy_config_.storage_rpc_connection_type = "single";
    proxy_config_.storage_rpc_timeout_ms = 1000;
    proxy_config_.storage_rpc_max_retry = 1;
    dispatcher_ = std::make_unique<Dispatcher::RequestDispatcher>(proxy_config_);
    ASSERT_EQ(dispatcher_->init_result(), 0);

    proxy_server_ = std::make_unique<Server>(nullptr, &proxy_config_);
    auto proxy_start_status = proxy_server_->Start();
    ASSERT_TRUE(proxy_start_status.IsOK()) << proxy_start_status.Msg();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void TearDown() override {
    if (proxy_server_) {
      proxy_server_->Stop();
      proxy_server_->Join();
      proxy_server_.reset();
    }
    if (datanode_server_) {
      datanode_server_->Stop();
      datanode_server_->Join();
      datanode_server_.reset();
    }
    dispatcher_.reset();
  }

  StatusOr<std::string> DispatchProxyAsync(const std::string &command_name, std::vector<std::string> tokens) {
    auto commands = Redis::GetOriginalCommands();
    auto iter = commands->find(command_name);
    if (iter == commands->end()) return Status(Status::RedisUnknownCmd, command_name);

    struct CallbackResult {
      Status status;
      std::string reply;
    };
    auto promise = std::make_shared<std::promise<CallbackResult>>();
    auto future = promise->get_future();
    auto submit = proxy_server_->DispatchProxyCommandAsync(*iter->second, tokens, ns_,
                                                           [promise](Status status, std::string reply) mutable {
                                                             promise->set_value({std::move(status), std::move(reply)});
                                                           });
    if (!submit.IsOK()) return submit;
    if (future.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
      return Status(Status::RedisExecErr, "async server callback timed out");
    }
    auto result = future.get();
    if (!result.status.IsOK()) return result.status;
    return std::move(result.reply);
  }

  const std::string ns_ = "ycsb-proxy";
  int datanode_port_ = 0;
  Config proxy_config_;
  std::unique_ptr<DatanodeServer> datanode_server_;
  std::unique_ptr<Dispatcher::RequestDispatcher> dispatcher_;
  std::unique_ptr<Server> proxy_server_;
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

  auto hmset_user1 = dispatcher_->DispatchStatusCommand("HMSET", ns_, {user1, "field0", "value0", "field1", "value1"},
                                                        {0}, Redis::GetOriginalCommands()->at("hmset")->flags);
  ASSERT_TRUE(hmset_user1.IsOK()) << hmset_user1.ToStatus().Msg();
  EXPECT_EQ(hmset_user1->status, "OK");

  auto zadd_user1 = dispatcher_->DispatchSingleCommand("ZADD", ns_, {"_indices", std::to_string(score1), user1}, {0},
                                                       Redis::GetOriginalCommands()->at("zadd")->flags);
  ASSERT_TRUE(zadd_user1.IsOK()) << zadd_user1.ToStatus().Msg();
  EXPECT_EQ(zadd_user1->integer, 1);

  auto hmget_user1 = dispatcher_->DispatchArrayCommand("HMGET", ns_, {user1, "field0", "field1", "missing"}, {0},
                                                       Redis::GetOriginalCommands()->at("hmget")->flags);
  ASSERT_TRUE(hmget_user1.IsOK()) << hmget_user1.ToStatus().Msg();
  EXPECT_EQ(hmget_user1->values, (std::vector<std::string>{"value0", "value1", ""}));
  EXPECT_EQ(hmget_user1->founds, (std::vector<bool>{true, true, false}));

  auto hgetall_user1 = dispatcher_->DispatchArrayCommand("HGETALL", ns_, {user1}, {0},
                                                         Redis::GetOriginalCommands()->at("hgetall")->flags);
  ASSERT_TRUE(hgetall_user1.IsOK()) << hgetall_user1.ToStatus().Msg();
  EXPECT_EQ(hgetall_user1->values, (std::vector<std::string>{"field0", "value0", "field1", "value1"}));

  auto hmset_user2 = dispatcher_->DispatchStatusCommand("HMSET", ns_, {user2, "field0", "value2", "field1", "value3"},
                                                        {0}, Redis::GetOriginalCommands()->at("hmset")->flags);
  ASSERT_TRUE(hmset_user2.IsOK()) << hmset_user2.ToStatus().Msg();

  auto zadd_user2 = dispatcher_->DispatchSingleCommand("ZADD", ns_, {"_indices", std::to_string(score2), user2}, {0},
                                                       Redis::GetOriginalCommands()->at("zadd")->flags);
  ASSERT_TRUE(zadd_user2.IsOK()) << zadd_user2.ToStatus().Msg();
  EXPECT_EQ(zadd_user2->integer, 1);

  auto scan_result =
      dispatcher_->DispatchArrayCommand("ZRANGEBYSCORE", ns_, {"_indices", first_score, "+inf", "LIMIT", "0", "2"}, {0},
                                        Redis::GetOriginalCommands()->at("zrangebyscore")->flags);
  ASSERT_TRUE(scan_result.IsOK()) << scan_result.ToStatus().Msg();
  EXPECT_EQ(scan_result->values, (std::vector<std::string>{first_key, second_key}));

  auto rmw_read = dispatcher_->DispatchArrayCommand("HMGET", ns_, {first_key, "field1"}, {0},
                                                    Redis::GetOriginalCommands()->at("hmget")->flags);
  ASSERT_TRUE(rmw_read.IsOK()) << rmw_read.ToStatus().Msg();
  ASSERT_EQ(rmw_read->values.size(), 1);

  auto rmw_write = dispatcher_->DispatchStatusCommand("HMSET", ns_, {first_key, "field1", "updated"}, {0},
                                                      Redis::GetOriginalCommands()->at("hmset")->flags);
  ASSERT_TRUE(rmw_write.IsOK()) << rmw_write.ToStatus().Msg();

  auto updated_record = dispatcher_->DispatchArrayCommand("HGETALL", ns_, {first_key}, {0},
                                                          Redis::GetOriginalCommands()->at("hgetall")->flags);
  ASSERT_TRUE(updated_record.IsOK()) << updated_record.ToStatus().Msg();
  EXPECT_NE(std::find(updated_record->values.begin(), updated_record->values.end(), "updated"),
            updated_record->values.end());

  auto delete_result =
      dispatcher_->DispatchIntegerCommand("DEL", ns_, {first_key}, {0}, Redis::GetOriginalCommands()->at("del")->flags);
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

  auto async_set = AwaitAsync<Dispatcher::RequestResult>([&](auto done) {
    return dispatcher_->DispatchStatusCommandAsync("HMSET", ns_, {"user-async", "field0", "value0"}, {0},
                                                   Redis::GetOriginalCommands()->at("hmset")->flags, std::move(done));
  });
  ASSERT_TRUE(async_set.IsOK()) << async_set.ToStatus().Msg();
  EXPECT_EQ(async_set->status, "OK");

  auto async_get = AwaitAsync<Dispatcher::MultiRequestResult>([&](auto done) {
    return dispatcher_->DispatchArrayCommandAsync("HMGET", ns_, {"user-async", "field0", "missing"}, {0},
                                                  Redis::GetOriginalCommands()->at("hmget")->flags, std::move(done));
  });
  ASSERT_TRUE(async_get.IsOK()) << async_get.ToStatus().Msg();
  EXPECT_EQ(async_get->values, (std::vector<std::string>{"value0", ""}));
  EXPECT_EQ(async_get->founds, (std::vector<bool>{true, false}));

  auto async_string_set = AwaitAsync<Dispatcher::RequestResult>([&](auto done) {
    return dispatcher_->DispatchRequestAsync(
        Dispatcher::StringSetRequest(ns_, "string-async", "string-value",
                                     Redis::GetOriginalCommands()->at("set")->flags),
        std::move(done));
  });
  ASSERT_TRUE(async_string_set.IsOK()) << async_string_set.ToStatus().Msg();
  EXPECT_EQ(async_string_set->status, "OK");

  auto async_string_get = AwaitAsync<Dispatcher::RequestResult>([&](auto done) {
    return dispatcher_->DispatchRequestAsync(
        Dispatcher::StringGetRequest(ns_, "string-async", Redis::GetOriginalCommands()->at("get")->flags),
        std::move(done));
  });
  ASSERT_TRUE(async_string_get.IsOK()) << async_string_get.ToStatus().Msg();
  EXPECT_TRUE(async_string_get->found);
  EXPECT_EQ(async_string_get->value, "string-value");

  auto async_hash_set = AwaitAsync<Dispatcher::RequestResult>([&](auto done) {
    return dispatcher_->DispatchRequestAsync(
        Dispatcher::HashSetRequest(ns_, "hash-async", "field0", "hash-value",
                                   Redis::GetOriginalCommands()->at("hset")->flags),
        std::move(done));
  });
  ASSERT_TRUE(async_hash_set.IsOK()) << async_hash_set.ToStatus().Msg();
  EXPECT_EQ(async_hash_set->integer, 1);

  auto async_hash_get = AwaitAsync<Dispatcher::RequestResult>([&](auto done) {
    return dispatcher_->DispatchRequestAsync(
        Dispatcher::HashGetRequest(ns_, "hash-async", "field0", Redis::GetOriginalCommands()->at("hget")->flags),
        std::move(done));
  });
  ASSERT_TRUE(async_hash_get.IsOK()) << async_hash_get.ToStatus().Msg();
  EXPECT_TRUE(async_hash_get->found);
  EXPECT_EQ(async_hash_get->value, "hash-value");

  Dispatcher::MultiSetRequest async_multi_set_request;
  async_multi_set_request.ns = ns_;
  async_multi_set_request.command_flags = Redis::GetOriginalCommands()->at("mset")->flags;
  async_multi_set_request.key_values = {{"multi-1", "value-1"}, {"multi-2", "value-2"}};
  auto async_multi_set = AwaitAsync<Dispatcher::MultiRequestResult>(
      [&](auto done) { return dispatcher_->DispatchRequestAsync(async_multi_set_request, std::move(done)); });
  ASSERT_TRUE(async_multi_set.IsOK()) << async_multi_set.ToStatus().Msg();
  EXPECT_EQ(async_multi_set->status, "OK");

  Dispatcher::MultiGetRequest async_multi_get_request;
  async_multi_get_request.ns = ns_;
  async_multi_get_request.command_flags = Redis::GetOriginalCommands()->at("mget")->flags;
  async_multi_get_request.keys = {"multi-1", "missing", "multi-2"};
  auto async_multi_get = AwaitAsync<Dispatcher::MultiRequestResult>(
      [&](auto done) { return dispatcher_->DispatchRequestAsync(async_multi_get_request, std::move(done)); });
  ASSERT_TRUE(async_multi_get.IsOK()) << async_multi_get.ToStatus().Msg();
  EXPECT_EQ(async_multi_get->values, (std::vector<std::string>{"value-1", "", "value-2"}));
  EXPECT_EQ(async_multi_get->founds, (std::vector<bool>{true, false, true}));

  auto async_zadd = AwaitAsync<Dispatcher::RequestResult>([&](auto done) {
    return dispatcher_->DispatchSingleCommandAsync("ZADD", ns_, {"zset-async", "1", "member"}, {0},
                                                   Redis::GetOriginalCommands()->at("zadd")->flags, std::move(done));
  });
  ASSERT_TRUE(async_zadd.IsOK()) << async_zadd.ToStatus().Msg();
  EXPECT_EQ(async_zadd->integer, 1);

  auto async_del = AwaitAsync<Dispatcher::RequestResult>([&](auto done) {
    return dispatcher_->DispatchIntegerCommandAsync("DEL", ns_, {"string-async"}, {0},
                                                    Redis::GetOriginalCommands()->at("del")->flags, std::move(done));
  });
  ASSERT_TRUE(async_del.IsOK()) << async_del.ToStatus().Msg();
  EXPECT_EQ(async_del->integer, 1);

  auto expect_proxy_reply = [&](const std::string &command, std::vector<std::string> tokens,
                                const std::string &expected) {
    auto result = DispatchProxyAsync(command, std::move(tokens));
    ASSERT_TRUE(result.IsOK()) << result.ToStatus().Msg();
    EXPECT_EQ(*result, expected);
  };

  expect_proxy_reply("set", {"set", "server-string", "value"}, "+OK\r\n");
  expect_proxy_reply("get", {"get", "server-string"}, "$5\r\nvalue\r\n");
  expect_proxy_reply("mset", {"mset", "server-multi-1", "one", "server-multi-2", "two"}, "+OK\r\n");
  expect_proxy_reply("mget", {"mget", "server-multi-1", "server-missing", "server-multi-2"},
                     "*3\r\n$3\r\none\r\n$-1\r\n$3\r\ntwo\r\n");
  expect_proxy_reply("del", {"del", "server-string"}, ":1\r\n");

  expect_proxy_reply("hset", {"hset", "server-hash", "field0", "hash-value"}, ":1\r\n");
  expect_proxy_reply("hget", {"hget", "server-hash", "field0"}, "$10\r\nhash-value\r\n");
  expect_proxy_reply("hmset", {"hmset", "server-hmset", "field0", "value0"}, "+OK\r\n");
  expect_proxy_reply("hmget", {"hmget", "server-hash", "field0", "missing"}, "*2\r\n$10\r\nhash-value\r\n$-1\r\n");
  expect_proxy_reply("hgetall", {"hgetall", "server-hash"}, "*2\r\n$6\r\nfield0\r\n$10\r\nhash-value\r\n");

  Redis::List list(storage_, ns_);
  std::vector<Slice> list_values = {"before"};
  int list_size = 0;
  ASSERT_TRUE(list.Push("server-list", list_values, false, &list_size).ok());
  ASSERT_EQ(list_size, 1);
  expect_proxy_reply("lset", {"lset", "server-list", "0", "after"}, "+OK\r\n");
  expect_proxy_reply("lindex", {"lindex", "server-list", "0"}, "$5\r\nafter\r\n");

  expect_proxy_reply("zadd", {"zadd", "server-zset", "1", "member"}, ":1\r\n");
  expect_proxy_reply("zrangebyscore", {"zrangebyscore", "server-zset", "-inf", "+inf"}, "*1\r\n$6\r\nmember\r\n");
  expect_proxy_reply("zrem", {"zrem", "server-zset", "member"}, ":1\r\n");

  const std::string ordered_pipeline =
      "*4\r\n$5\r\nhmset\r\n$10\r\nuser-order\r\n$6\r\nfield0\r\n$5\r\nfirst\r\n"
      "*4\r\n$5\r\nhmset\r\n$10\r\nuser-order\r\n$6\r\nfield0\r\n$6\r\nsecond\r\n"
      "*3\r\n$5\r\nhmget\r\n$10\r\nuser-order\r\n$6\r\nfield0\r\n";
  const std::string ordered_expected = "+OK\r\n+OK\r\n*1\r\n$6\r\nsecond\r\n";
  int ordered_fd = ConnectLoopback(proxy_config_.port);
  ASSERT_GE(ordered_fd, 0);
  ASSERT_TRUE(SendAll(ordered_fd, ordered_pipeline));
  EXPECT_EQ(ReceiveExactly(ordered_fd, ordered_expected.size(), 3000), ordered_expected);
  close(ordered_fd);

  std::string long_pipeline;
  const std::string repeated_hmset = "*4\r\n$5\r\nhmset\r\n$9\r\nflood-key\r\n$6\r\nfield0\r\n$6\r\nvalue0\r\n";
  long_pipeline.reserve(repeated_hmset.size() * 65536);
  for (int i = 0; i < 65536; ++i) long_pipeline.append(repeated_hmset);

  int flood_fd = ConnectLoopback(proxy_config_.port);
  ASSERT_GE(flood_fd, 0);
  int send_buffer_size = 4 * 1024 * 1024;
  ASSERT_EQ(setsockopt(flood_fd, SOL_SOCKET, SO_SNDBUF, &send_buffer_size, sizeof(send_buffer_size)), 0);
  int probe_fd = ConnectLoopback(proxy_config_.port);
  ASSERT_GE(probe_fd, 0);
  std::thread flood_sender([&]() { SendAll(flood_fd, long_pipeline); });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  const bool probe_sent = SendAll(probe_fd, "*1\r\n$4\r\nping\r\n");
  const auto probe_reply = probe_sent ? ReceiveExactly(probe_fd, 7, 200) : std::string();
  close(probe_fd);
  shutdown(flood_fd, SHUT_RDWR);
  flood_sender.join();
  close(flood_fd);
  EXPECT_EQ(probe_reply, "+PONG\r\n");

  int recovery_fd = ConnectLoopback(proxy_config_.port);
  ASSERT_GE(recovery_fd, 0);
  ASSERT_TRUE(SendAll(recovery_fd, "*1\r\n$4\r\nping\r\n"));
  EXPECT_EQ(ReceiveExactly(recovery_fd, 7, 3000), "+PONG\r\n");
  close(recovery_fd);

  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count();
  RecordProperty("ycsb_sequence_ms", static_cast<int>(elapsed_ms));
  EXPECT_GE(elapsed_ms, 0);
}

#endif  // BRPC_FOUND
