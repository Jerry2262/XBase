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
 */

#include "server/worker_completion_queue.h"

#include <event2/event.h>
#include <gtest/gtest.h>

#include <memory>
#include <thread>

TEST(WorkerCompletionQueueTest, DeliversCompletionOnEventLoopThread) {
  auto *base = event_base_new();
  ASSERT_NE(base, nullptr);
  const auto event_thread = std::this_thread::get_id();
  bool delivered = false;
  auto queue = std::make_shared<WorkerCompletionQueue>(base, [&](ProxyCommandCompletion completion) {
    EXPECT_EQ(std::this_thread::get_id(), event_thread);
    EXPECT_EQ(completion.fd, 11);
    EXPECT_EQ(completion.connection_id, 22);
    EXPECT_TRUE(completion.status.IsOK());
    EXPECT_EQ(completion.reply, "+OK\r\n");
    delivered = true;
    event_base_loopbreak(base);
  });

  std::thread producer([queue] { EXPECT_TRUE(queue->Post({11, 22, Status::OK(), "+OK\r\n"})); });
  event_base_dispatch(base);
  producer.join();
  EXPECT_TRUE(delivered);
  queue->Stop();
  queue.reset();
  event_base_free(base);
}

TEST(WorkerCompletionQueueTest, RejectsCompletionAfterStop) {
  auto *base = event_base_new();
  ASSERT_NE(base, nullptr);
  auto queue = std::make_shared<WorkerCompletionQueue>(
      base, [](ProxyCommandCompletion) { FAIL() << "stopped queue invoked handler"; });
  queue->Stop();
  EXPECT_FALSE(queue->Post({11, 22, Status::OK(), "+OK\r\n"}));
  queue.reset();
  event_base_free(base);
}

TEST(WorkerCompletionQueueTest, HandleRejectsCompletionAfterStop) {
  auto *base = event_base_new();
  ASSERT_NE(base, nullptr);
  auto queue = std::make_unique<WorkerCompletionQueue>(
      base, [](ProxyCommandCompletion) { FAIL() << "stopped handle invoked handler"; });
  WorkerCompletionQueueHandle handle(queue.get());
  handle.Stop();
  EXPECT_FALSE(handle.Post({11, 22, Status::OK(), "+OK\r\n"}));
  queue->Stop();
  queue.reset();
  event_base_free(base);
}
