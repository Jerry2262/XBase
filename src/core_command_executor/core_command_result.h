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

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace kvrocks_datanode {

// 核心命令返回值的种类标签。
// 决定统一结果对象在上层应按哪种 RESP 语义编码。
enum class CoreReplyKind {
  kNil = 0,
  kString,
  kInteger,
  kStatus,
  kArray,
};

// 核心命令统一返回类型。
// 语义上对应 proxy-wt 中 RequestResult / MultiRequestResult 的合并版本。
struct CoreCommandResult {
  CoreReplyKind reply_kind = CoreReplyKind::kNil;  // 返回值种类，决定上层如何编码为 RESP。
  std::string value;                               // 单值字符串结果，例如 GET/HGET/LINDEX 命中时返回。
  int64_t integer = 0;                             // 整数结果，例如 HSET 返回新增 field 数量。
  std::string status;                              // 状态结果，例如 SET/MSET/LSET 成功时返回 "OK"。
  std::vector<std::string> values;                 // 数组结果中的每个值，对应 MGET 的批量返回。
  std::vector<bool> founds;                        // 数组结果中的命中标记，与 values 按位置一一对应。

  // 构造 nil 返回，表示单值读取未命中。
  static CoreCommandResult Nil() { return CoreCommandResult{}; }

  // 构造字符串返回。
  static CoreCommandResult String(std::string v) {
    CoreCommandResult result;
    result.reply_kind = CoreReplyKind::kString;
    result.value = std::move(v);
    return result;
  }

  // 构造整数返回。
  static CoreCommandResult Integer(int64_t v) {
    CoreCommandResult result;
    result.reply_kind = CoreReplyKind::kInteger;
    result.integer = v;
    return result;
  }

  // 构造状态返回。
  static CoreCommandResult Status(std::string v) {
    CoreCommandResult result;
    result.reply_kind = CoreReplyKind::kStatus;
    result.status = std::move(v);
    return result;
  }

  // 构造数组返回，主要用于 MGET。
  static CoreCommandResult Array(std::vector<std::string> vs, std::vector<bool> fs) {
    CoreCommandResult result;
    result.reply_kind = CoreReplyKind::kArray;
    result.values = std::move(vs);
    result.founds = std::move(fs);
    return result;
  }
};

}  // namespace kvrocks_datanode
