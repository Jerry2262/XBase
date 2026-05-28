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

#include "app/app_util.h"

#include <string>

#include "util.h"

namespace {

std::string GetProgramName(const std::string &path) {
  auto pos = path.find_last_of("/\\");
  if (pos == std::string::npos) return path;
  return path.substr(pos + 1);
}

}  // namespace

namespace AppUtil {

ServiceType DetectServiceType(const std::string &program_name) {
  auto name = Util::ToLower(GetProgramName(program_name));
  if (name.find("proxy") != std::string::npos) {
    return kServiceProxy;
  }
  if (name.find("datanode") != std::string::npos) {
    return kServiceDatanode;
  }
  if (name.find("all") != std::string::npos || name == "kvrocks" || name == "kvrocks-brpc") {
    return kServiceAll;
  }
  return kServiceAll;
}

}  // namespace AppUtil
