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

#include "redis_string.h"

#include <cmath>
#include <limits>
#include <string>

#include "parse_util.h"

namespace Redis {

namespace {

void EncodeStringRawValue(const std::string &value, std::string *raw_value, uint32_t expire = 0) {
  raw_value->clear();
  Metadata metadata(kRedisString, false);
  metadata.expire = expire;
  metadata.Encode(raw_value);
  raw_value->append(value);
}

bool TryGetCachedStringValue(Engine::Storage *storage, const std::string &ns_key, std::string *value) {
  return storage->Cache().TryGet(StringCacheKey(ns_key), value);
}

}  // namespace

std::vector<rocksdb::Status> String::getRawValues(const std::vector<Slice> &keys,
                                                  std::vector<std::string> *raw_values) {
  raw_values->clear();

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  raw_values->resize(keys.size());
  std::vector<rocksdb::Status> statuses(keys.size());
  std::vector<rocksdb::PinnableSlice> pin_values(keys.size());
  db_->MultiGet(read_options, metadata_cf_handle_, keys.size(), keys.data(), pin_values.data(), statuses.data(), false);
  for (size_t i = 0; i < keys.size(); i++) {
    if (!statuses[i].ok()) continue;
    (*raw_values)[i].assign(pin_values[i].data(), pin_values[i].size());
    Metadata metadata(kRedisNone, false);
    metadata.Decode((*raw_values)[i]);
    if (metadata.Expired()) {
      (*raw_values)[i].clear();
      statuses[i] = rocksdb::Status::NotFound(kErrMsgKeyExpired);
      continue;
    }
    if (metadata.Type() != kRedisString && metadata.size > 0) {
      (*raw_values)[i].clear();
      statuses[i] = rocksdb::Status::InvalidArgument(kErrMsgWrongType);
      continue;
    }
  }
  return statuses;
}

rocksdb::Status String::getRawValue(const std::string &ns_key, std::string *raw_value) {
  raw_value->clear();

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Status s = db_->Get(read_options, metadata_cf_handle_, ns_key, raw_value);
  if (!s.ok()) return s;

  Metadata metadata(kRedisNone, false);
  metadata.Decode(*raw_value);
  if (metadata.Expired()) {
    raw_value->clear();
    return rocksdb::Status::NotFound(kErrMsgKeyExpired);
  }
  if (metadata.Type() != kRedisString && metadata.size > 0) {
    return rocksdb::Status::InvalidArgument(kErrMsgWrongType);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status String::getValue(const std::string &ns_key, std::string *value) {
  value->clear();

  std::string raw_value;
  auto s = getRawValue(ns_key, &raw_value);
  if (!s.ok()) return s;
  *value = raw_value.substr(STRING_HDR_SIZE, raw_value.size() - STRING_HDR_SIZE);
  return rocksdb::Status::OK();
}

std::vector<rocksdb::Status> String::getValues(const std::vector<Slice> &ns_keys, std::vector<std::string> *values) {
  auto statuses = getRawValues(ns_keys, values);
  for (size_t i = 0; i < ns_keys.size(); i++) {
    if (!statuses[i].ok()) continue;
    syncCacheValue(ns_keys[i].ToString(), (*values)[i]);
    (*values)[i] = (*values)[i].substr(STRING_HDR_SIZE, (*values)[i].size() - STRING_HDR_SIZE);
  }
  return statuses;
}

rocksdb::Status String::updateRawValue(const std::string &ns_key, const std::string &raw_value) {
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisString);
  batch.PutLogData(log_data.Encode());
  batch.Put(metadata_cf_handle_, ns_key, raw_value);
  return storage_->Write(storage_->DefaultWriteOptions(), &batch);
}

void String::syncCacheValue(const std::string &ns_key, const std::string &raw_value) {
  Metadata metadata(kRedisString, false);
  metadata.Decode(raw_value);
  auto cache_key = StringCacheKey(ns_key);
  if (metadata.expire > 0) {
    storage_->Cache().Remove(cache_key);
    return;
  }
  storage_->Cache().Put(cache_key, raw_value.substr(STRING_HDR_SIZE, raw_value.size() - STRING_HDR_SIZE));
}

void String::removeCacheValue(const std::string &ns_key) { storage_->Cache().Remove(StringCacheKey(ns_key)); }

rocksdb::Status String::Append(const std::string &user_key, const std::string &value, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  std::string raw_value;
  std::string cached_value;
  rocksdb::Status s = rocksdb::Status::NotFound();
  if (TryGetCachedStringValue(storage_, ns_key, &cached_value)) {
    EncodeStringRawValue(cached_value, &raw_value);
    s = rocksdb::Status::OK();
  } else {
    s = getRawValue(ns_key, &raw_value);
  }
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    EncodeStringRawValue("", &raw_value);
  }
  raw_value.append(value);
  *ret = static_cast<int>(raw_value.size() - STRING_HDR_SIZE);
  s = updateRawValue(ns_key, raw_value);
  if (!s.ok()) return s;
  syncCacheValue(ns_key, raw_value);
  return rocksdb::Status::OK();
}

std::vector<rocksdb::Status> String::MGet(const std::vector<Slice> &keys, std::vector<std::string> *values) {
  std::vector<std::string> ns_keys;
  ns_keys.reserve(keys.size());
  for (const auto &key : keys) {
    std::string ns_key;
    AppendNamespacePrefix(key, &ns_key);
    ns_keys.emplace_back(ns_key);
  }

  MultiLockGuard guard(storage_->GetLockManager(), ns_keys);
  values->assign(keys.size(), "");

  std::vector<size_t> miss_indices;
  miss_indices.reserve(keys.size());
  for (size_t i = 0; i < ns_keys.size(); ++i) {
    if (!TryGetCachedStringValue(storage_, ns_keys[i], &(*values)[i])) {
      miss_indices.emplace_back(i);
    }
  }
  if (miss_indices.empty()) {
    std::vector<rocksdb::Status> statuses(keys.size(), rocksdb::Status::OK());
    return statuses;
  }

  std::vector<std::string> missed_ns_keys;
  missed_ns_keys.reserve(miss_indices.size());
  std::vector<Slice> missed_slice_keys;
  missed_slice_keys.reserve(miss_indices.size());
  for (const auto index : miss_indices) {
    missed_ns_keys.emplace_back(ns_keys[index]);
  }
  for (const auto &ns_key : missed_ns_keys) {
    missed_slice_keys.emplace_back(ns_key);
  }

  std::vector<std::string> missed_values;
  auto missed_statuses = getValues(missed_slice_keys, &missed_values);
  std::vector<rocksdb::Status> statuses(keys.size(), rocksdb::Status::OK());
  for (size_t i = 0; i < miss_indices.size(); ++i) {
    (*values)[miss_indices[i]] = missed_values[i];
    statuses[miss_indices[i]] = missed_statuses[i];
  }
  return statuses;
}

rocksdb::Status String::Get(const std::string &user_key, std::string *value) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  auto cache_key = StringCacheKey(ns_key);
  if (storage_->Cache().TryGet(cache_key, value)) {
    return rocksdb::Status::OK();
  }
  std::string raw_value;
  auto s = getRawValue(ns_key, &raw_value);
  if (!s.ok()) return s;
  syncCacheValue(ns_key, raw_value);
  *value = raw_value.substr(STRING_HDR_SIZE, raw_value.size() - STRING_HDR_SIZE);
  return rocksdb::Status::OK();
}

rocksdb::Status String::GetEx(const std::string &user_key, std::string *value, int ttl) {
  uint32_t expire = 0;
  if (ttl > 0) {
    int64_t now = Util::GetTimeStamp();
    expire = uint32_t(now) + ttl;
  }
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = rocksdb::Status::NotFound();
  if (TryGetCachedStringValue(storage_, ns_key, value)) {
    s = rocksdb::Status::OK();
  } else {
    s = getValue(ns_key, value);
  }
  if (!s.ok()) return s;

  std::string raw_data;
  EncodeStringRawValue(*value, &raw_data, expire);
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisString);
  batch.PutLogData(log_data.Encode());
  batch.Put(metadata_cf_handle_, ns_key, raw_data);
  s = storage_->Write(storage_->DefaultWriteOptions(), &batch);
  if (!s.ok()) return s;
  removeCacheValue(ns_key);
  return rocksdb::Status::OK();
}

rocksdb::Status String::GetSet(const std::string &user_key, const std::string &new_value, std::string *old_value) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = rocksdb::Status::NotFound();
  if (TryGetCachedStringValue(storage_, ns_key, old_value)) {
    s = rocksdb::Status::OK();
  } else {
    s = getValue(ns_key, old_value);
  }
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string raw_value;
  EncodeStringRawValue(new_value, &raw_value);
  auto write_status = updateRawValue(ns_key, raw_value);
  if (write_status.ok()) {
    syncCacheValue(ns_key, raw_value);
  }
  // prev status was used to tell whether old value was empty or not
  return !write_status.ok() ? write_status : s;
}
rocksdb::Status String::GetDel(const std::string &user_key, std::string *value) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = rocksdb::Status::NotFound();
  if (TryGetCachedStringValue(storage_, ns_key, value)) {
    s = rocksdb::Status::OK();
  } else {
    s = getValue(ns_key, value);
  }
  if (!s.ok()) return s;

  s = storage_->Delete(storage_->DefaultWriteOptions(), metadata_cf_handle_, ns_key);
  if (!s.ok()) return s;
  removeCacheValue(ns_key);
  return rocksdb::Status::OK();
}

rocksdb::Status String::Set(const std::string &user_key, const std::string &value) {
  std::vector<StringPair> pairs{StringPair{user_key, value}};
  return MSet(pairs, 0);
}

rocksdb::Status String::SetEX(const std::string &user_key, const std::string &value, int ttl) {
  std::vector<StringPair> pairs{StringPair{user_key, value}};
  return MSet(pairs, ttl);
}

rocksdb::Status String::SetNX(const std::string &user_key, const std::string &value, int ttl, int *ret) {
  std::vector<StringPair> pairs{StringPair{user_key, value}};
  return MSetNX(pairs, ttl, ret);
}

rocksdb::Status String::SetXX(const std::string &user_key, const std::string &value, int ttl, int *ret) {
  *ret = 0;
  int exists = 0;
  uint32_t expire = 0;
  if (ttl > 0) {
    int64_t now = Util::GetTimeStamp();
    expire = uint32_t(now) + ttl;
  }

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  if (storage_->Cache().Contains(StringCacheKey(ns_key))) {
    exists = 1;
  } else {
    Exists({user_key}, &exists);
  }
  if (exists != 1) return rocksdb::Status::OK();

  *ret = 1;
  std::string raw_value;
  EncodeStringRawValue(value, &raw_value, expire);
  auto s = updateRawValue(ns_key, raw_value);
  if (!s.ok()) return s;
  syncCacheValue(ns_key, raw_value);
  return rocksdb::Status::OK();
}

rocksdb::Status String::SetRange(const std::string &user_key, int offset, const std::string &value, int *ret) {
  int size;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  std::string raw_value;
  std::string cached_value;
  rocksdb::Status s = rocksdb::Status::NotFound();
  if (TryGetCachedStringValue(storage_, ns_key, &cached_value)) {
    EncodeStringRawValue(cached_value, &raw_value);
    s = rocksdb::Status::OK();
  } else {
    s = getRawValue(ns_key, &raw_value);
  }
  if (!s.ok() && !s.IsNotFound()) return s;

  if (s.IsNotFound()) {
    // Return 0 directly instead of storing an empty key
    // when set nothing on a non-existing string.
    if (value.empty()) {
      *ret = 0;
      return rocksdb::Status::OK();
    }
    EncodeStringRawValue("", &raw_value);
  }
  size = static_cast<int>(raw_value.size());
  offset += STRING_HDR_SIZE;
  if (offset > size) {
    // padding the value with zero byte while offset is longer than value size
    int paddings = offset - size;
    raw_value.append(paddings, '\0');
  }
  if (offset + static_cast<int>(value.size()) >= size) {
    raw_value = raw_value.substr(0, offset);
    raw_value.append(value);
  } else {
    for (size_t i = 0; i < value.size(); i++) {
      raw_value[offset + i] = value[i];
    }
  }
  *ret = static_cast<int>(raw_value.size() - STRING_HDR_SIZE);
  s = updateRawValue(ns_key, raw_value);
  if (!s.ok()) return s;
  syncCacheValue(ns_key, raw_value);
  return rocksdb::Status::OK();
}

rocksdb::Status String::IncrBy(const std::string &user_key, int64_t increment, int64_t *ret) {
  std::string ns_key, value;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  std::string raw_value;
  rocksdb::Status s = rocksdb::Status::NotFound();
  if (TryGetCachedStringValue(storage_, ns_key, &value)) {
    EncodeStringRawValue(value, &raw_value);
    s = rocksdb::Status::OK();
  } else {
    s = getRawValue(ns_key, &raw_value);
  }
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    EncodeStringRawValue("", &raw_value);
  }

  value = raw_value.substr(STRING_HDR_SIZE, raw_value.size() - STRING_HDR_SIZE);
  int64_t n = 0;
  if (!value.empty()) {
    auto parse_result = ParseInt<int64_t>(value, 10);
    if (!parse_result) {
      return rocksdb::Status::InvalidArgument("value is not an integer or out of range");
    }
    if (isspace(value[0])) {
      return rocksdb::Status::InvalidArgument("value is not an integer");
    }
    n = *parse_result;
  }
  if ((increment < 0 && n <= 0 && increment < (LLONG_MIN - n)) ||
      (increment > 0 && n >= 0 && increment > (LLONG_MAX - n))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }
  n += increment;
  if (ret != nullptr) {
    *ret = n;
  }

  raw_value = raw_value.substr(0, STRING_HDR_SIZE);
  raw_value.append(std::to_string(n));
  s = updateRawValue(ns_key, raw_value);
  if (!s.ok()) return s;
  syncCacheValue(ns_key, raw_value);
  if (ret != nullptr) {
    *ret = n;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status String::IncrByFloat(const std::string &user_key, double increment, double *ret) {
  std::string ns_key, value;
  AppendNamespacePrefix(user_key, &ns_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  std::string raw_value;
  rocksdb::Status s = rocksdb::Status::NotFound();
  if (TryGetCachedStringValue(storage_, ns_key, &value)) {
    EncodeStringRawValue(value, &raw_value);
    s = rocksdb::Status::OK();
  } else {
    s = getRawValue(ns_key, &raw_value);
  }
  if (!s.ok() && !s.IsNotFound()) return s;

  if (s.IsNotFound()) {
    EncodeStringRawValue("", &raw_value);
  }
  value = raw_value.substr(STRING_HDR_SIZE, raw_value.size() - STRING_HDR_SIZE);
  double n = 0;
  std::size_t idx;
  if (!value.empty()) {
    try {
      n = std::stod(value, &idx);
    } catch (std::exception &e) {
      return rocksdb::Status::InvalidArgument("value is not an float");
    }
    if (isspace(value[0]) || idx != value.size()) {
      return rocksdb::Status::InvalidArgument("value is not an float");
    }
  }

  n += increment;
  if (std::isinf(n) || std::isnan(n)) {
    return rocksdb::Status::InvalidArgument("increment would produce NaN or Infinity");
  }
  *ret = n;

  raw_value = raw_value.substr(0, STRING_HDR_SIZE);
  raw_value.append(std::to_string(n));
  s = updateRawValue(ns_key, raw_value);
  if (!s.ok()) return s;
  syncCacheValue(ns_key, raw_value);
  return rocksdb::Status::OK();
}

rocksdb::Status String::MSet(const std::vector<StringPair> &pairs, int ttl) {
  uint32_t expire = 0;
  if (ttl > 0) {
    int64_t now = Util::GetTimeStamp();
    expire = uint32_t(now) + ttl;
  }

  std::vector<std::string> ns_keys;
  ns_keys.reserve(pairs.size());
  for (const auto &pair : pairs) {
    std::string ns_key;
    AppendNamespacePrefix(pair.key, &ns_key);
    ns_keys.emplace_back(std::move(ns_key));
  }

  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisString);
  batch.PutLogData(log_data.Encode());
  for (size_t i = 0; i < pairs.size(); ++i) {
    std::string bytes;
    Metadata metadata(kRedisString, false);
    metadata.expire = expire;
    metadata.Encode(&bytes);
    bytes.append(pairs[i].value.data(), pairs[i].value.size());
    batch.Put(metadata_cf_handle_, ns_keys[i], bytes);
  }

  MultiLockGuard guard(storage_->GetLockManager(), ns_keys);
  auto s = storage_->Write(storage_->DefaultWriteOptions(), &batch);
  if (!s.ok()) return s;
  for (size_t i = 0; i < pairs.size(); ++i) {
    auto cache_key = StringCacheKey(ns_keys[i]);
    if (expire == 0) {
      storage_->Cache().Put(cache_key, pairs[i].value.ToString());
    } else {
      storage_->Cache().Remove(cache_key);
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status String::MSetNX(const std::vector<StringPair> &pairs, int ttl, int *ret) {
  *ret = 0;

  uint32_t expire = 0;
  if (ttl > 0) {
    int64_t now = Util::GetTimeStamp();
    expire = uint32_t(now) + ttl;
  }

  int exists;
  std::vector<Slice> keys;
  for (StringPair pair : pairs) {
    keys.emplace_back(pair.key);
  }
  if (Exists(keys, &exists).ok() && exists > 0) {
    return rocksdb::Status::OK();
  }

  std::string ns_key;
  for (StringPair pair : pairs) {
    AppendNamespacePrefix(pair.key, &ns_key);
    LockGuard guard(storage_->GetLockManager(), ns_key);
    if (Exists({pair.key}, &exists).ok() && exists == 1) {
      return rocksdb::Status::OK();
    }
    std::string bytes;
    Metadata metadata(kRedisString, false);
    metadata.expire = expire;
    metadata.Encode(&bytes);
    bytes.append(pair.value.data(), pair.value.size());
    rocksdb::WriteBatch batch;
    WriteBatchLogData log_data(kRedisString);
    batch.PutLogData(log_data.Encode());
    batch.Put(metadata_cf_handle_, ns_key, bytes);
    auto s = storage_->Write(storage_->DefaultWriteOptions(), &batch);
    if (!s.ok()) return s;
    if (expire == 0) {
      storage_->Cache().Put(StringCacheKey(ns_key), pair.value.ToString());
    } else {
      storage_->Cache().Remove(StringCacheKey(ns_key));
    }
  }
  *ret = 1;
  return rocksdb::Status::OK();
}

// Change the value of user_key to a new_value if the current value of the key matches old_value.
// ret will be:
//  1 if the operation is successful
//  -1 if the user_key does not exist
//  0 if the operation fails
rocksdb::Status String::CAS(const std::string &user_key, const std::string &old_value, const std::string &new_value,
                            int ttl, int *ret) {
  *ret = 0;

  std::string ns_key, current_value;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = rocksdb::Status::NotFound();
  if (TryGetCachedStringValue(storage_, ns_key, &current_value)) {
    s = rocksdb::Status::OK();
  } else {
    s = getValue(ns_key, &current_value);
  }

  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound()) {
    *ret = -1;
    return rocksdb::Status::OK();
  }

  if (old_value == current_value) {
    std::string raw_value;
    uint32_t expire = 0;
    if (ttl > 0) {
      int64_t now = Util::GetTimeStamp();
      expire = uint32_t(now) + ttl;
    }
    EncodeStringRawValue(new_value, &raw_value, expire);
    auto write_status = updateRawValue(ns_key, raw_value);
    if (!write_status.ok()) {
      return write_status;
    }
    syncCacheValue(ns_key, raw_value);
    *ret = 1;
  }

  return rocksdb::Status::OK();
}

// Delete a specified user_key if the current value of the user_key matches a specified value.
// For ret, same as CAS.
rocksdb::Status String::CAD(const std::string &user_key, const std::string &value, int *ret) {
  *ret = 0;

  std::string ns_key, current_value;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = rocksdb::Status::NotFound();
  if (TryGetCachedStringValue(storage_, ns_key, &current_value)) {
    s = rocksdb::Status::OK();
  } else {
    s = getValue(ns_key, &current_value);
  }

  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound()) {
    *ret = -1;
    return rocksdb::Status::OK();
  }

  if (value == current_value) {
    auto delete_status = storage_->Delete(storage_->DefaultWriteOptions(),
                                          storage_->GetCFHandle(Engine::kMetadataColumnFamilyName), ns_key);
    if (!delete_status.ok()) {
      return delete_status;
    }
    removeCacheValue(ns_key);
    *ret = 1;
  }

  return rocksdb::Status::OK();
}

}  // namespace Redis
