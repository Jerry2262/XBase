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

#include "redis_hash.h"

#include <rocksdb/status.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <limits>
#include <utility>

#include "db_util.h"
#include "parse_util.h"

namespace Redis {

namespace {

std::string EncodeHashFieldValues(const std::vector<FieldValue> &field_values) {
  std::string encoded;
  PutFixed32(&encoded, static_cast<uint32_t>(field_values.size()));
  for (const auto &fv : field_values) {
    PutFixed32(&encoded, static_cast<uint32_t>(fv.field.size()));
    encoded.append(fv.field);
    PutFixed32(&encoded, static_cast<uint32_t>(fv.value.size()));
    encoded.append(fv.value);
  }
  return encoded;
}

bool DecodeHashFieldValues(const std::string &encoded, std::vector<FieldValue> *field_values) {
  field_values->clear();

  rocksdb::Slice input(encoded);
  uint32_t size = 0;
  if (!GetFixed32(&input, &size)) {
    return false;
  }

  field_values->reserve(size);
  for (uint32_t i = 0; i < size; ++i) {
    uint32_t field_size = 0;
    uint32_t value_size = 0;
    if (!GetFixed32(&input, &field_size) || input.size() < field_size) {
      field_values->clear();
      return false;
    }
    FieldValue fv;
    fv.field.assign(input.data(), field_size);
    input.remove_prefix(field_size);
    if (!GetFixed32(&input, &value_size) || input.size() < value_size) {
      field_values->clear();
      return false;
    }
    fv.value.assign(input.data(), value_size);
    input.remove_prefix(value_size);
    field_values->emplace_back(std::move(fv));
  }
  return input.empty();
}

bool TryGetCachedHashAllValues(Engine::Storage *storage, const std::string &ns_key,
                               std::vector<FieldValue> *field_values) {
  std::string encoded;
  if (!storage->Cache().TryGet(HashAllCacheKey(ns_key), &encoded)) {
    return false;
  }
  if (!DecodeHashFieldValues(encoded, field_values)) {
    storage->Cache().Remove(HashAllCacheKey(ns_key));
    return false;
  }
  return true;
}

}  // namespace

rocksdb::Status Hash::GetMetadata(const Slice &ns_key, HashMetadata *metadata) {
  return Database::GetMetadata(kRedisHash, ns_key, metadata);
}

rocksdb::Status Hash::Size(const Slice &user_key, uint32_t *ret) {
  *ret = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s;
  *ret = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Get(const Slice &user_key, const Slice &field, std::string *value) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  auto cache_key = HashCacheKey(ns_key, field);
  if (storage_->Cache().TryGet(cache_key, value)) {
    return rocksdb::Status::OK();
  }
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s;
  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key;
  InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  s = db_->Get(read_options, sub_key, value);
  if (s.ok() && metadata.expire == 0) {
    storage_->Cache().Put(cache_key, *value);
  }
  return s;
}

rocksdb::Status Hash::IncrBy(const Slice &user_key, const Slice &field, int64_t increment, int64_t *ret) {
  bool exists = false;
  int64_t old_value = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  const bool cacheable = metadata.expire == 0;

  std::string sub_key;
  InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  if (s.ok()) {
    std::string value_bytes;
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      auto parse_result = ParseInt<int64_t>(value_bytes, 10);
      if (!parse_result) {
        return rocksdb::Status::InvalidArgument(parse_result.Msg());
      }
      if (isspace(value_bytes[0])) {
        return rocksdb::Status::InvalidArgument("value is not an integer");
      }
      old_value = *parse_result;
      exists = true;
    }
  }
  if ((increment < 0 && old_value < 0 && increment < (LLONG_MIN - old_value)) ||
      (increment > 0 && old_value > 0 && increment > (LLONG_MAX - old_value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }

  *ret = old_value + increment;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  batch.Put(sub_key, std::to_string(*ret));
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  s = storage_->Write(storage_->DefaultWriteOptions(), &batch);
  if (s.ok()) {
    if (cacheable) {
      storage_->Cache().Put(HashCacheKey(ns_key, field), std::to_string(*ret));
    } else {
      storage_->Cache().Remove(HashCacheKey(ns_key, field));
    }
    storage_->Cache().Remove(HashAllCacheKey(ns_key));
  }
  return s;
}

rocksdb::Status Hash::IncrByFloat(const Slice &user_key, const Slice &field, double increment, double *ret) {
  bool exists = false;
  float old_value = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  const bool cacheable = metadata.expire == 0;

  std::string sub_key;
  InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
  if (s.ok()) {
    std::string value_bytes;
    std::size_t idx = 0;
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      try {
        old_value = std::stod(value_bytes, &idx);
      } catch (std::exception &e) {
        return rocksdb::Status::InvalidArgument(e.what());
      }
      if (isspace(value_bytes[0]) || idx != value_bytes.size()) {
        return rocksdb::Status::InvalidArgument("value is not an float");
      }
      exists = true;
    }
  }
  double n = old_value + increment;
  if (std::isinf(n) || std::isnan(n)) {
    return rocksdb::Status::InvalidArgument("increment would produce NaN or Infinity");
  }

  *ret = n;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  batch.Put(sub_key, std::to_string(*ret));
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  s = storage_->Write(storage_->DefaultWriteOptions(), &batch);
  if (s.ok()) {
    if (cacheable) {
      storage_->Cache().Put(HashCacheKey(ns_key, field), std::to_string(*ret));
    } else {
      storage_->Cache().Remove(HashCacheKey(ns_key, field));
    }
    storage_->Cache().Remove(HashAllCacheKey(ns_key));
  }
  return s;
}

rocksdb::Status Hash::MGet(const Slice &user_key, const std::vector<Slice> &fields, std::vector<std::string> *values,
                           std::vector<rocksdb::Status> *statuses) {
  values->clear();
  statuses->clear();

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  values->resize(fields.size());
  statuses->resize(fields.size());

  std::vector<size_t> miss_indices;
  miss_indices.reserve(fields.size());
  std::string cache_value;
  for (size_t i = 0; i < fields.size(); ++i) {
    cache_value.clear();
    if (storage_->Cache().TryGet(HashCacheKey(ns_key, fields[i]), &cache_value)) {
      (*values)[i] = cache_value;
      (*statuses)[i] = rocksdb::Status::OK();
    } else {
      miss_indices.emplace_back(i);
    }
  }
  if (miss_indices.empty()) {
    return rocksdb::Status::OK();
  }

  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }
  const bool cacheable = metadata.expire == 0;

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key, value;
  for (const auto index : miss_indices) {
    const auto &field = fields[index];
    const auto cache_key = HashCacheKey(ns_key, field);
    InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
    value.clear();
    auto field_status = db_->Get(read_options, sub_key, &value);
    if (!field_status.ok() && !field_status.IsNotFound()) return field_status;
    if (field_status.ok() && cacheable) {
      storage_->Cache().Put(cache_key, value);
    }
    (*values)[index] = value;
    (*statuses)[index] = field_status;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Set(const Slice &user_key, const Slice &field, const Slice &value, int *ret) {
  FieldValue fv = {field.ToString(), value.ToString()};
  std::vector<FieldValue> fvs;
  fvs.emplace_back(std::move(fv));
  return MSet(user_key, fvs, false, ret);
}

rocksdb::Status Hash::SetNX(const Slice &user_key, const Slice &field, Slice value, int *ret) {
  FieldValue fv = {field.ToString(), value.ToString()};
  std::vector<FieldValue> fvs;
  fvs.emplace_back(std::move(fv));
  return MSet(user_key, fvs, true, ret);
}

rocksdb::Status Hash::Delete(const Slice &user_key, const std::vector<Slice> &fields, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  HashMetadata metadata(false);
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string sub_key, value;
  std::vector<std::string> removed_cache_keys;
  for (const auto &field : fields) {
    InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value);
    if (s.ok()) {
      *ret += 1;
      batch.Delete(sub_key);
      removed_cache_keys.emplace_back(HashCacheKey(ns_key, field));
    }
  }
  if (*ret == 0) {
    return rocksdb::Status::OK();
  }
  metadata.size -= *ret;
  std::string bytes;
  metadata.Encode(&bytes);
  batch.Put(metadata_cf_handle_, ns_key, bytes);
  s = storage_->Write(storage_->DefaultWriteOptions(), &batch);
  if (!s.ok()) return s;
  for (const auto &cache_key : removed_cache_keys) {
    storage_->Cache().Remove(cache_key);
  }
  storage_->Cache().Remove(HashAllCacheKey(ns_key));
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::MSet(const Slice &user_key, const std::vector<FieldValue> &field_values, bool nx, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  const bool cacheable = metadata.expire == 0;

  int added = 0;
  bool exists = false;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  std::vector<std::pair<std::string, std::string>> cache_updates;
  for (const auto &fv : field_values) {
    exists = false;
    std::string sub_key;
    InternalKey(ns_key, fv.field, metadata.version, storage_->IsSlotIdEncoded()).Encode(&sub_key);
    if (metadata.size > 0) {
      std::string fieldValue;
      s = db_->Get(rocksdb::ReadOptions(), sub_key, &fieldValue);
      if (!s.ok() && !s.IsNotFound()) return s;
      if (s.ok()) {
        if (((fieldValue == fv.value) || nx)) continue;
        exists = true;
      }
    }
    if (!exists) added++;
    batch.Put(sub_key, fv.value);
    if (cacheable) {
      cache_updates.emplace_back(HashCacheKey(ns_key, fv.field), fv.value);
    }
  }
  if (added > 0) {
    *ret = added;
    metadata.size += added;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  s = storage_->Write(storage_->DefaultWriteOptions(), &batch);
  if (!s.ok()) return s;
  for (const auto &entry : cache_updates) {
    storage_->Cache().Put(entry.first, entry.second);
  }
  if (!cacheable) {
    for (const auto &fv : field_values) {
      storage_->Cache().Remove(HashCacheKey(ns_key, fv.field));
    }
  }
  storage_->Cache().Remove(HashAllCacheKey(ns_key));
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Range(const Slice &user_key, const Slice &start, const Slice &stop, int64_t limit,
                            std::vector<FieldValue> *field_values) {
  field_values->clear();
  if (start.compare(stop) >= 0 || limit <= 0) {
    return rocksdb::Status::OK();
  }
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  limit = std::min(static_cast<int64_t>(metadata.size), limit);
  std::string start_key, stop_key;
  InternalKey(ns_key, start, metadata.version, storage_->IsSlotIdEncoded()).Encode(&start_key);
  InternalKey(ns_key, stop, metadata.version, storage_->IsSlotIdEncoded()).Encode(&stop_key);
  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(stop_key);
  read_options.iterate_upper_bound = &upper_bound;
  read_options.fill_cache = false;

  auto iter = DBUtil::UniqueIterator(db_, read_options);
  iter->Seek(start_key);
  for (int64_t i = 0; iter->Valid() && i <= limit - 1; ++i) {
    FieldValue tmp_field_value;
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    tmp_field_value.field = ikey.GetSubKey().ToString();
    tmp_field_value.value = iter->value().ToString();
    field_values->emplace_back(tmp_field_value);
    iter->Next();
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::GetAll(const Slice &user_key, std::vector<FieldValue> *field_values, HashFetchType type) {
  field_values->clear();

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);
  std::vector<FieldValue> cached_field_values;
  if (TryGetCachedHashAllValues(storage_, ns_key, &cached_field_values)) {
    field_values->reserve(cached_field_values.size());
    for (const auto &entry : cached_field_values) {
      FieldValue fv;
      if (type != HashFetchType::kOnlyValue) {
        fv.field = entry.field;
      }
      if (type != HashFetchType::kOnlyKey) {
        fv.value = entry.value;
      }
      field_values->emplace_back(std::move(fv));
    }
    return rocksdb::Status::OK();
  }

  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  const bool cacheable = metadata.expire == 0;

  std::string prefix_key, next_version_prefix_key;
  InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode(&prefix_key);
  InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode(&next_version_prefix_key);

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  read_options.fill_cache = false;

  auto iter = DBUtil::UniqueIterator(db_, read_options);
  std::vector<FieldValue> entries;
  for (iter->Seek(prefix_key); iter->Valid() && iter->key().starts_with(prefix_key); iter->Next()) {
    FieldValue fv;
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    fv.field = ikey.GetSubKey().ToString();
    fv.value = iter->value().ToString();
    entries.emplace_back(std::move(fv));
  }

  for (auto &entry : entries) {
    const auto cache_key = HashCacheKey(ns_key, entry.field);
    std::string cached_value;
    if (storage_->Cache().TryGet(cache_key, &cached_value)) {
      entry.value = std::move(cached_value);
    } else if (cacheable) {
      storage_->Cache().Put(cache_key, entry.value);
    }
  }

  if (cacheable && !entries.empty()) {
    storage_->Cache().Put(HashAllCacheKey(ns_key), EncodeHashFieldValues(entries));
  } else {
    storage_->Cache().Remove(HashAllCacheKey(ns_key));
  }

  field_values->reserve(entries.size());
  for (const auto &entry : entries) {
    FieldValue fv;
    if (type != HashFetchType::kOnlyValue) {
      fv.field = entry.field;
    }
    if (type != HashFetchType::kOnlyKey) {
      fv.value = entry.value;
    }
    field_values->emplace_back(std::move(fv));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Scan(const Slice &user_key, const std::string &cursor, uint64_t limit,
                           const std::string &field_prefix, std::vector<std::string> *fields,
                           std::vector<std::string> *values) {
  return SubKeyScanner::Scan(kRedisHash, user_key, cursor, limit, field_prefix, fields, values);
}

}  // namespace Redis
