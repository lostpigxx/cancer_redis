#include "engine/db_engine.h"

#include <memory>
#include <utility>

#include "engine/key_codec.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/write_batch.h"

namespace minikv {

namespace {

constexpr char kMetaCF[] = "meta";
constexpr char kHashCF[] = "hash";

rocksdb::Options BaseOptions() {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  return options;
}

rocksdb::ColumnFamilyOptions HashCFOptions() {
  rocksdb::ColumnFamilyOptions options;
  options.optimize_filters_for_hits = true;
  return options;
}

}  // namespace

DBEngine::DBEngine() = default;

DBEngine::~DBEngine() {
  for (auto* handle : handles_) {
    delete handle;
  }
  delete db_;
}

rocksdb::ColumnFamilyHandle* DBEngine::FindHandle(
    const std::string& name) const {
  for (auto* handle : handles_) {
    if (handle->GetName() == name) {
      return handle;
    }
  }
  return nullptr;
}

rocksdb::Status DBEngine::Open(const Config& config) {
  return OpenWithColumnFamilies(config);
}

rocksdb::Status DBEngine::OpenWithColumnFamilies(const Config& config) {
  rocksdb::Options options = BaseOptions();
  std::vector<std::string> cf_names;
  rocksdb::Status status =
      rocksdb::DB::ListColumnFamilies(options, config.db_path, &cf_names);
  if (!status.ok() && !status.IsIOError() && !status.IsNotFound()) {
    return status;
  }

  if (!status.ok()) {
    cf_names = {rocksdb::kDefaultColumnFamilyName, kMetaCF, kHashCF};
  } else {
    bool has_meta = false;
    bool has_hash = false;
    for (const auto& name : cf_names) {
      has_meta = has_meta || name == kMetaCF;
      has_hash = has_hash || name == kHashCF;
    }
    if (!has_meta) {
      cf_names.push_back(kMetaCF);
    }
    if (!has_hash) {
      cf_names.push_back(kHashCF);
    }
  }

  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  descriptors.reserve(cf_names.size());
  for (const auto& name : cf_names) {
    if (name == kHashCF) {
      descriptors.emplace_back(name, HashCFOptions());
    } else {
      descriptors.emplace_back(name, rocksdb::ColumnFamilyOptions());
    }
  }

  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::DB* db = nullptr;
  status = rocksdb::DB::Open(rocksdb::DBOptions(options), config.db_path,
                             descriptors, &handles, &db);
  if (!status.ok()) {
    for (auto* handle : handles) {
      delete handle;
    }
    return status;
  }

  db_ = db;
  handles_ = std::move(handles);
  default_cf_ = FindHandle(rocksdb::kDefaultColumnFamilyName);
  meta_cf_ = FindHandle(kMetaCF);
  hash_cf_ = FindHandle(kHashCF);
  if (meta_cf_ == nullptr || hash_cf_ == nullptr) {
    return rocksdb::Status::Corruption("required column families missing");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status DBEngine::HSet(const std::string& key, const std::string& field,
                               const std::string& value, bool* inserted) {
  KeyMetadata metadata;
  bool key_exists = false;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  rocksdb::Status status =
      db_->Get(read_options, meta_cf_, KeyCodec::EncodeMetaKey(key), &meta_value);
  if (status.ok()) {
    key_exists = true;
    if (!KeyCodec::DecodeMetaValue(meta_value, &metadata) ||
        metadata.type != ValueType::kHash) {
      return rocksdb::Status::InvalidArgument("key type mismatch");
    }
  } else if (!status.IsNotFound()) {
    return status;
  }

  if (!key_exists) {
    metadata.type = ValueType::kHash;
    metadata.encoding = ValueEncoding::kHashPlain;
    metadata.version = 1;
    metadata.size = 0;
    metadata.expire_at_ms = 0;
  }

  const std::string data_key =
      KeyCodec::EncodeHashDataKey(key, metadata.version, field);
  std::string existing_value;
  status = db_->Get(read_options, hash_cf_, data_key, &existing_value);
  bool field_exists = false;
  if (status.ok()) {
    field_exists = true;
  } else if (!status.IsNotFound()) {
    return status;
  }

  rocksdb::WriteBatch batch;
  if (!field_exists) {
    ++metadata.size;
  }
  batch.Put(meta_cf_, KeyCodec::EncodeMetaKey(key),
            KeyCodec::EncodeMetaValue(metadata));
  batch.Put(hash_cf_, data_key, value);

  status = db_->Write(rocksdb::WriteOptions(), &batch);
  if (inserted != nullptr) {
    *inserted = !field_exists;
  }
  return status;
}

rocksdb::Status DBEngine::HGetAll(const std::string& key,
                                  std::vector<FieldValue>* out) {
  out->clear();
  std::string meta_value;
  KeyMetadata metadata;
  rocksdb::Status status =
      db_->Get(rocksdb::ReadOptions(), meta_cf_, KeyCodec::EncodeMetaKey(key),
               &meta_value);
  if (status.IsNotFound()) {
    return rocksdb::Status::OK();
  }
  if (!status.ok()) {
    return status;
  }
  if (!KeyCodec::DecodeMetaValue(meta_value, &metadata) ||
      metadata.type != ValueType::kHash) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }

  const std::string prefix = KeyCodec::EncodeHashDataPrefix(key, metadata.version);
  rocksdb::ReadOptions read_options;
  std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(read_options, hash_cf_));
  for (iter->Seek(prefix); iter->Valid() && KeyCodec::StartsWith(iter->key(), prefix);
       iter->Next()) {
    std::string field;
    if (!KeyCodec::ExtractFieldFromHashDataKey(iter->key(), prefix, &field)) {
      break;
    }
    out->push_back(FieldValue{field, iter->value().ToString()});
  }
  return iter->status();
}

rocksdb::Status DBEngine::HDel(const std::string& key,
                               const std::vector<std::string>& fields,
                               uint64_t* deleted) {
  if (deleted != nullptr) {
    *deleted = 0;
  }
  std::string meta_value;
  KeyMetadata metadata;
  rocksdb::Status status =
      db_->Get(rocksdb::ReadOptions(), meta_cf_, KeyCodec::EncodeMetaKey(key),
               &meta_value);
  if (status.IsNotFound()) {
    return rocksdb::Status::OK();
  }
  if (!status.ok()) {
    return status;
  }
  if (!KeyCodec::DecodeMetaValue(meta_value, &metadata) ||
      metadata.type != ValueType::kHash) {
    return rocksdb::Status::InvalidArgument("key type mismatch");
  }

  uint64_t removed = 0;
  rocksdb::WriteBatch batch;
  std::string scratch;
  for (const auto& field : fields) {
    const std::string data_key =
        KeyCodec::EncodeHashDataKey(key, metadata.version, field);
    status = db_->Get(rocksdb::ReadOptions(), hash_cf_, data_key, &scratch);
    if (status.ok()) {
      batch.Delete(hash_cf_, data_key);
      ++removed;
    } else if (!status.IsNotFound()) {
      return status;
    }
  }

  if (removed == 0) {
    return rocksdb::Status::OK();
  }

  if (removed >= metadata.size) {
    batch.Delete(meta_cf_, KeyCodec::EncodeMetaKey(key));
  } else {
    metadata.size -= removed;
    batch.Put(meta_cf_, KeyCodec::EncodeMetaKey(key),
              KeyCodec::EncodeMetaValue(metadata));
  }

  status = db_->Write(rocksdb::WriteOptions(), &batch);
  if (deleted != nullptr) {
    *deleted = removed;
  }
  return status;
}

}  // namespace minikv
