#pragma once

#include <memory>
#include <string>
#include <vector>

#include "minikv/command.h"
#include "minikv/config.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace minikv {

class DBEngine {
 public:
  DBEngine();
  ~DBEngine();

  DBEngine(const DBEngine&) = delete;
  DBEngine& operator=(const DBEngine&) = delete;

  rocksdb::Status Open(const Config& config);

  rocksdb::Status HSet(const std::string& key, const std::string& field,
                       const std::string& value, bool* inserted);
  rocksdb::Status HGetAll(const std::string& key, std::vector<FieldValue>* out);
  rocksdb::Status HDel(const std::string& key,
                       const std::vector<std::string>& fields,
                       uint64_t* deleted);

 private:
  rocksdb::Status OpenWithColumnFamilies(const Config& config);
  rocksdb::ColumnFamilyHandle* FindHandle(const std::string& name) const;

  rocksdb::DB* db_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
  rocksdb::ColumnFamilyHandle* default_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* meta_cf_ = nullptr;
  rocksdb::ColumnFamilyHandle* hash_cf_ = nullptr;
};

}  // namespace minikv
