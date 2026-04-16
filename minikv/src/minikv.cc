#include "minikv/minikv.h"

#include <memory>
#include <utility>

#include "command/cmd_create.h"
#include "engine/db_engine.h"
#include "worker/key_lock_table.h"
#include "worker/worker.h"

namespace minikv {

class MiniKV::Impl {
 public:
  explicit Impl(const Config& config_value)
      : config(config_value),
        key_lock_table(
            KeyLockTable::DefaultStripeCount(config_value.worker_threads)),
        submit_runtime(std::make_unique<WorkerRuntime>(
            &engine, &key_lock_table, config_value.worker_threads,
            config_value.max_pending_requests_per_worker)) {}

  Config config;
  DBEngine engine;
  KeyLockTable key_lock_table;
  std::unique_ptr<WorkerRuntime> submit_runtime;
};

MiniKV::MiniKV(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

MiniKV::~MiniKV() = default;

rocksdb::Status MiniKV::Open(const Config& config,
                             std::unique_ptr<MiniKV>* minikv) {
  auto impl = std::unique_ptr<Impl>(new Impl(config));
  rocksdb::Status status = impl->engine.Open(config);
  if (!status.ok()) {
    return status;
  }
  minikv->reset(new MiniKV(std::move(impl)));
  return rocksdb::Status::OK();
}

CommandResponse MiniKV::Execute(const CommandRequest& request) {
  std::unique_ptr<Cmd> cmd;
  rocksdb::Status status = CreateCmd(request, &cmd);
  if (!status.ok()) {
    return CommandResponse{status, {}};
  }
  return Execute(std::move(cmd));
}

CommandResponse MiniKV::Execute(std::string name,
                                std::vector<std::string> args) {
  return Execute(CommandRequest(std::move(name), std::move(args)));
}

CommandResponse MiniKV::Execute(std::string name, std::string key,
                                std::vector<std::string> args) {
  return Execute(
      CommandRequest(std::move(name), std::move(key), std::move(args)));
}

rocksdb::Status MiniKV::Submit(const CommandRequest& request,
                               CommandCallback callback) {
  std::unique_ptr<Cmd> cmd;
  rocksdb::Status status = CreateCmd(request, &cmd);
  if (!status.ok()) {
    return status;
  }
  return Submit(std::move(cmd), std::move(callback));
}

rocksdb::Status MiniKV::Submit(std::string name, std::vector<std::string> args,
                               CommandCallback callback) {
  return Submit(CommandRequest(std::move(name), std::move(args)),
                std::move(callback));
}

rocksdb::Status MiniKV::Submit(std::string name, std::string key,
                               std::vector<std::string> args,
                               CommandCallback callback) {
  return Submit(CommandRequest(std::move(name), std::move(key), std::move(args)),
                std::move(callback));
}

rocksdb::Status MiniKV::Submit(std::unique_ptr<Cmd> cmd,
                               CommandCallback callback) {
  return impl_->submit_runtime->Submit(std::move(cmd), std::move(callback));
}

CommandResponse MiniKV::Execute(std::unique_ptr<Cmd> cmd) {
  if (cmd == nullptr) {
    return CommandResponse{rocksdb::Status::InvalidArgument("cmd is required"),
                           {}};
  }
  return ExecuteCommand(&impl_->engine, &impl_->key_lock_table, cmd.get());
}

DBEngine* MiniKV::engine() { return &impl_->engine; }

KeyLockTable* MiniKV::key_lock_table() { return &impl_->key_lock_table; }

rocksdb::Status MiniKV::HSet(const std::string& key, const std::string& field,
                             const std::string& value, bool* inserted) {
  CommandRequest request{"HSET", key, {field, value}};
  CommandResponse response = Execute(request);
  if (inserted != nullptr) {
    *inserted = response.status.ok() && response.reply.IsInteger() &&
                response.reply.integer() == 1;
  }
  return response.status;
}

rocksdb::Status MiniKV::HGetAll(const std::string& key,
                                std::vector<FieldValue>* out) {
  CommandRequest request{"HGETALL", key, {}};
  CommandResponse response = Execute(request);
  if (response.status.ok() && response.reply.IsArray()) {
    out->clear();
    const auto& values = response.reply.array();
    for (size_t i = 0; i + 1 < values.size(); i += 2) {
      out->push_back(FieldValue{values[i].string(), values[i + 1].string()});
    }
  }
  return response.status;
}

rocksdb::Status MiniKV::HDel(const std::string& key,
                             const std::vector<std::string>& fields,
                             uint64_t* deleted) {
  CommandRequest request{"HDEL", key, fields};
  CommandResponse response = Execute(request);
  if (deleted != nullptr) {
    *deleted = response.status.ok() && response.reply.IsInteger()
                   ? static_cast<uint64_t>(response.reply.integer())
                   : 0;
  }
  return response.status;
}

}  // namespace minikv
