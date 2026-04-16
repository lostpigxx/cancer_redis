#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "command/cmd.h"
#include "engine/db_engine.h"
#include "server/metrics.h"
#include "worker/key_lock_table.h"

namespace minikv {

struct WorkerTask {
  using Completion = std::function<void(CommandResponse)>;

  size_t io_thread_id = 0;
  uint64_t connection_id = 0;
  uint64_t request_seq = 0;
  std::unique_ptr<Cmd> cmd;
  Completion completion;
};

class Worker {
 public:
  Worker(DBEngine* engine, KeyLockTable* key_lock_table, size_t queue_depth,
         size_t worker_id);
  ~Worker();

  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  bool Enqueue(WorkerTask* task);
  size_t backlog() const;

 private:
  class BoundedMPSCQueue {
   public:
    explicit BoundedMPSCQueue(size_t capacity);

    bool TryEnqueue(WorkerTask* task);
    bool TryDequeue(WorkerTask** task);
    bool HasPending() const;
    size_t Backlog() const;

   private:
    struct Cell {
      std::atomic<bool> ready{false};
      WorkerTask* task = nullptr;
    };

    static size_t NormalizeCapacity(size_t capacity);

    const size_t capacity_;
    const size_t mask_;
    std::unique_ptr<Cell[]> buffer_;
    std::atomic<size_t> head_{0};
    std::atomic<size_t> tail_{0};
  };

  CommandResponse ExecuteTask(WorkerTask* task);
  void Run();

  DBEngine* engine_;
  KeyLockTable* key_lock_table_;
  BoundedMPSCQueue queue_;
  size_t worker_id_;
  std::mutex wait_mutex_;
  std::condition_variable wait_cv_;
  std::atomic<bool> stopping_{false};
  std::thread thread_;
};

class WorkerRuntime {
 public:
  using Completion = WorkerTask::Completion;

  WorkerRuntime(DBEngine* engine, KeyLockTable* key_lock_table,
                size_t worker_count, size_t max_queue_depth);
  ~WorkerRuntime() = default;

  WorkerRuntime(const WorkerRuntime&) = delete;
  WorkerRuntime& operator=(const WorkerRuntime&) = delete;

  rocksdb::Status Submit(std::unique_ptr<Cmd> cmd, Completion completion,
                         size_t io_thread_id = 0, uint64_t connection_id = 0,
                         uint64_t request_seq = 0);
  uint64_t rejected_requests() const {
    return rejected_requests_.load(std::memory_order_relaxed);
  }
  uint64_t inflight_requests() const {
    return inflight_requests_.load(std::memory_order_relaxed);
  }
  std::vector<size_t> worker_queue_depth() const;
  MetricsSnapshot GetMetricsSnapshot() const;

 private:
  std::vector<std::unique_ptr<Worker>> workers_;
  std::atomic<size_t> next_worker_{0};
  std::atomic<uint64_t> rejected_requests_{0};
  std::atomic<uint64_t> inflight_requests_{0};
};

CommandResponse ExecuteCommand(DBEngine* engine, KeyLockTable* key_lock_table,
                               Cmd* cmd);

}  // namespace minikv
