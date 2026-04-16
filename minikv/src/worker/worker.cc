#include "worker/worker.h"

#include <algorithm>
#include <exception>
#include <limits>
#include <utility>

#include "common/thread_name.h"

namespace minikv {

Worker::BoundedMPSCQueue::BoundedMPSCQueue(size_t capacity)
    : capacity_(NormalizeCapacity(capacity)),
      mask_(capacity_ - 1),
      buffer_(std::make_unique<Cell[]>(capacity_)) {}

size_t Worker::BoundedMPSCQueue::NormalizeCapacity(size_t capacity) {
  size_t normalized = std::max<size_t>(1, capacity);
  size_t power_of_two = 1;
  while (power_of_two < normalized &&
         power_of_two < (std::numeric_limits<size_t>::max() >> 1)) {
    power_of_two <<= 1;
  }
  return std::max<size_t>(1, power_of_two);
}

bool Worker::BoundedMPSCQueue::TryEnqueue(WorkerTask* task) {
  size_t pos = head_.load(std::memory_order_relaxed);
  while (true) {
    const size_t tail = tail_.load(std::memory_order_acquire);
    if (pos - tail >= capacity_) {
      return false;
    }
    if (!head_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
      continue;
    }

    Cell& cell = buffer_[pos & mask_];
    cell.task = task;
    cell.ready.store(true, std::memory_order_release);
    return true;
  }
}

bool Worker::BoundedMPSCQueue::TryDequeue(WorkerTask** task) {
  const size_t pos = tail_.load(std::memory_order_relaxed);
  Cell& cell = buffer_[pos & mask_];
  if (!cell.ready.load(std::memory_order_acquire)) {
    return false;
  }

  *task = cell.task;
  cell.task = nullptr;
  cell.ready.store(false, std::memory_order_release);
  tail_.store(pos + 1, std::memory_order_release);
  return true;
}

bool Worker::BoundedMPSCQueue::HasPending() const {
  return head_.load(std::memory_order_acquire) !=
         tail_.load(std::memory_order_acquire);
}

size_t Worker::BoundedMPSCQueue::Backlog() const {
  const size_t head = head_.load(std::memory_order_acquire);
  const size_t tail = tail_.load(std::memory_order_acquire);
  return head - tail;
}

Worker::Worker(DBEngine* engine, KeyLockTable* key_lock_table,
               size_t queue_depth, size_t worker_id)
    : engine_(engine),
      key_lock_table_(key_lock_table),
      queue_(queue_depth),
      worker_id_(worker_id),
      thread_([this] { Run(); }) {}

Worker::~Worker() {
  stopping_.store(true, std::memory_order_release);
  wait_cv_.notify_one();
  if (thread_.joinable()) {
    thread_.join();
  }
}

bool Worker::Enqueue(WorkerTask* task) {
  const bool was_empty = !queue_.HasPending();
  if (!queue_.TryEnqueue(task)) {
    return false;
  }
  if (was_empty) {
    wait_cv_.notify_one();
  }
  return true;
}

size_t Worker::backlog() const { return queue_.Backlog(); }

CommandResponse ExecuteCommand(DBEngine* engine, KeyLockTable* key_lock_table,
                               Cmd* cmd) {
  KeyLockTable::Guard guard;
  if (key_lock_table != nullptr) {
    guard = key_lock_table->Acquire(cmd->RouteKey());
  }

  try {
    return cmd->Execute(engine);
  } catch (const std::exception& e) {
    return CommandResponse{rocksdb::Status::Aborted(e.what()), {}};
  } catch (...) {
    return CommandResponse{
        rocksdb::Status::Aborted("unknown worker failure"), {}};
  }
}

CommandResponse Worker::ExecuteTask(WorkerTask* task) {
  return ExecuteCommand(engine_, key_lock_table_, task->cmd.get());
}

void Worker::Run() {
  SetCurrentThreadName("minikv-w" + std::to_string(worker_id_));

  while (true) {
    WorkerTask* raw_task = nullptr;
    if (!queue_.TryDequeue(&raw_task)) {
      std::unique_lock<std::mutex> lock(wait_mutex_);
      wait_cv_.wait(lock, [&] {
        return stopping_.load(std::memory_order_acquire) || queue_.HasPending();
      });
      if (stopping_.load(std::memory_order_acquire) && !queue_.HasPending()) {
        return;
      }
      continue;
    }

    std::unique_ptr<WorkerTask> task(raw_task);
    CommandResponse response = ExecuteTask(task.get());
    task->completion(std::move(response));
  }
}

WorkerRuntime::WorkerRuntime(DBEngine* engine, KeyLockTable* key_lock_table,
                             size_t worker_count, size_t max_queue_depth) {
  const size_t normalized_worker_count = std::max<size_t>(1, worker_count);
  workers_.reserve(normalized_worker_count);
  for (size_t i = 0; i < normalized_worker_count; ++i) {
    workers_.push_back(std::make_unique<Worker>(engine, key_lock_table,
                                                max_queue_depth, i));
  }
}

rocksdb::Status WorkerRuntime::Submit(std::unique_ptr<Cmd> cmd,
                                      Completion completion,
                                      size_t io_thread_id,
                                      uint64_t connection_id,
                                      uint64_t request_seq) {
  if (cmd == nullptr) {
    return rocksdb::Status::InvalidArgument("cmd is required");
  }

  auto task = std::make_unique<WorkerTask>();
  task->io_thread_id = io_thread_id;
  task->connection_id = connection_id;
  task->request_seq = request_seq;
  task->cmd = std::move(cmd);
  task->completion = [this, completion = std::move(completion)](
                         CommandResponse response) mutable {
    inflight_requests_.fetch_sub(1, std::memory_order_relaxed);
    completion(std::move(response));
  };

  const size_t start = next_worker_.fetch_add(1, std::memory_order_relaxed);
  for (size_t offset = 0; offset < workers_.size(); ++offset) {
    Worker* worker = workers_[(start + offset) % workers_.size()].get();
    if (worker->Enqueue(task.get())) {
      inflight_requests_.fetch_add(1, std::memory_order_relaxed);
      task.release();
      return rocksdb::Status::OK();
    }
  }

  rejected_requests_.fetch_add(1, std::memory_order_relaxed);
  return rocksdb::Status::Busy("worker queue full");
}

std::vector<size_t> WorkerRuntime::worker_queue_depth() const {
  std::vector<size_t> queue_depth;
  queue_depth.reserve(workers_.size());
  for (const auto& worker : workers_) {
    queue_depth.push_back(worker->backlog());
  }
  return queue_depth;
}

MetricsSnapshot WorkerRuntime::GetMetricsSnapshot() const {
  MetricsSnapshot snapshot;
  snapshot.worker_queue_depth = worker_queue_depth();
  snapshot.worker_rejections = rejected_requests();
  snapshot.worker_inflight = inflight_requests();
  return snapshot;
}

}  // namespace minikv
