# 项目说明

本项目是 DB repair 自动化测试辅助库和用例集合，用于验证分布式 Redis/DFV-Redis 集群中单个 partition 的 RocksDB 文件损坏后，cfgsvr 能识别 `corrupted` 状态，partition 不发生非预期迁移，`dbrepair auto <partition-id>` 能完成修复，并且修复后的数据边界符合预期。

核心文件是 `dbrepair_at_lib.py`，配置入口是 `env.py`，接口说明在 `interface.md`，现有用例文件以 `test_<场景编号>_<故障描述>.py` 命名。

`dbrepair_at_lib.py` 封装了连接 cfgsvr、proxy、shardsvr，查询 partition 路由和状态，构造落入指定 partition 的 hashtag，写入和校验测试数据，控制 shardsvr 进程，定位 RocksDB partition 目录，注入 CURRENT、MANIFEST、WAL、SST 文件级故障，以及执行 `dbrepair auto <partition-id>` 等能力。测试用例应该优先调用该库暴露的接口，不要在用例中重复实现底层连接、路由解析、文件定位或 repair 等逻辑。

## 现有用例覆盖

- `test_m1_current_file_missing.py`：删除目标 partition 的 `CURRENT` 文件，要求重启后 open 失败并进入 `corrupted`，repair 后 `CURRENT` 重新生成，目标 SST 数据使用强校验。
- `test_m2_current_points_to_missing_manifest.py`：将 `CURRENT` 指向不存在的 `MANIFEST`，repair 后目标数据允许丢失或正确保留。
- `test_w3_wal_middle_loss_open_fail_repair.py`：对写入后增长的 WAL 注入中间丢失，要求 RocksDB Open 失败；WAL-only 数据 repair 后允许丢失，但不允许读出错误值。
- `test_s1_sst_file_missing.py`：构造目标 SST 后删除 SST 文件，repair 后目标历史数据允许丢失或正确保留。
- `test_s2_sst_file_truncated.py`：构造目标 SST 后截断 SST 文件，repair 后目标历史数据允许丢失或正确保留。
- `test_s3_sst_footer_corrupted.py`：构造目标 SST 后损坏尾部 footer 区域，repair 后目标历史数据允许丢失或正确保留。
- `test_s4_sst_data_block_corrupted.py`：构造较大 SST 后损坏 data block 区域。该损坏不一定在 RocksDB Open 阶段暴露；如果重启后仍是 opened，用例会明确失败并提示当前 RocksDB 配置不适合验证 open-fail repair 链路。

## 标准用例流程

新增破坏性 repair 用例时，优先复用现有用例的结构：

1. 创建 `RepairAT()`，调用 `enable_heartbeat()` 和 `assert_all_partitions_opened()` 确认初始状态。
2. 通过 `pick_target_partition()` 选择目标 partition，并用 `snapshot_owners()` 记录 repair 前 owner 分布。
3. 通过 `hashtag_for()` 构造落入目标 partition 的 tag，写入目标数据。SST 类用例优先使用 `prepare_sst_for_partition()` 构造可选择的目标 SST。
4. 使用 `write_guard_strings(exclude_partition_id=target.partition_id, ...)` 写入非目标 partition guard key，并用 `assert_values_exact()` 做强校验。
5. 离线文件损坏必须放在 `with ctx.heartbeat_disabled():` 内执行：先 `kill_shardsvr(target.shard_port)`，再 `assert_pinned(target)`，然后只损坏目标 partition 文件，最后 `start_shardsvr(target.shard_port)`。
6. 重启后用 `wait_corrupted()` 或 `wait_state_in()` 等待目标状态变化，校验 `owner` 和 `shard_port` 仍等于目标 partition 原值。
7. 通过 `assert_pinned()`、`assert_owners_unchanged()`、guard key 强校验和 `wait_all_shards_ping()` 验证故障没有迁移、扩散或影响其他 shardsvr。
8. 调用 `repair_and_wait_opened()` 执行 `dbrepair auto` 并等待目标 partition 恢复 opened。
9. repair 后再次校验 owner 不变、guard key 完整、目标数据语义正确，并用 `write_one_and_assert()` 确认目标 partition 可继续通过 proxy 写入读取。
10. 结束前调用 `assert_all_partitions_opened()` 做最终状态确认。

## 数据校验约定

- 非目标 partition guard key 必须使用强校验，优先调用 `assert_values_exact()`。
- 目标 partition 中明确不应被故障破坏的数据，也应该使用 `assert_values_exact()`，例如 M1 删除 `CURRENT` 后已有 SST 理论上仍应保留。
- WAL-only 数据、缺失或损坏 SST 覆盖的数据，以及 repair 行为可能合法丢弃的数据，使用 `assert_values_missing_or_exact()`：允许 key 不存在，但如果读到 value，必须等于预期值。
- 不要把错误值、旧版本复活、删除 tombstone 失效等问题简单视为“允许丢失”。这些属于数据正确性风险，应在用例中单独表达。

## 开发约定

- 保持 Python 代码兼容当前项目风格：标准库优先，函数和变量使用 snake_case，错误检查主要使用 `assert` 给出明确失败信息。
- 测试用例保持线性、显式的步骤结构。现有用例使用 `# ---------- Tn：... ----------` 注释分段，新增复杂用例建议沿用该风格。
- 修改 `dbrepair_at_lib.py` 的外部接口时，同步更新 `interface.md`，确保测试用例作者只读接口文档也能正确调用。
- `env.py` 只放运行环境配置，不放测试逻辑。不要把本地私有路径、密码或一次性调试命令硬编码进库代码。
- 文件损坏、进程停止、repair 等操作具有破坏性。新增逻辑时要明确目标 partition，并避免影响非目标 partition。
- 离线损坏 shardsvr 文件前，优先使用 `RepairAT.heartbeat_disabled()`，避免 cfgsvr heartbeat 触发 partition 迁移。
- 构造 SST 故障时，优先使用 `prepare_sst_for_partition()`、`pick_largest_sst_from()`、`pick_largest_live_sst()` 等已有辅助函数，避免误选非目标文件。
- 新增 WAL 故障时，先记录 `wal_snapshot()`，写入目标数据后再次记录快照，并只对确实增长过的 WAL 做故障注入。
- 新增需要 lower-level SST、compact、风险开关的用例时，优先读取并复用 `env.py` 中已有的 S7/S8/S9/S10 配置项。

## 运行前提

需要安装 Python 依赖：

```bash
pip install redis
```

运行前需要检查 `env.py` 中的配置，尤其是：

- `BASE_PATH`
- `REDIS_HOST`
- `PASSWORD`
- `CFGSVR_PORT`
- `PROXY_PORT`
- `SHARDSVR_PORTS`
- `EXPECTED_PARTITION_COUNT`
- `PREFERRED_TARGET_SHARDSVR_PORT`
- `START_SHARDSVR_COMMANDS`
- `FLUSHMEM_COMMAND_TEMPLATE`
- 如需 compact 或 lower-level SST 用例，配置 `COMPACT_COMMAND_TEMPLATE`

## 常用检查

项目当前没有统一测试入口。修改后至少执行 Python 语法检查：

```bash
python -m py_compile dbrepair_at_lib.py env.py
```

如果运行环境可用，可以用最小 smoke check 验证集群状态：

```python
from dbrepair_at_lib import RepairAT

at = RepairAT()
at.wait_all_shards_ping()
at.assert_all_partitions_opened()
```

单个用例可直接运行：

```bash
python -m pytest -s test_s1_sst_file_missing.py
```

## 重要文件

- `dbrepair_at_lib.py`：DB repair AT 辅助库实现。
- `interface.md`：外部接口说明，供测试用例作者使用。
- `env.py`：环境配置模板和默认参数。
- `test_m*.py`：CURRENT/MANIFEST 类故障用例。
- `test_w*.py`：WAL 类故障用例。
- `test_s*.py`：SST 类故障用例。
