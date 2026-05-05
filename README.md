# DBRepair AT

DBRepair AT 是一组 Redis/DFV-Redis DB repair 自动化测试辅助库和用例，用于验证单个 partition 的 RocksDB 文件损坏后，系统是否满足以下核心行为：

- cfgsvr 能把目标 partition 标记为 `corrupted`。
- 故障 partition pin 在原 shardsvr 上，不发生非预期迁移。
- 非目标 partition 不受影响，guard key 保持可读且值正确。
- `dbrepair auto <partition-id>` 能修复目标 partition，并恢复 `opened`。
- 修复后目标 partition 可以继续通过 proxy 写入和读取。

测试用例应优先复用 `RepairAT` 提供的接口，不在用例中重复实现连接、路由解析、RocksDB 文件定位、故障注入或 repair 逻辑。

## 目录结构

- `dbrepair_mode.py`：运行模式开关，选择 `local` 或 `cluster`。
- `env.py`：按运行模式加载 `env_local.py` 或 `env_cluster.py`。
- `env_local.py`：local 模式环境配置。
- `env_cluster.py`：cluster 模式环境配置。
- `dbrepair_at_lib.py`：按运行模式加载 local/cluster 版 `RepairAT`。
- `dbrepair_at_lib_local.py`：local 模式测试辅助库。
- `dbrepair_at_lib_cluster.py`：cluster/HDFS/手工拉起 shardsvr 版测试辅助库。
- `hdfs_file_backend.py`：cluster 模式下 HDFS 与本地 staging 文件同步。
- `cluster_pytest_console.py`：cluster 模式下的轻量 pytest 兼容入口。
- `sitecustomize.py`：Python 启动时自动加载；仅在 cluster 模式下注入本地 pytest shim。
- `manifest_fault_common.py`：MANIFEST 类故障用例的公共流程。
- `interface_local.md`：local 模式接口说明。
- `interface_cluster.md`：cluster 模式接口说明。
- `test_*.py`：具体故障注入与 repair 验证用例。

## 用例覆盖

### CURRENT / MANIFEST 类

| 用例 | 文件 | 覆盖场景 |
| --- | --- | --- |
| M1 | `test_m1_current_file_missing.py` | 删除目标 partition 的 `CURRENT` 文件 |
| M2 | `test_m2_current_points_to_missing_manifest.py` | `CURRENT` 指向不存在的 `MANIFEST` |
| M3 | `test_m3_manifest_head_corrupted.py` | 损坏 `MANIFEST` 文件头部 |
| M4 | `test_m4_manifest_tail_truncated.py` | 截断 `MANIFEST` 文件尾部 |
| M5 | `test_m5_manifest_middle_corrupted.py` | 损坏 `MANIFEST` 中间区域 |
| M6 | `test_m6_current_file_empty.py` | 将 `CURRENT` 清空为 0 字节 |
| M7 | `test_m7_current_random_string.py` | 将 `CURRENT` 写成随机字符串 |
| M8 | `test_m8_current_points_to_wrong_file_type.py` | `CURRENT` 指向非 MANIFEST 类型文件 |
| M9 | `test_m9_current_points_to_old_incomplete_manifest.py` | `CURRENT` 指向旧的、不完整的 MANIFEST |
| M10 | `test_m10_current_strict_parsing.py` | `CURRENT` 严格解析异常 |
| M11 | `test_m11_current_truncated_manifest_name.py` | `CURRENT` 中 MANIFEST 文件名被截断 |
| M12 | `test_m12_manifest_file_missing.py` | 删除当前 `MANIFEST` 文件 |
| M13 | `test_m13_manifest_file_empty.py` | 将当前 `MANIFEST` 截断为 0 字节 |
| M14 | `test_m14_manifest_append_garbage.py` | 向 `MANIFEST` 追加垃圾数据 |
| M15 | `test_m15_manifest_record_checksum_corrupted.py` | 损坏 `MANIFEST` record checksum |
| M16 | `test_m16_manifest_sst_metadata_tampered.py` | 使用外部工具篡改 MANIFEST 中的 SST metadata |

### WAL 类

| 用例 | 文件 | 覆盖场景 |
| --- | --- | --- |
| W3 | `test_w3_wal_middle_loss_open_fail_repair.py` | WAL 中间丢失，要求 RocksDB open fail 并 repair |

WAL 类用例需要考虑 RocksDB point-in-time recovery：WAL-only 数据在 repair 后可能合法丢失，因此目标数据通常使用“允许缺失但不允许错值”的校验方式。

### SST 类

| 用例 | 文件 | 覆盖场景 |
| --- | --- | --- |
| S1 | `test_s1_sst_file_missing.py` | 删除目标 SST 文件 |
| S2 | `test_s2_sst_file_truncated.py` | 截断目标 SST 文件 |
| S3 | `test_s3_sst_footer_corrupted.py` | 损坏 SST footer / tail 区域 |
| S5 | `test_s5_sst_file_zeroed.py` | SST 文件存在但内容清零 |
| S6 | `test_s6_sst_checksum_corrupted.py` | 损坏 SST data block trailer checksum 区域 |
| S7 | `test_s7_sst_magic_number_corrupted.py` | 损坏 SST magic number |
| S8 | `test_s8_sst_filter_block_corrupted.py` | 损坏 SST filter block 区域 |
| S9 | `test_s9_sst_index_block_corrupted.py` | 损坏 SST index block 区域 |
| S10 | `test_s10_sst_properties_metaindex_block_corrupted.py` | 损坏 SST properties / metaindex block 区域 |
| S11 | `test_s11_sst_file_unreadable.py` | SST 文件权限不可读 |
| S12 | `test_s12_sst_file_replaced_by_directory.py` | SST 文件被同名目录替代 |
| S13 | `test_s13_multiple_sst_files_missing.py` | 多个 SST 同时缺失 |
| S14 | `test_s14_multiple_sst_files_partially_corrupted.py` | 多个 SST 同时部分损坏 |
| S15 | `test_s15_complex_type_sst_corrupted.py` | 损坏复合类型数据对应的 SST |

SST 类用例必须先写入目标数据并执行 `flushmem` 构造 SST，再选择目标 partition 下的 SST 注入故障。

## 运行模式

运行模式只在 `dbrepair_mode.py` 中选择：

```python
DBREPAIR_AT_MODE = "local"
```

或：

```python
DBREPAIR_AT_MODE = "cluster"
```

测试用例文件同时服务两种模式，不应为了 local 或 cluster 单独修改 `test_*.py`。

## local 模式

local 模式适用于本地单机集群和本地 RocksDB 文件系统目录。典型节点如下：

- cfgsvr：`6378`
- proxy：`6379`
- shardsvr1：`6381`
- shardsvr2：`6382`

### local 依赖

```bash
pip install redis pytest
```

### local 配置

确认 `dbrepair_mode.py`：

```python
DBREPAIR_AT_MODE = "local"
```

确认 `env_local.py` 中的配置符合当前环境：

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

### local 执行

推荐从项目根目录执行：

```bash
python -m pytest -s test_s1_sst_file_missing.py
```

也可以运行其他单个用例：

```bash
python -m pytest -s test_m1_current_file_missing.py
python -m pytest -s test_w3_wal_middle_loss_open_fail_repair.py
```

## cluster 模式

cluster 模式适用于 cfgsvr、proxy、shardsvr 位于不同 host，RocksDB 文件位于 HDFS 的环境。该模式下 shardsvr 节点身份是 `owner = host:port`，不是单独的 port，因此不同 host 上的 shardsvr 可以使用相同端口。

cluster 模式不依赖 pip 安装的 pytest。项目会通过 `sitecustomize.py` 自动注入本地 pytest shim，支持当前用例实际使用的：

- `pytest.main()`
- `pytest.skip()`
- `pytest.xfail()`
- `pytest.mark.parametrize()`

### cluster 依赖

```bash
pip install redis
```

不要求：

```bash
pip install pytest
```

### cluster 配置

确认 `dbrepair_mode.py`：

```python
DBREPAIR_AT_MODE = "cluster"
```

确认 `env_cluster.py` 中的配置符合真实环境：

- `CFGSVR_HOST`
- `PROXY_HOST`
- `SHARDSVR_NODES`
- `PASSWORD`
- `BASE_PATH`
- `SHARDSVR_DB_SUBDIR`
- `HDFS_PARTITION_DIR_TEMPLATE`
- `HDFS_DFS_COMMAND`
- `LOCAL_STAGING_DIR`
- `KILL_SHARDSVR_COMMANDS`
- `CLUSTER_WAIT_PORT_DOWN_TIMEOUT_SEC`
- `CLUSTER_REQUIRE_PORT_DOWN_AFTER_KILL`
- `CLUSTER_START_WAIT_PING_TIMEOUT_SEC`

`KILL_SHARDSVR_COMMANDS` 建议按 `SHARDSVR_NODES` 的 `name` 配置。当多个节点使用同一个 port 时，不要用 port 作为唯一 key。

### cluster 执行约束

必须满足：

- 从项目根目录运行命令，或把项目根目录放入 `PYTHONPATH`。
- 不要使用 `python -S`，否则 Python 不会加载 `sitecustomize.py`。
- 保留交互式 stdin/stdout。
- 命令中保留 `-s`，确保手工输入路径不被输出捕获机制干扰。

推荐从项目根目录执行：

```bash
python -m pytest -s test_s1_sst_file_missing.py
```

如果不在项目根目录执行，需要显式设置 `PYTHONPATH`：

```bash
PYTHONPATH=/Users/liuyu/centos_ex/projects/VibeCoding/AT \
python -m pytest -s /Users/liuyu/centos_ex/projects/VibeCoding/AT/test_s1_sst_file_missing.py
```

### cluster 手工启动 shardsvr

cluster 模式下 `ctx.start_shardsvr(target.shard_port)` 不会自动拉起远端 shardsvr。它会：

1. 将本地 staging 中的故障文件同步回 HDFS。
2. 输出目标 host、owner、相关 partition 和 HDFS 文件。
3. 提示操作者手工启动目标 host 上的 shardsvr。
4. 等待操作者在同一终端输入 `yes`。
5. 等待目标 shardsvr `PING` 成功后继续后续断言。

提示示例：

```text
RocksDB fault injection has been completed and synced to HDFS.
Related partition(s): <partition-id>
Related file(s):
  - <hdfs-path>
Please start the shardsvr process manually.
Target host: <host>
Target owner: <host:port>
After the shardsvr process is started, type 'yes' and press Enter.
Continue after manual shardsvr start? [yes/no]:
```

看到提示后，在目标机器上启动 shardsvr，然后在运行测试的终端输入：

```text
yes
```

### cluster HDFS 文件流程

cluster 模式下 RocksDB 文件位于 HDFS，用例仍然按本地文件方式调用 `RepairAT` 接口。框架内部流程如下：

1. 从 HDFS 下载目标 partition 文件到 `LOCAL_STAGING_DIR`。
2. 用例在 staging 文件上执行删除、截断、覆盖、替换目录等故障注入。
3. `ctx.start_shardsvr(target.shard_port)` 前比较 fault window 前后的 staging 快照。
4. 将变化同步回 HDFS。
5. 等待人工启动 shardsvr 并输入 `yes`。
6. 继续 corrupted、pin、repair 和数据校验断言。

## 标准用例流程

新增破坏性 repair 用例时，优先复用现有结构：

1. 创建 `RepairAT()`，调用 `enable_heartbeat()` 和 `assert_all_partitions_opened()`。
2. 通过 `pick_target_partition()` 选择目标 partition，并用 `snapshot_owners()` 记录 owner 分布。
3. 构造落到目标 partition 的 hashtag，写入目标数据。
4. 写入非目标 partition guard key，并使用强校验。
5. 在 `with ctx.heartbeat_disabled():` 内 kill shardsvr、断言 pin、注入文件故障、start shardsvr。
6. 等待目标 partition 进入 `corrupted`，断言 owner/shard_port 不变。
7. 断言无迁移、无扩散，guard key 完整。
8. 调用 `repair_and_wait_opened()` 执行 `dbrepair auto`。
9. repair 后断言目标 partition 恢复 opened，guard key 完整，目标数据语义正确。
10. 用 `write_one_and_assert()` 确认目标 partition 可继续写入读取。

## 数据校验约定

- 非目标 partition guard key 必须使用强校验。
- 目标 partition 中明确不应被故障或 repair 破坏的数据，也应使用强校验。
- WAL-only 数据、缺失或损坏 SST 覆盖的数据、repair 可能合法丢弃的数据，应使用允许缺失但不允许错值的校验。
- 不要把错误值、旧版本复活、删除 tombstone 失效等问题视为“允许丢失”。

## 常用检查

语法检查：

```bash
python -m py_compile \
  cluster_pytest_console.py \
  sitecustomize.py \
  dbrepair_at_lib.py \
  dbrepair_at_lib_local.py \
  dbrepair_at_lib_cluster.py \
  env.py \
  env_local.py \
  env_cluster.py \
  hdfs_file_backend.py \
  manifest_fault_common.py \
  test_*.py
```

最小 smoke check：

```python
from dbrepair_at_lib import RepairAT

at = RepairAT()
at.wait_all_shards_ping()
at.assert_all_partitions_opened()
```
