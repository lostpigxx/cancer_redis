# dbrepair_at_lib.py 接口说明

本文档描述 `dbrepair_at_lib.py` 的外部可用接口、数据结构、参数类型、返回值结构、运行前提和典型使用方式。测试用例只需要阅读本文档即可调用该 Python 文件，不需要阅读实现。

## 1. 模块定位与运行前提

`dbrepair_at_lib.py` 是用于 DB repair 自动化测试的辅助库，封装了以下能力：

- 连接 cfgsvr、proxy、shardsvr。
- 查询和解析 partition 路由、owner、状态。
- 构造落入指定 partition 的 hashtag。
- 写入测试 string key，并校验 repair 前后的数据一致性。
- 控制 shardsvr 进程停止和重启。
- 定位 partition 对应的 RocksDB 目录。
- 注入 CURRENT、MANIFEST、WAL、SST 文件级故障。
- 执行 `dbrepair auto <partition-id>` 并等待 partition 恢复 opened 状态。
- 执行 `flushmem` 构造 SST，并选择 live SST 文件进行损坏测试。

### 1.1 依赖

运行环境需要安装：

```bash
pip install redis
```

Python 侧依赖：

```python
import redis
import env as test_env
```

其中 `env.py` 必须和测试用例在同一 import path 下，或者能被 Python 正常 import。

### 1.2 env.py 必需配置项

`dbrepair_at_lib.py` 通过 `import env as test_env` 读取运行参数。至少需要提供以下变量：

| 变量名 | 类型 | 用途 |
|---|---:|---|
| `REDIS_HOST` | `str` | Redis/cfgsvr/proxy/shardsvr 所在主机地址。 |
| `PASSWORD` | `Optional[str]` 或 `str` | Redis 密码。无密码时通常设为 `None`。 |
| `CFGSVR_PORT` | `int` | cfgsvr 端口。 |
| `PROXY_PORT` | `int` | proxy 端口。 |
| `SHARDSVR_PORTS` | `List[int]` | 所有 shardsvr 端口列表。 |
| `EXPECTED_PARTITION_COUNT` | `int` | 期望的 partition 总数，用于启动后状态检查。 |
| `PREFERRED_TARGET_SHARDSVR_PORT` | `int` | 优先选择该 shardsvr 上的 opened partition 作为目标 partition。 |
| `START_SHARDSVR_COMMANDS` | `Dict[int, str]` | 按端口映射 shardsvr 启动命令。 |
| `BASE_PATH` | `str` | 测试环境数据根目录。 |
| `SHARDSVR_DB_SUBDIR` | `str` | shardsvr RocksDB 数据目录名，例如 `"shdsvrdb"`。 |
| `DEFAULT_WRITE_COUNT` | `int` | `write_strings()` 默认写入 key 数量。 |
| `DEFAULT_VALUE_SIZE` | `int` | `write_strings()` 默认 value payload 长度。 |
| `MIN_TARGET_WAL_GROWTH` | `int` | WAL 增长量阈值，小于该值的 WAL 不作为默认损坏候选。 |
| `WAL_MIDDLE_LOSS_GAP_COUNT` | `int` | WAL 中间丢失注入的 gap 数量。 |
| `WAL_MIDDLE_LOSS_GAP_SIZE` | `int` | WAL 单个 gap 目标大小，单位 byte。 |
| `FLUSHMEM_COMMAND_TEMPLATE` | `List[str]` | flushmem 命令模板。支持 `{partition_id}` 和 `{shard_port}` 占位符。 |
| `WAIT_SST_TIMEOUT_SEC` | `float` 或 `int` | 等待 flushmem 生成新 SST 的超时时间。 |
| `SST_PREPARE_WRITE_COUNT` | `int` | `prepare_sst_for_partition()` 默认写入 key 数量。 |
| `SST_PREPARE_VALUE_SIZE` | `int` | `prepare_sst_for_partition()` 默认 value payload 长度。 |
| `SST_DATA_BLOCK_MIN_FILE_SIZE` | `int` | `corrupt_sst_data_block_area()` 允许损坏 data block 区域的最小 SST 文件大小。 |

`START_SHARDSVR_COMMANDS` 示例：

```python
START_SHARDSVR_COMMANDS = {
    6381: "./gemini-redis-server ./conf/shdsvr1.conf",
    6382: "./gemini-redis-server ./conf/shdsvr2.conf",
}
```

`FLUSHMEM_COMMAND_TEMPLATE` 示例：

```python
FLUSHMEM_COMMAND_TEMPLATE = ["flushmem", "{partition_id}"]
```

渲染后会向目标 partition 所在 shardsvr 发送：

```text
flushmem <partition-id>
```

### 1.3 目录约定

`partition_db_dir(target)` 默认定位到：

```text
<BASE_PATH>/<SHARDSVR_DB_SUBDIR>/<partition_id>
```

例如：

```text
/data/xxx/shdsvrdb/178a1dff61c4d20b
```

该目录下通常包含 RocksDB 文件：

```text
CURRENT
MANIFEST-xxxxx
000xxx.log
000xxx.sst
000xxx.ldb
OPTIONS-xxxxx
LOCK
IDENTITY
```

## 2. 数据结构与常量

### 2.1 常量

```python
OPEN_STATES = {"opened", "open"}
CORRUPTED_STATES = {"corrupted"}
REPAIRING_STATES = {"repairing"}
```

| 常量 | 类型 | 含义 |
|---|---:|---|
| `OPEN_STATES` | `Set[str]` | 被视为正常 opened/open 的 partition 状态集合。 |
| `CORRUPTED_STATES` | `Set[str]` | 被视为 corrupted 的 partition 状态集合。 |
| `REPAIRING_STATES` | `Set[str]` | 被视为 repairing 的 partition 状态集合。当前文件中未直接使用，但保留给测试用例或后续扩展。 |

### 2.2 `Partition`

```python
@dataclass
class Partition:
    owner: str
    shard_port: int
    start: int
    end: int
    partition_id: str
    state: Optional[str] = None
```

表示一个 partition 的路由、归属和状态。

| 字段 | 类型 | 含义 |
|---|---:|---|
| `owner` | `str` | owner 地址，格式一般为 `<host>:<port>`。 |
| `shard_port` | `int` | 从 `owner` 解析出的 shardsvr 端口。 |
| `start` | `int` | partition hash range 起点，包含。 |
| `end` | `int` | partition hash range 终点，包含。 |
| `partition_id` | `str` | partition ID。 |
| `state` | `Optional[str]` | partition 状态。来自 `INFO chunksmap` 时可能为 `None`；来自 cfgsvr query partitions 时通常为 `opened`、`corrupted` 等。 |

### 2.3 `WalMeta`

```python
@dataclass
class WalMeta:
    path: str
    size: int
    mtime: float
```

表示一个 WAL 文件在某一时刻的快照元数据。

| 字段 | 类型 | 含义 |
|---|---:|---|
| `path` | `str` | WAL 文件绝对路径。 |
| `size` | `int` | 文件大小，单位 byte。 |
| `mtime` | `float` | 文件最后修改时间，来自 `os.stat().st_mtime`。 |

### 2.4 `WalInjectRecord`

```python
@dataclass
class WalInjectRecord:
    path: str
    old_size: int
    final_size: int
    segments: List[Tuple[int, int]]
```

表示一次 WAL 中间丢失注入记录。

| 字段 | 类型 | 含义 |
|---|---:|---|
| `path` | `str` | 被修改的 WAL 文件路径。 |
| `old_size` | `int` | 注入前文件大小。 |
| `final_size` | `int` | 注入后文件大小。 |
| `segments` | `List[Tuple[int, int]]` | 被删除的文件区间列表。每个元素为 `(offset, length)`，单位 byte。 |

## 3. 快速使用示例

### 3.1 初始化并检查集群状态

```python
from dbrepair_at_lib import RepairAT

at = RepairAT()
at.wait_all_shards_ping()
at.assert_all_partitions_opened()

target = at.pick_target_partition()
print(target)
```

### 3.2 构造落在目标 partition 的 key

```python
tag = at.hashtag_for(target, prefix="repair-target")
key = at.write_one_and_assert(tag=tag, key_prefix="probe", value="v1")
at.assert_key_routes_to_partition(key, target)
```

注意：`hashtag_for()` 返回值形如 `{repair-target:123}`。写入 key 时只要把这个 tag 放进 key 内，proxy 的路由就应落入目标 partition。

### 3.3 WAL 中间丢失注入测试框架

```python
target = at.pick_target_partition()
tag = at.hashtag_for(target, prefix="wal-loss")

wal_before = at.wal_snapshot(target)
expected = at.write_strings(tag=tag, key_prefix="wal-loss")
wal_after = at.wal_snapshot(target)

owners_before = at.snapshot_owners()

with at.heartbeat_disabled():
    at.kill_shardsvr(target.shard_port)
    records = at.inject_wal_middle_loss(target, before=wal_before, after=wal_after)
    at.start_shardsvr(target.shard_port)

at.wait_corrupted(target)
at.repair_and_wait_opened(target)
at.assert_owners_unchanged(owners_before)
at.assert_values_missing_or_exact(expected)
```

语义：WAL-only 数据在修复后允许丢失，但不允许读出错误值，所以使用 `assert_values_missing_or_exact()`。

### 3.4 SST 损坏注入测试框架

```python
target = at.pick_target_partition()
tag = at.hashtag_for(target, prefix="sst-tail")

expected, new_ssts = at.prepare_sst_for_partition(
    target=target,
    tag=tag,
    key_prefix="sst-tail",
)

guards = at.write_guard_strings(
    exclude_partition_id=target.partition_id,
    prefix="guard",
)
at.flushmem_partitions_except({target.partition_id})

owners_before = at.snapshot_owners()

with at.heartbeat_disabled():
    at.kill_shardsvr(target.shard_port)
    sst = at.pick_largest_live_sst(target, preferred_files=new_ssts)
    at.corrupt_sst_tail(sst)
    at.start_shardsvr(target.shard_port)

at.wait_corrupted(target)
at.repair_and_wait_opened(target)
at.assert_owners_unchanged(owners_before)
at.assert_values_exact(guards)
```

语义：guard key 写入非目标 partition，用于验证目标 partition 故障没有扩散到其他 partition。`flushmem_partitions_except({target.partition_id})` 只 flush 非目标 partition，避免目标 partition 产生额外 SST 或 compaction 干扰待损坏 SST 的选择。

## 4. `RepairAT` 构造与连接接口

### 4.1 `RepairAT.__init__()`

```python
def __init__(self) -> None
```

创建测试辅助对象，并初始化：

```python
self.cfg = self.redis_conn(test_env.CFGSVR_PORT)
self.proxy = self.redis_conn(test_env.PROXY_PORT)
```

参数：无。

返回值：无。

副作用：建立 cfgsvr 和 proxy 的 redis-py 客户端对象。

### 4.2 `redis_conn()`

```python
def redis_conn(self, port: int) -> redis.Redis
```

创建连接到指定端口的 Redis 客户端。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `port` | `int` | Redis 节点端口。 |

返回值：`redis.Redis`。

连接参数：

- host: `test_env.REDIS_HOST`
- port: `port`
- password: `test_env.PASSWORD`
- decode_responses: `True`
- socket_timeout: `5`
- socket_connect_timeout: `5`

特殊行为：将 `INFO` 命令 response callback 设置为直接返回原始 response，避免 redis-py 把 `INFO` 自动解析为 dict。

### 4.3 `shard_conn()`

```python
def shard_conn(self, port: int) -> redis.Redis
```

创建连接到指定 shardsvr 端口的 Redis 客户端。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `port` | `int` | shardsvr 端口。 |

返回值：`redis.Redis`。

内部等价于：

```python
return self.redis_conn(port)
```

### 4.4 `enable_heartbeat()`

```python
def enable_heartbeat(self) -> None
```

向 cfgsvr 发送：

```text
cfgsvr heartbeat enable
```

参数：无。

返回值：`None`。

### 4.5 `disable_heartbeat()`

```python
def disable_heartbeat(self) -> None
```

向 cfgsvr 发送：

```text
cfgsvr heartbeat disable
```

参数：无。

返回值：`None`。

### 4.6 `heartbeat_disabled()`

```python
@contextmanager
def heartbeat_disabled(self)
```

上下文管理器。进入时关闭 cfgsvr heartbeat，退出时重新开启。

参数：无。

返回值：context manager。

典型用法：

```python
with at.heartbeat_disabled():
    at.kill_shardsvr(target.shard_port)
    at.break_current_to_missing_manifest(target)
    at.start_shardsvr(target.shard_port)
```

主要用途：在停止 shardsvr 并做离线文件损坏时，避免 cfgsvr 触发 partition 迁移。

## 5. Partition 查询与状态接口

### 5.1 `parse_chunksmap()`

```python
def parse_chunksmap(self, raw: str) -> List[Partition]
```

解析 proxy `INFO chunksmap` 返回的 raw bulk string。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `raw` | `str` 或 `bytes` | `INFO chunksmap` 原始返回。 |

输入示例：

```text
# ChunksMap
172.17.0.2:6381,0,262143,c96d6ef78a54a16a
172.17.0.2:6382,262144,524287,13de029c342c5aa4
```

返回值：`List[Partition]`。

返回结构示例：

```python
[
    Partition(
        owner="172.17.0.2:6381",
        shard_port=6381,
        start=0,
        end=262143,
        partition_id="c96d6ef78a54a16a",
        state=None,
    )
]
```

失败条件：无法解析出任何 partition 时抛出 `AssertionError`。

### 5.2 `chunksmap()`

```python
def chunksmap(self) -> List[Partition]
```

向 proxy 发送：

```text
INFO chunksmap
```

并调用 `parse_chunksmap()` 解析。

参数：无。

返回值：`List[Partition]`。其中 `state` 通常为 `None`。

### 5.3 `query_partitions()`

```python
def query_partitions(self) -> List[Partition]
```

向 cfgsvr 发送：

```text
cfgsvr query partitions
```

并解析 partition owner、hash range、partition ID、state。

参数：无。

返回值：`List[Partition]`。

输入响应形态假设：

```python
[
    [
        "172.17.0.2:6381",
        ["0:262143:a8767dbb7e73b29c:opened"]
    ]
]
```

返回结构示例：

```python
[
    Partition(
        owner="172.17.0.2:6381",
        shard_port=6381,
        start=0,
        end=262143,
        partition_id="a8767dbb7e73b29c",
        state="opened",
    )
]
```

### 5.4 `get_partition()`

```python
def get_partition(self, partition_id: str) -> Partition
```

从 `query_partitions()` 结果中查找指定 partition。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `partition_id` | `str` | 目标 partition ID。 |

返回值：`Partition`。

失败条件：找不到该 partition 时抛出 `AssertionError`。

### 5.5 `assert_all_partitions_opened()`

```python
def assert_all_partitions_opened(self) -> None
```

检查：

1. `query_partitions()` 返回数量等于 `test_env.EXPECTED_PARTITION_COUNT`。
2. 每个 partition 的 `state` 都属于 `OPEN_STATES`。

参数：无。

返回值：`None`。

失败条件：数量不符合预期或存在非 opened/open 状态时抛出 `AssertionError`。

### 5.6 `wait_state_in()`

```python
def wait_state_in(
    self,
    partition_id: str,
    expected_states: Set[str],
    timeout_sec: float = 30.0,
) -> Partition
```

轮询等待指定 partition 状态进入目标状态集合。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `partition_id` | `str` | 目标 partition ID。 |
| `expected_states` | `Set[str]` | 目标状态集合。 |
| `timeout_sec` | `float` | 超时时间，单位秒。默认 `30.0`。 |

返回值：`Partition`。返回时其 `state` 一定在 `expected_states` 中。

轮询间隔：`0.2` 秒。

失败条件：超时仍未进入目标状态集合时抛出 `AssertionError`。

### 5.7 `wait_opened()`

```python
def wait_opened(self, target: Partition, timeout_sec: float = 60.0) -> Partition
```

等待 `target.partition_id` 状态进入 `OPEN_STATES`。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。使用其 `partition_id`。 |
| `timeout_sec` | `float` | 超时时间，单位秒。默认 `60.0`。 |

返回值：`Partition`。

### 5.8 `wait_corrupted()`

```python
def wait_corrupted(self, target: Partition, timeout_sec: float = 30.0) -> Partition
```

等待 `target.partition_id` 状态进入 `CORRUPTED_STATES`。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。使用其 `partition_id`。 |
| `timeout_sec` | `float` | 超时时间，单位秒。默认 `30.0`。 |

返回值：`Partition`。

### 5.9 `pick_target_partition()`

```python
def pick_target_partition(self) -> Partition
```

选择一个 opened/open partition 作为故障注入目标。

选择策略：

1. 优先选择 `test_env.PREFERRED_TARGET_SHARDSVR_PORT` 上的 opened/open partition。
2. 如果没有，则选择任意 opened/open partition。
3. 如果仍没有，则失败。

参数：无。

返回值：`Partition`。返回对象的 `state` 会从 cfgsvr 查询结果补齐。

失败条件：找不到 opened/open partition 时抛出 `AssertionError`。

### 5.10 `snapshot_owners()`

```python
def snapshot_owners(self) -> Dict[str, str]
```

获取当前 partition owner 快照。

参数：无。

返回值：`Dict[str, str]`。

结构：

```python
{
    "<partition_id>": "<owner_host>:<owner_port>",
}
```

用途：故障注入前记录 owner，repair 后用 `assert_owners_unchanged()` 验证没有发生迁移。

### 5.11 `assert_pinned()`

```python
def assert_pinned(self, target: Partition) -> None
```

检查目标 partition 当前 owner 和 shard_port 是否仍等于 `target` 中记录的值。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 故障前保存的目标 partition。 |

返回值：`None`。

失败条件：owner 或 shard_port 发生变化时抛出 `AssertionError`。

### 5.12 `assert_owners_unchanged()`

```python
def assert_owners_unchanged(self, owners_before: Dict[str, str]) -> None
```

检查所有给定 partition 的 owner 是否未变化。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `owners_before` | `Dict[str, str]` | 故障前 owner 快照，通常来自 `snapshot_owners()`。 |

返回值：`None`。

失败条件：任一 partition owner 变化时抛出 `AssertionError`。

## 6. Hashtag、路由与数据校验接口

### 6.1 `parse_whereis_hash()`

```python
def parse_whereis_hash(self, raw: str) -> int
```

从 `whereis` 命令返回中提取 hash/slot 数字。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `raw` | `str`、`bytes`、`list` 或 `tuple` | `whereis` 原始返回。 |

返回值：`int`，解析出的 hash/slot。

解析规则：正则匹配：

```text
Hash:\s*(\d+)
```

失败条件：无法匹配 `Hash: <number>` 时抛出 `AssertionError`。

### 6.2 `whereis_hash()`

```python
def whereis_hash(self, key: str) -> int
```

向 proxy 发送：

```text
whereis <key>
```

并解析返回中的 hash/slot。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `key` | `str` | 要查询路由的 key。 |

返回值：`int`。

### 6.3 `assert_key_routes_to_partition()`

```python
def assert_key_routes_to_partition(self, key: str, target: Partition) -> None
```

检查 key 的 hash/slot 是否落入目标 partition 的 `[start, end]` 区间。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `key` | `str` | 待检查 key。 |
| `target` | `Partition` | 目标 partition。 |

返回值：`None`。

失败条件：`slot < target.start` 或 `slot > target.end` 时抛出 `AssertionError`。

### 6.4 `hashtag_for()`

```python
def hashtag_for(
    self,
    partition: Partition,
    prefix: str,
    max_try: int = 200000,
) -> str
```

构造一个能路由到指定 partition 的 hashtag。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `partition` | `Partition` | 目标 partition。 |
| `prefix` | `str` | hashtag 前缀。 |
| `max_try` | `int` | 最多尝试次数。默认 `200000`。 |

返回值：`str`。

返回格式：

```text
{<prefix>:<i>}
```

示例：

```python
tag = at.hashtag_for(target, prefix="repair")
# 可能返回 "{repair:12345}"
```

失败条件：尝试 `max_try` 次仍找不到落入目标 partition 的 hashtag 时抛出 `AssertionError`。

### 6.5 `write_strings()`

```python
def write_strings(
    self,
    tag: str,
    key_prefix: str,
    count: Optional[int] = None,
    value_size: Optional[int] = None,
) -> Dict[str, str]
```

通过 proxy 写入一批 string key。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `tag` | `str` | hashtag，建议来自 `hashtag_for()`。 |
| `key_prefix` | `str` | key 前缀。 |
| `count` | `Optional[int]` | 写入 key 数量。为 `None` 时使用 `test_env.DEFAULT_WRITE_COUNT`。 |
| `value_size` | `Optional[int]` | 每个 value 的 payload 长度。为 `None` 时使用 `test_env.DEFAULT_VALUE_SIZE`。 |

写入 key 格式：

```text
<key_prefix>:<i>:<tag>
```

写入 value 格式：

```text
value:<i>:<payload>
```

其中 `payload = "x" * value_size`。

返回值：`Dict[str, str]`。

结构：

```python
{
    "<key>": "<expected_value>",
}
```

失败条件：任一 `SET` 返回值不是 `True` 时抛出 `AssertionError`。

### 6.6 `write_one_and_assert()`

```python
def write_one_and_assert(self, tag: str, key_prefix: str, value: str) -> str
```

写入一个 string key，并立即读取校验。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `tag` | `str` | hashtag。 |
| `key_prefix` | `str` | key 前缀。 |
| `value` | `str` | 写入 value。 |

写入 key 格式：

```text
<key_prefix>:<tag>
```

返回值：`str`，实际写入的 key。

失败条件：`SET` 失败或 `GET` 结果不等于 `value` 时抛出 `AssertionError`。

### 6.7 `write_guard_strings()`

```python
def write_guard_strings(
    self,
    exclude_partition_id: str,
    prefix: str,
) -> Dict[str, str]
```

给非目标 partition 写入 guard key。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `exclude_partition_id` | `str` | 排除的目标 partition ID。该 partition 不写 guard key。 |
| `prefix` | `str` | guard key 前缀。 |

返回值：`Dict[str, str]`。

结构：

```python
{
    "<guard_key>": "guard-value:<partition_id>",
}
```

用途：repair 后用 `assert_values_exact()` 检查非目标 partition 的 guard 数据仍然完整。

### 6.8 `assert_values_exact()`

```python
def assert_values_exact(self, expected: Dict[str, str]) -> None
```

强校验：所有 key 必须存在，且 value 必须完全匹配。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `expected` | `Dict[str, str]` | 期望 key/value 映射。 |

返回值：`None`。

失败条件：任一 key 不存在，或 value 不等于期望值时抛出 `AssertionError`。

### 6.9 `assert_values_missing_or_exact()`

```python
def assert_values_missing_or_exact(self, expected: Dict[str, str]) -> None
```

弱校验：key 允许不存在；但只要存在，value 必须完全匹配。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `expected` | `Dict[str, str]` | 期望 key/value 映射。 |

返回值：`None`。

输出：打印 kept/lost/total 统计。

用途：WAL 损坏修复后，WAL-only 数据允许丢失，但不能读出错误值。

失败条件：任一存在的 key 对应 value 不等于期望值时抛出 `AssertionError`。

## 7. shardsvr 进程控制接口

### 7.1 `kill_shardsvr()`

```python
def kill_shardsvr(self, port: int) -> None
```

查找监听指定端口的进程，并发送 `SIGKILL`。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `port` | `int` | shardsvr 监听端口。 |

返回值：`None`。

内部行为：

1. 调用 `_find_listen_pids(port)` 查找监听端口的 PID。
2. 对每个 PID 执行 `os.kill(pid, signal.SIGKILL)`。
3. 调用 `_wait_port_down(port, timeout_sec=10)` 等待端口不可 ping。

失败条件：

- 找不到监听进程时抛出 `AssertionError`。
- kill 后端口仍可 ping 时抛出 `AssertionError`。

### 7.2 `start_shardsvr()`

```python
def start_shardsvr(self, port: int) -> None
```

根据 `test_env.START_SHARDSVR_COMMANDS[port]` 启动指定 shardsvr。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `port` | `int` | shardsvr 端口。 |

返回值：`None`。

内部行为：

```python
subprocess.Popen(
    cmd,
    shell=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    preexec_fn=os.setsid,
)
```

随后调用：

```python
self.wait_ping(port, timeout_sec=30)
```

失败条件：

- `START_SHARDSVR_COMMANDS` 没有该端口的启动命令时抛出 `AssertionError`。
- 启动后 30 秒内不能 ping 通时抛出 `AssertionError`。

### 7.3 `wait_ping()`

```python
def wait_ping(self, port: int, timeout_sec: float = 15.0) -> None
```

等待指定 Redis 节点可以 `PING`。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `port` | `int` | 节点端口。 |
| `timeout_sec` | `float` | 超时时间，单位秒。默认 `15.0`。 |

返回值：`None`。

轮询间隔：`0.2` 秒。

失败条件：超时仍无法 ping 通时抛出 `AssertionError`，错误信息包含最后一次异常。

### 7.4 `wait_all_shards_ping()`

```python
def wait_all_shards_ping(self, timeout_sec: float = 10.0) -> None
```

依次等待 `test_env.SHARDSVR_PORTS` 中所有 shardsvr 可 ping。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `timeout_sec` | `float` | 每个 shardsvr 的 ping 超时时间。默认 `10.0`。 |

返回值：`None`。

失败条件：任一 shardsvr 超时不可 ping 时抛出 `AssertionError`。

### 7.5 `_find_listen_pids()`

```python
def _find_listen_pids(self, port: int) -> List[int]
```

内部接口。查找监听指定 TCP 端口的进程 PID。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `port` | `int` | TCP 监听端口。 |

返回值：`List[int]`，按升序排序。

查找命令优先级：

1. `lsof -tiTCP:<port> -sTCP:LISTEN`
2. `ss -ltnp | grep ':<port> ' ...`
3. `netstat -ltnp | grep ':<port> ' ...`

### 7.6 `_wait_port_down()`

```python
def _wait_port_down(self, port: int, timeout_sec: float) -> None
```

内部接口。等待指定端口不可 ping。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `port` | `int` | 节点端口。 |
| `timeout_sec` | `float` | 超时时间，单位秒。 |

返回值：`None`。

失败条件：超时后端口仍可 ping 时抛出 `AssertionError`。

## 8. DB 文件路径与通用文件故障接口

### 8.1 `partition_db_dir()`

```python
def partition_db_dir(self, target: Partition) -> str
```

返回目标 partition 的 RocksDB 目录。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |

返回值：`str`，目录路径。

路径规则：

```python
os.path.join(
    test_env.BASE_PATH,
    test_env.SHARDSVR_DB_SUBDIR,
    target.partition_id,
)
```

失败条件：目录不存在时抛出 `AssertionError`。

### 8.2 `current_file_path()`

```python
def current_file_path(self, target: Partition) -> str
```

返回目标 partition 的 `CURRENT` 文件路径。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |

返回值：`str`。

路径规则：

```text
<partition_db_dir>/CURRENT
```

### 8.3 `read_current_manifest_name()`

```python
def read_current_manifest_name(self, target: Partition) -> str
```

读取目标 partition 的 `CURRENT` 文件内容，返回其中记录的 MANIFEST 文件名。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |

返回值：`str`，例如 `"MANIFEST-000005"`。

### 8.4 `current_manifest_path()`

```python
def current_manifest_path(self, target: Partition) -> str
```

返回目标 partition 当前 MANIFEST 文件路径。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |

返回值：`str`。

路径规则：

```text
<partition_db_dir>/<manifest_name_from_CURRENT>
```

### 8.5 `delete_file()`

```python
def delete_file(self, path: str) -> None
```

删除指定文件。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `path` | `str` | 文件路径。 |

返回值：`None`。

失败条件：文件不存在时抛出 `AssertionError`。

### 8.6 `overwrite_file_middle()`

```python
def overwrite_file_middle(self, path: str, length: int = 4096) -> Tuple[int, int, int]
```

覆盖文件中间一段字节，制造 checksum/格式损坏。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `path` | `str` | 文件路径。 |
| `length` | `int` | 覆盖长度，单位 byte。默认 `4096`。 |

返回值：`Tuple[int, int, int]`。

结构：

```python
(old_size, offset, length)
```

| 返回元素 | 类型 | 含义 |
|---|---:|---|
| `old_size` | `int` | 覆盖前文件大小。 |
| `offset` | `int` | 覆盖起始 offset。 |
| `length` | `int` | 实际覆盖长度。 |

失败条件：文件大小不满足 `old_size > length * 2` 时抛出 `AssertionError`。

### 8.7 `break_current_to_missing_manifest()`

```python
def break_current_to_missing_manifest(
    self,
    target: Partition,
    manifest_name: str = "MANIFEST-999999",
) -> None
```

把目标 partition 的 `CURRENT` 文件改成指向一个不存在的 MANIFEST。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |
| `manifest_name` | `str` | 写入 CURRENT 的 MANIFEST 文件名。默认 `"MANIFEST-999999"`。 |

返回值：`None`。

用途：稳定制造 RocksDB Open 失败。

### 8.8 `delete_current_file()`

```python
def delete_current_file(self, target: Partition) -> None
```

删除目标 partition 的 `CURRENT` 文件。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |

返回值：`None`。

失败条件：`CURRENT` 不存在时抛出 `AssertionError`。

### 8.9 `delete_current_manifest_file()`

```python
def delete_current_manifest_file(self, target: Partition) -> None
```

读取 `CURRENT` 指向的 MANIFEST 文件名，并删除该 MANIFEST 文件。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |

返回值：`None`。

失败条件：MANIFEST 文件不存在时抛出 `AssertionError`。

## 9. WAL 快照与 WAL 中间丢失接口

### 9.1 `wal_snapshot()`

```python
def wal_snapshot(self, target: Partition) -> Dict[str, WalMeta]
```

扫描目标 partition RocksDB 目录下的 `*.log` 文件，生成 WAL 快照。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |

返回值：`Dict[str, WalMeta]`。

结构：

```python
{
    "<wal_path>": WalMeta(path="<wal_path>", size=<bytes>, mtime=<mtime>),
}
```

### 9.2 `print_wal_snapshot()`

```python
def print_wal_snapshot(self, title: str, snapshot: Dict[str, WalMeta]) -> None
```

打印 WAL 快照。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `title` | `str` | 打印标题。 |
| `snapshot` | `Dict[str, WalMeta]` | WAL 快照，通常来自 `wal_snapshot()`。 |

返回值：`None`。

### 9.3 `select_grown_wals()`

```python
def select_grown_wals(
    self,
    before: Dict[str, WalMeta],
    after: Dict[str, WalMeta],
) -> List[str]
```

比较写入前后的 WAL 快照，选择增长量达到阈值的 WAL 文件。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `before` | `Dict[str, WalMeta]` | 写入前 WAL 快照。 |
| `after` | `Dict[str, WalMeta]` | 写入后 WAL 快照。 |

返回值：`List[str]`，候选 WAL 文件路径列表。

选择规则：

- 如果文件是新增文件：`after_meta.size >= test_env.MIN_TARGET_WAL_GROWTH` 才选中。
- 如果文件写入前已存在：`after_meta.size - before_meta.size >= test_env.MIN_TARGET_WAL_GROWTH` 才选中。

### 9.4 `inject_wal_middle_loss()`

```python
def inject_wal_middle_loss(
    self,
    target: Partition,
    before: Dict[str, WalMeta],
    after: Dict[str, WalMeta],
) -> List[WalInjectRecord]
```

向目标 partition 的候选 WAL 文件注入“中间丢失”。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。用于错误信息。 |
| `before` | `Dict[str, WalMeta]` | 写入前 WAL 快照。 |
| `after` | `Dict[str, WalMeta]` | 写入后 WAL 快照。 |

返回值：`List[WalInjectRecord]`。

行为：

1. 调用 `select_grown_wals(before, after)` 选择候选 WAL。
2. 对每个候选 WAL 调用 `_inject_middle_loss_to_wal_file()`。
3. 打印注入记录。
4. 返回注入记录列表。

失败条件：没有 WAL 达到增长阈值时抛出 `AssertionError`，并打印 before/after 快照。

### 9.5 `print_wal_injection_records()`

```python
def print_wal_injection_records(self, records: List[WalInjectRecord]) -> None
```

打印 WAL 中间丢失注入记录。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `records` | `List[WalInjectRecord]` | 注入记录列表。 |

返回值：`None`。

### 9.6 `_inject_middle_loss_to_wal_file()`

```python
def _inject_middle_loss_to_wal_file(
    self,
    path: str,
    before_meta: Optional[WalMeta],
    after_meta: WalMeta,
) -> WalInjectRecord
```

内部接口。对单个 WAL 文件执行中间丢失注入。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `path` | `str` | WAL 文件路径。 |
| `before_meta` | `Optional[WalMeta]` | 写入前该 WAL 的元信息。新增 WAL 时为 `None`。 |
| `after_meta` | `WalMeta` | 写入后该 WAL 的元信息。当前实现不直接读取该对象字段，但保留在接口中。 |

返回值：`WalInjectRecord`。

行为：

- 优先破坏写入增长区间 `[before_meta.size, old_size)`。
- 如果增长区间小于 `test_env.MIN_TARGET_WAL_GROWTH`，退化为破坏整个 WAL 的中间区域。
- 调用 `_make_loss_segments()` 计算删除区间。
- 从后往前删除区间，避免 offset 被前一次删除影响。
- 重写 WAL 文件。

### 9.7 `_make_loss_segments()`

```python
def _make_loss_segments(
    self,
    file_size: int,
    region_start: int,
    region_end: int,
    gap_count: int,
    gap_size: int,
) -> List[Tuple[int, int]]
```

内部接口。根据文件大小和目标区域，生成要删除的 WAL 区间列表。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `file_size` | `int` | 文件总大小。 |
| `region_start` | `int` | 候选损坏区域起始 offset，包含。 |
| `region_end` | `int` | 候选损坏区域结束 offset，不包含。 |
| `gap_count` | `int` | 期望 gap 数量。 |
| `gap_size` | `int` | 期望单个 gap 大小。 |

返回值：`List[Tuple[int, int]]`。

结构：

```python
[(offset, length), ...]
```

失败条件：`region_start/region_end/file_size` 不满足 `0 <= region_start < region_end <= file_size` 时抛出 `AssertionError`。

如果区域过小，返回空列表。

## 10. flushmem 与 SST 构造接口

### 10.1 `flushmem()`

```python
def flushmem(self, target: Partition)
```

对目标 partition 所在 shardsvr 执行 flushmem 命令。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |

返回值：redis-py `execute_command()` 的原始返回，类型取决于服务端响应。通常可视为 `Any`。

命令由 `test_env.FLUSHMEM_COMMAND_TEMPLATE` 渲染：

```python
cmd = [
    item.format(
        partition_id=target.partition_id,
        shard_port=target.shard_port,
    )
    for item in test_env.FLUSHMEM_COMMAND_TEMPLATE
]
```

失败条件：`FLUSHMEM_COMMAND_TEMPLATE` 渲染后为空时抛出 `AssertionError`。

### 10.2 `flushmem_all_partitions()`

```python
def flushmem_all_partitions(self) -> None
```

对当前所有 opened/open partition 执行 `flushmem()`。

参数：无。

返回值：`None`。

用途：把所有 opened/open partition 的 memtable 数据尽量落成 SST。

### 10.3 `flushmem_partitions_except()`

```python
def flushmem_partitions_except(self, excluded_partition_ids: Set[str]) -> None
```

对除了 `excluded_partition_ids` 之外的 opened/open partition 执行 `flushmem()`。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `excluded_partition_ids` | `Set[str]` | 不执行 flushmem 的 partition ID 集合。 |

返回值：`None`。

主要用途：guard key 写入后，只 flush 非目标 partition，避免目标 partition 在选定待损坏 SST 前后又产生额外 SST 或 compaction。

### 10.4 `sst_files()`

```python
def sst_files(self, target: Partition) -> List[str]
```

返回目标 partition RocksDB 目录下所有 SST 表文件。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |

返回值：`List[str]`，按路径排序。

匹配后缀：

- `*.sst`
- `*.ldb`

### 10.5 `sst_snapshot()`

```python
def sst_snapshot(self, target: Partition) -> Dict[str, Tuple[int, float]]
```

生成目标 partition 的 SST 快照。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |

返回值：`Dict[str, Tuple[int, float]]`。

结构：

```python
{
    "<sst_path>": (<size_bytes>, <mtime>),
}
```

### 10.6 `wait_new_sst_files()`

```python
def wait_new_sst_files(
    self,
    target: Partition,
    before_snapshot: Dict[str, Tuple[int, float]],
    timeout_sec: Optional[float] = None,
) -> List[str]
```

等待目标 partition 在 flushmem 后出现新的 SST 文件。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |
| `before_snapshot` | `Dict[str, Tuple[int, float]]` | flushmem 前的 SST 快照，通常来自 `sst_snapshot()`。 |
| `timeout_sec` | `Optional[float]` | 超时时间。为 `None` 时使用 `test_env.WAIT_SST_TIMEOUT_SEC`。 |

返回值：`List[str]`，新出现的 SST 文件路径列表，按路径排序。

成功条件：

- 当前 SST 文件中出现了 `before_snapshot` 没有的路径。
- 或者 `before_snapshot` 为空且当前出现了任意 SST 文件。

失败条件：超时仍未发现新 SST 文件时抛出 `AssertionError`。

### 10.7 `prepare_sst_for_partition()`

```python
def prepare_sst_for_partition(
    self,
    target: Partition,
    tag: str,
    key_prefix: str,
    count: Optional[int] = None,
    value_size: Optional[int] = None,
) -> Tuple[Dict[str, str], List[str]]
```

在目标 partition 内主动构造 SST。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |
| `tag` | `str` | 目标 partition 的 hashtag，建议来自 `hashtag_for(target, ...)`。 |
| `key_prefix` | `str` | 写入 key 前缀。 |
| `count` | `Optional[int]` | 写入 key 数量。为 `None` 时使用 `test_env.SST_PREPARE_WRITE_COUNT`。 |
| `value_size` | `Optional[int]` | value payload 长度。为 `None` 时使用 `test_env.SST_PREPARE_VALUE_SIZE`。 |

返回值：`Tuple[Dict[str, str], List[str]]`。

结构：

```python
(expected, new_sst_files)
```

| 返回元素 | 类型 | 含义 |
|---|---:|---|
| `expected` | `Dict[str, str]` | 写入的 key/value 期望映射。 |
| `new_sst_files` | `List[str]` | flushmem 后新出现的 SST 文件列表。 |

内部流程：

1. 调用 `sst_snapshot(target)` 记录 before。
2. 调用 `write_strings()` 写入目标 partition 数据。
3. 调用 `flushmem(target)`。
4. 调用 `wait_new_sst_files()` 等待新 SST。
5. 调用 `assert_values_exact(expected)` 验证 flush 后数据仍可读。

### 10.8 `pick_largest_sst_from()`

```python
def pick_largest_sst_from(
    self,
    sst_files: List[str],
    min_size: int = 1,
) -> str
```

从给定 SST 文件列表里选择当前仍然存在、且大小满足阈值的最大文件。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `sst_files` | `List[str]` | 候选 SST 文件路径列表。 |
| `min_size` | `int` | 最小文件大小，单位 byte。默认 `1`。 |

返回值：`str`，选中的 SST 文件路径。

选择规则：

1. 过滤掉已经不存在的文件。
2. 过滤掉大小小于 `min_size` 的文件。
3. 返回剩余文件中 `os.path.getsize()` 最大的文件。

失败条件：

- `sst_files` 为空时抛出 `AssertionError`。
- 候选文件全部已经不存在时抛出 `AssertionError`。
- 没有 live 文件满足 `min_size` 时抛出 `AssertionError`。

### 10.9 `pick_largest_live_sst()`

```python
def pick_largest_live_sst(
    self,
    target: Partition,
    preferred_files: Optional[List[str]] = None,
    min_size: int = 1,
) -> str
```

选择目标 partition 当前仍然存在的最大 live SST 文件。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |
| `preferred_files` | `Optional[List[str]]` | 优先候选 SST 列表，通常来自 `prepare_sst_for_partition()` 返回的 `new_sst_files`。 |
| `min_size` | `int` | 最小文件大小，单位 byte。默认 `1`。 |

返回值：`str`，选中的 SST 文件路径。

选择策略：

1. 如果 `preferred_files` 非空，优先从其中选择仍存在且大小不小于 `min_size` 的最大文件。
2. 如果 preferred 文件都不存在或不满足大小阈值，则 fallback 到 `sst_files(target)` 当前扫描到的所有 live SST。
3. 返回最大文件。

建议调用时机：尽量在 `kill_shardsvr(target.shard_port)` 之后调用，因为 shardsvr 停止后 SST 文件集合相对稳定。

失败条件：没有任何 live SST 满足条件时抛出 `AssertionError`。

## 11. SST 故障注入接口

### 11.1 `delete_sst_file()`

```python
def delete_sst_file(self, path: str) -> None
```

删除指定 SST 文件。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `path` | `str` | SST 文件路径。 |

返回值：`None`。

失败条件：文件不存在时抛出 `AssertionError`。

### 11.2 `truncate_sst_file_to_half()`

```python
def truncate_sst_file_to_half(self, path: str) -> Tuple[int, int]
```

把指定 SST 文件截断为原来的一半。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `path` | `str` | SST 文件路径。 |

返回值：`Tuple[int, int]`。

结构：

```python
(old_size, new_size)
```

| 返回元素 | 类型 | 含义 |
|---|---:|---|
| `old_size` | `int` | 截断前文件大小。 |
| `new_size` | `int` | 截断后文件大小。 |

失败条件：`old_size <= 4096` 时抛出 `AssertionError`。

### 11.3 `zero_sst_file()`

```python
def zero_sst_file(self, path: str) -> int
```

把指定 SST 文件内容全部写成 `0x00`，保留文件名和文件大小。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `path` | `str` | SST 文件路径。 |

返回值：`int`，清零前文件大小。

失败条件：

- 文件大小为 `0` 时抛出 `AssertionError`。
- 清零后文件大小发生变化时抛出 `AssertionError`。

### 11.4 `corrupt_sst_tail()`

```python
def corrupt_sst_tail(
    self,
    path: str,
    length: int = 4096,
) -> Tuple[int, int, int]
```

覆盖 SST 文件尾部，用于模拟 footer、metaindex、index 损坏。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `path` | `str` | SST 文件路径。 |
| `length` | `int` | 目标覆盖长度，单位 byte。默认 `4096`。 |

返回值：`Tuple[int, int, int]`。

结构：

```python
(old_size, offset, actual_len)
```

| 返回元素 | 类型 | 含义 |
|---|---:|---|
| `old_size` | `int` | 覆盖前文件大小。 |
| `offset` | `int` | 覆盖起始 offset。 |
| `actual_len` | `int` | 实际覆盖长度。 |

实际覆盖长度：

```python
actual_len = min(length, old_size // 4)
```

失败条件：`old_size <= length * 2` 时抛出 `AssertionError`。

### 11.5 `corrupt_sst_data_block_area()`

```python
def corrupt_sst_data_block_area(
    self,
    path: str,
    length: int = 4096,
) -> Tuple[int, int, int]
```

覆盖 SST 文件前中部，尽量命中 data block 区域。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `path` | `str` | SST 文件路径。 |
| `length` | `int` | 目标覆盖长度，单位 byte。默认 `4096`。 |

返回值：`Tuple[int, int, int]`。

结构：

```python
(old_size, offset, actual_len)
```

| 返回元素 | 类型 | 含义 |
|---|---:|---|
| `old_size` | `int` | 覆盖前文件大小。 |
| `offset` | `int` | 覆盖起始 offset。 |
| `actual_len` | `int` | 实际覆盖长度。 |

损坏位置计算：

```python
offset = old_size // 3
actual_len = min(length, old_size // 16)
```

失败条件：

- `old_size < test_env.SST_DATA_BLOCK_MIN_FILE_SIZE`。
- `actual_len < 512`。
- `offset + actual_len >= old_size - 8192`。

注意：data block 损坏不一定在 RocksDB Open 阶段暴露。有些配置只会在 Get、Iterator 或 Compaction 读取该 block 时发现 checksum mismatch。

### 11.6 `corrupt_sst_checksum_area()`

```python
def corrupt_sst_checksum_area(self, path: str) -> Tuple[int, int, int]
```

按 RocksDB 9.2.1 block-based table 格式解析 SST footer，定位 metaindex block，并只翻转该 block trailer 中的 4 字节 checksum 区域。

版本约束：

- RocksDB 版本：`9.2.1`。
- 仅支持 block-based table SST。
- 通过 SST 末尾 magic number 判断 footer 类型，不做 48/53 字节长度猜测。
- legacy block-based table magic 使用 legacy footer，metaindex handle 直接存于 footer。
- 新版 block-based table magic 使用 53 字节 footer；读取 footer version。
- footer version `<= 5` 时，metaindex handle 直接存于 footer。
- footer version `>= 6` 时，footer 不保存 metaindex handle；从 footer 固定字段读取 `metaindex_size`，按 footer 前一段反推 metaindex block。

RocksDB block trailer 结构：

```text
1 byte compression type + 4 byte checksum
```

| 参数 | 类型 | 含义 |
|---|---:|---|
| `path` | `str` | SST 文件路径。 |

返回值：`Tuple[int, int, int]`。

结构：

```python
(old_size, offset, actual_len)
```

| 返回元素 | 类型 | 含义 |
|---|---:|---|
| `old_size` | `int` | 覆盖前文件大小。 |
| `offset` | `int` | checksum 覆盖起始 offset。 |
| `actual_len` | `int` | 实际覆盖长度，固定为 `4`。 |

失败条件：

- SST 文件过小，无法解析 RocksDB 9.2.1 footer。
- SST 文件 magic number 不是 RocksDB 9.2.1 block-based table magic。
- metaindex block handle 或由 `metaindex_size` 反推的 metaindex block 范围超出文件。
- 计算出的 checksum offset 超出文件范围。

注意：checksum 损坏通常在 RocksDB Open 读取 table block 时暴露；如果当前 RocksDB 配置不会在 Open 阶段校验对应 block，该类用例可能无法进入 `corrupted`。

### 11.7 `corrupt_sst_filter_block_area()`

```python
def corrupt_sst_filter_block_area(self, path: str) -> Tuple[int, int, int]
```

按 RocksDB 9.2.1 block-based table 格式解析 SST footer 和 metaindex，选择 metaindex 中 key 包含 `filter` 的 block handle，破坏该 handle 的编码内容，并重新计算 metaindex block trailer 中的 4 字节 checksum。

这样 metaindex block 本身仍能通过 checksum 校验，但 Open 阶段解析 filter block handle 时会读到非法 handle，更适合验证 filter metadata 损坏能否触发 `corrupted`。

版本约束同 `corrupt_sst_checksum_area()`：

- RocksDB 版本：`9.2.1`。
- 仅支持 block-based table SST。
- 通过 SST 末尾 magic number 判断 footer 类型，不做 48/53 字节长度猜测。
- legacy block-based table magic 使用 legacy footer，metaindex handle 直接存于 footer。
- 新版 block-based table magic 使用 53 字节 footer；读取 footer version。
- footer version `<= 5` 时，metaindex handle 直接存于 footer。
- footer version `>= 6` 时，footer 不保存 metaindex handle；从 footer 固定字段读取 `metaindex_size`，按 footer 前一段反推 metaindex block。

RocksDB block trailer 结构：

```text
1 byte compression type + 4 byte checksum
```

| 参数 | 类型 | 含义 |
|---|---:|---|
| `path` | `str` | SST 文件路径。 |

返回值：`Tuple[int, int, int]`。

结构：

```python
(old_size, offset, actual_len)
```

| 返回元素 | 类型 | 含义 |
|---|---:|---|
| `old_size` | `int` | 覆盖前文件大小。 |
| `offset` | `int` | filter block handle 覆盖起始 offset。 |
| `actual_len` | `int` | 实际覆盖长度，即 filter block handle 编码长度。 |

失败条件：

- SST 文件过小，无法解析 RocksDB 9.2.1 footer。
- SST 文件 magic number 不是 RocksDB 9.2.1 block-based table magic。
- metaindex block 压缩类型不是 `0`，当前 helper 无法解析压缩后的 metaindex block。
- metaindex 中找不到 key 包含 `filter` 的 block handle。
- filter block handle 超出文件范围。
- filter block handle 编码 offset 超出文件范围。

注意：如果当前 RocksDB 配置仍不会在 Open 阶段解析或校验 filter metadata，该类用例可能无法进入 `corrupted`。

### 11.8 `corrupt_sst_properties_block_area()`

```python
def corrupt_sst_properties_block_area(self, path: str) -> Tuple[int, int, int]
```

按 RocksDB 9.2.1 block-based table 格式解析 SST footer 和 metaindex，选择 metaindex 中 key 包含 `properties` 的 block handle，并翻转该 properties block trailer 中的 4 字节 checksum 区域。

版本约束同 `corrupt_sst_checksum_area()`：

- RocksDB 版本：`9.2.1`。
- 仅支持 block-based table SST。
- 通过 SST 末尾 magic number 判断 footer 类型，不做 48/53 字节长度猜测。
- legacy block-based table magic 使用 legacy footer，metaindex handle 直接存于 footer。
- 新版 block-based table magic 使用 53 字节 footer；读取 footer version。
- footer version `<= 5` 时，metaindex handle 直接存于 footer。
- footer version `>= 6` 时，footer 不保存 metaindex handle；从 footer 固定字段读取 `metaindex_size`，按 footer 前一段反推 metaindex block。

RocksDB block trailer 结构：

```text
1 byte compression type + 4 byte checksum
```

| 参数 | 类型 | 含义 |
|---|---:|---|
| `path` | `str` | SST 文件路径。 |

返回值：`Tuple[int, int, int]`。

结构：

```python
(old_size, offset, actual_len)
```

| 返回元素 | 类型 | 含义 |
|---|---:|---|
| `old_size` | `int` | 覆盖前文件大小。 |
| `offset` | `int` | properties block checksum 覆盖起始 offset。 |
| `actual_len` | `int` | 实际覆盖长度，固定为 `4`。 |

失败条件：

- SST 文件过小，无法解析 RocksDB 9.2.1 footer。
- SST 文件 magic number 不是 RocksDB 9.2.1 block-based table magic。
- metaindex block 压缩类型不是 `0`，当前 helper 无法解析压缩后的 metaindex block。
- metaindex 中找不到 key 包含 `properties` 的 block handle。
- properties block handle 超出文件范围。
- 计算出的 checksum offset 超出文件范围。

注意：properties block 损坏通常依赖 RocksDB Open 读取 table properties 时暴露；如果当前 RocksDB 配置不会在 Open 阶段校验该 block，该类用例可能无法进入 `corrupted`。

## 12. Repair 接口

### 12.1 `repair_partition()`

```python
def repair_partition(self, target: Partition)
```

向目标 partition 所属 shardsvr 发送：

```text
dbrepair auto <partition-id>
```

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。使用其 `shard_port` 和 `partition_id`。 |

返回值：redis-py `execute_command()` 的原始返回，类型取决于服务端响应。通常可视为 `Any`。

副作用：打印 dbrepair 响应。

### 12.2 `repair_and_wait_opened()`

```python
def repair_and_wait_opened(self, target: Partition, timeout_sec: float = 60.0) -> Partition
```

执行 `repair_partition(target)`，然后等待该 partition 恢复 opened/open 状态。

| 参数 | 类型 | 含义 |
|---|---:|---|
| `target` | `Partition` | 目标 partition。 |
| `timeout_sec` | `float` | 等待 opened/open 的超时时间，单位秒。默认 `60.0`。 |

返回值：`Partition`，repair 后从 cfgsvr 查询到的 partition 信息。

额外校验：

```python
assert repaired.owner == target.owner
assert repaired.shard_port == target.shard_port
```

失败条件：

- 超时未恢复 opened/open 时抛出 `AssertionError`。
- owner 或 shard_port 变化时抛出 `AssertionError`。

## 13. 推荐测试编排模式

### 13.1 Open 失败类故障

适用于 CURRENT 指向不存在 MANIFEST、删除 CURRENT、删除当前 MANIFEST 等场景。

```python
at = RepairAT()
at.assert_all_partitions_opened()
target = at.pick_target_partition()
owners_before = at.snapshot_owners()

with at.heartbeat_disabled():
    at.kill_shardsvr(target.shard_port)
    at.break_current_to_missing_manifest(target)
    at.start_shardsvr(target.shard_port)

at.wait_corrupted(target)
at.repair_and_wait_opened(target)
at.assert_owners_unchanged(owners_before)
```

可替换的故障注入动作：

```python
at.delete_current_file(target)
at.delete_current_manifest_file(target)
at.overwrite_file_middle(at.current_manifest_path(target))
```

### 13.2 WAL 损坏类故障

适用于 WAL 中间丢失或 WAL 格式损坏场景。

```python
target = at.pick_target_partition()
tag = at.hashtag_for(target, prefix="wal")

wal_before = at.wal_snapshot(target)
expected = at.write_strings(tag=tag, key_prefix="wal")
wal_after = at.wal_snapshot(target)

with at.heartbeat_disabled():
    at.kill_shardsvr(target.shard_port)
    at.inject_wal_middle_loss(target, wal_before, wal_after)
    at.start_shardsvr(target.shard_port)

at.wait_corrupted(target)
at.repair_and_wait_opened(target)
at.assert_values_missing_or_exact(expected)
```

### 13.3 SST 损坏类故障

适用于删除 SST、截断 SST、损坏 SST tail、损坏 SST data block 区域、损坏 SST checksum 区域、损坏 SST filter block 区域、损坏 SST properties block 区域等场景。

```python
target = at.pick_target_partition()
tag = at.hashtag_for(target, prefix="sst")
expected, new_ssts = at.prepare_sst_for_partition(target, tag, "sst")

with at.heartbeat_disabled():
    at.kill_shardsvr(target.shard_port)
    sst = at.pick_largest_live_sst(target, preferred_files=new_ssts)
    at.truncate_sst_file_to_half(sst)
    at.start_shardsvr(target.shard_port)

at.wait_corrupted(target)
at.repair_and_wait_opened(target)
```

可替换的 SST 故障注入动作：

```python
at.delete_sst_file(sst)
at.zero_sst_file(sst)
at.corrupt_sst_tail(sst)
at.corrupt_sst_data_block_area(sst)
at.corrupt_sst_checksum_area(sst)
at.corrupt_sst_filter_block_area(sst)
at.corrupt_sst_properties_block_area(sst)
```

## 14. 接口副作用与注意事项

### 14.1 会修改磁盘文件的接口

以下接口会直接修改或删除 RocksDB 文件：

- `delete_file()`
- `overwrite_file_middle()`
- `break_current_to_missing_manifest()`
- `delete_current_file()`
- `delete_current_manifest_file()`
- `inject_wal_middle_loss()`
- `_inject_middle_loss_to_wal_file()`
- `delete_sst_file()`
- `truncate_sst_file_to_half()`
- `zero_sst_file()`
- `corrupt_sst_tail()`
- `corrupt_sst_data_block_area()`
- `corrupt_sst_checksum_area()`
- `corrupt_sst_filter_block_area()`
- `corrupt_sst_properties_block_area()`

这些接口应只在测试环境中使用。

### 14.2 建议在 shardsvr 停止后修改文件

对 RocksDB 文件做离线损坏时，推荐流程是：

```python
with at.heartbeat_disabled():
    at.kill_shardsvr(target.shard_port)
    # 修改 RocksDB 文件
    at.start_shardsvr(target.shard_port)
```

原因：shardsvr 运行时 RocksDB 可能继续写 WAL、flush、compaction 或删除 SST，直接修改 live 文件会引入不确定性。

### 14.3 `pick_largest_sst_from()` 与 `pick_largest_live_sst()` 的区别

| 接口 | 输入 | 选择范围 | 主要用途 |
|---|---|---|---|
| `pick_largest_sst_from()` | 显式给定的 `List[str]` | 只在给定列表中选择仍存在的最大文件 | 对已有候选列表做过滤选择。 |
| `pick_largest_live_sst()` | `Partition` + 可选 preferred list | 先看 preferred list，再 fallback 到当前 partition 目录下所有 live SST | SST 可能被 compaction/purge 删除时，更适合作为测试用例入口。 |

### 14.4 `flushmem_all_partitions()` 与 `flushmem_partitions_except()` 的区别

| 接口 | 行为 | 典型用途 |
|---|---|---|
| `flushmem_all_partitions()` | flush 所有 opened/open partition | 想让所有 partition 数据尽量落盘。 |
| `flushmem_partitions_except()` | flush 除指定 partition 外的 opened/open partition | 已经为目标 partition 准备好 SST 后，只想把 guard key 落盘，避免目标 partition 继续产生 SST 变化。 |

### 14.5 数据校验强度选择

| 接口 | key 不存在 | key 存在但 value 错误 | 适用场景 |
|---|---|---|---|
| `assert_values_exact()` | 失败 | 失败 | guard key、SST 已 flush 数据、必须完整保留的数据。 |
| `assert_values_missing_or_exact()` | 允许 | 失败 | WAL-only 数据在 repair 后允许丢失，但不能返回错误值。 |
