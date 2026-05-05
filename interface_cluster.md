# DBRepair AT 集群模式接口说明

集群模式通过 `dbrepair_mode.py` 开启：

```python
DBREPAIR_AT_MODE = "cluster"
```

测试用例 import 不变：

```python
import env as test_env
from dbrepair_at_lib import RepairAT
```

## 1. 文件结构

- `dbrepair_mode.py`：总控，只选择 `local` 或 `cluster`。
- `env_cluster.py`：分布式环境配置。
- `dbrepair_at_lib_cluster.py`：分布式/HDFS/HA 版 `RepairAT`。
- `hdfs_file_backend.py`：HDFS 命令与本地 staging 镜像。
- `interface_cluster.md`：本文档。
- `interface_local.md`：单机模式原接口说明。

## 2. Redis 连接配置

集群模式下，6378、6379、6381、6382 可以分别运行在不同 host：

```python
CFGSVR_HOST = "cfgsvr-host"
PROXY_HOST = "proxy-host"

SHARDSVR_ENDPOINTS = {
    6381: ("shardsvr1-host", 6381),
    6382: ("shardsvr2-host", 6382),
}
```

`RepairAT.redis_conn(port)` 会按端口选择 host；`flushmem`、`dbrepair auto`、`wait_ping` 等用例接口不需要改。

## 3. HDFS RocksDB 文件配置

配置示例：

```python
BASE_PATH = "/redis/5562a7c61b7a4f209f9caa75a17fd5a8in12/rocksdbdata/db"
SHARDSVR_DB_SUBDIR = ""
HDFS_PARTITION_DIR_TEMPLATE = "{partition_id}"

HDFS_DFS_COMMAND = [
    "/usr/sbin/chroot",
    "--userspec=Ruby:Ruby",
    "/var/chroot/gemini/",
    "/opt/stream/hadoop-2.7.3/bin/hdfs",
    "dfs",
]

LOCAL_STAGING_DIR = "/tmp/dbrepair_at_hdfs_staging"
```

集群版 `RepairAT.partition_db_dir(target)` 返回的是本地 staging 目录，不是 HDFS 路径。后端会从 HDFS 同步目标 partition 文件到本地，测试用例可以继续用 `open()`、`os.path.exists()`、`glob.glob()` 操作这些路径。

如果真实 HDFS 目录名不是纯 partition id，可调整 `HDFS_PARTITION_DIR_TEMPLATE`。例如固定前缀目录可配置为 `"rocks-{partition_id}"`。

## 4. HDFS 修改模型

HDFS 不支持本地文件式原地修改，因此集群版流程是：

1. 从 HDFS `-get` partition 文件到 `LOCAL_STAGING_DIR`。
2. 用例在 staging 文件上执行删除、截断、覆盖、创建目录等故障注入。
3. `ctx.start_shardsvr(port)` 前，框架比较 fault window 前后的 staging 快照。
4. 发生变化的文件通过 `hdfs dfs -rm/-mkdir/-put` 同步回 HDFS。
5. 通过 `START_SHARDSVR_COMMANDS` ssh 到目标节点启动 shardsvr。
6. 等待 shardsvr `PING` 成功。

因此集群模式下，用例仍然写：

```python
with ctx.heartbeat_disabled():
    ctx.kill_shardsvr(target.shard_port)
    ctx.assert_pinned(target)
    ctx.delete_current_file(target)
    ctx.start_shardsvr(target.shard_port)
```

`start_shardsvr()` 在集群模式负责同步 HDFS 修改、执行远程启动命令并等待目标 shardsvr 恢复。

## 5. shardsvr kill/start 配置

集群模式必须配置 kill 命令：

```python
KILL_SHARDSVR_COMMANDS = {
    6381: ["ssh", "{host}", "pkill -9 -f 'gemini-redis-server.*6381'"],
    6382: ["ssh", "{host}", "pkill -9 -f 'gemini-redis-server.*6382'"],
}
```

关闭 HA 后，需要配置 start 命令。框架会自动 ssh 到目标 shardsvr
节点执行命令：

```python
START_SHARDSVR_COMMANDS = {
    6381: [
        "ssh",
        "{host}",
        "su - Ruby -c \"python /dbs/agent/engine/gemini/gemini_agent/db/redis/redis_manager.py start_shard\"",
    ],
    6382: [
        "ssh",
        "{host}",
        "su - Ruby -c \"python /dbs/agent/engine/gemini/gemini_agent/db/redis/redis_manager.py start_shard\"",
    ],
}
```

支持占位符：

- `{host}`：端口对应的 host。
- `{owner_host}`：同 `{host}`，保留给 owner 映射扩展。
- `{port}`：shardsvr 端口。

关闭 HA 后，建议要求 kill 后端口确实 down：

```python
CLUSTER_WAIT_PORT_DOWN_TIMEOUT_SEC = 1.0
CLUSTER_REQUIRE_PORT_DOWN_AFTER_KILL = True
CLUSTER_START_WAIT_PING_TIMEOUT_SEC = 60.0
```

如果仍使用 HA，把 `START_SHARDSVR_COMMANDS` 置空，`start_shardsvr()` 会退化为只等待 HA 自动拉起：

```python
START_SHARDSVR_COMMANDS = {}
CLUSTER_HA_WAIT_PING_TIMEOUT_SEC = 60.0
```

## 6. 已知能力差异

- `chmod_sst_file()` 默认在 HDFS 模式 skip，因为 HDFS 权限语义通常不能等价模拟本地 RocksDB 文件不可读。
- M16 外部 MANIFEST tamper 工具会收到 staging 本地路径；工具修改完成后由 `start_shardsvr()` 上传到 HDFS。
- 如果实际 HDFS partition 路径不是 `BASE_PATH/<partition-id>`，优先调整 `SHARDSVR_DB_SUBDIR` 和 `HDFS_PARTITION_DIR_TEMPLATE`。

## 7. 兼容性

单机模式完全保留原实现和原配置：

- `env_local.py`
- `dbrepair_at_lib_local.py`
- `interface_local.md`

从单机切换到集群，只应修改 `dbrepair_mode.py` 和 `env_cluster.py`。
