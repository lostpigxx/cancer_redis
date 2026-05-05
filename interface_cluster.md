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

SHARDSVR_NODES = [
    {"name": "shardsvr1", "host": "shardsvr1-host", "port": 6378},
    {"name": "shardsvr2", "host": "shardsvr2-host", "port": 6378},
]
```

cluster 模式下 shardsvr 节点身份是 `owner = host:port`，不是单独的 port。
因此 shardsvr1 和 shardsvr2 可以使用相同端口，只要 host 不同即可。

`flushmem`、`dbrepair auto`、`kill_shardsvr(target.shard_port)`、`start_shardsvr(target.shard_port)` 等现有用例接口不需要改；cluster 版 `RepairAT` 会优先使用最近一次 `pick_target_partition()` 选出的 target owner 来消除同端口歧义。

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
5. 打印英文提示，列出故障已同步、相关 partition、相关 HDFS 文件和目标 host。
6. 操作者手工拉起目标 host 上的 shardsvr 后，在控制台输入 `yes`。
7. 框架等待 shardsvr `PING` 成功，然后继续后续 corrupted/repair 断言。

因此集群模式下，用例仍然写：

```python
with ctx.heartbeat_disabled():
    ctx.kill_shardsvr(target.shard_port)
    ctx.assert_pinned(target)
    ctx.delete_current_file(target)
    ctx.start_shardsvr(target.shard_port)
```

`start_shardsvr()` 在集群模式负责同步 HDFS 修改、提示人工启动并等待目标 shardsvr 恢复。

## 5. shardsvr shutdown/start 配置

集群模式下 `ctx.kill_shardsvr(...)` 不再依赖外部 kill 命令配置。
框架会解析目标 shardsvr 节点，并通过 Redis 协议向该节点发送无参数
`shutdown` 命令。

关闭 HA 后，建议要求 shutdown 后端口确实 down：

```python
CLUSTER_WAIT_PORT_DOWN_TIMEOUT_SEC = 1.0
CLUSTER_REQUIRE_PORT_DOWN_AFTER_KILL = True
CLUSTER_START_WAIT_PING_TIMEOUT_SEC = 60.0
```

`start_shardsvr()` 同步 HDFS 修改后会输出类似：

```text
RocksDB fault injection has been completed and synced to HDFS.
Related partition(s): <partition-id>
Related file(s):
  - <hdfs-path>
Please start the shardsvr process manually.
Target host: <host>
Target owner: <host:port>
After the shardsvr process is started, type 'yes' and press Enter.
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
