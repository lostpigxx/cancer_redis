# env_cluster.py
#
# DBRepair AT 分布式 / HDFS / HA 场景配置。
#
# 本文件只放环境配置，不放测试逻辑。公共写入量、用例风险开关等默认值
# 复用 env_local.py；集群场景只覆盖连接、HDFS 和 HA 相关配置。

from env_local import *  # noqa: F401,F403


# =============================================================================
# Redis 协议连接配置
# =============================================================================

PASSWORD = "a"

CFGSVR_HOST = "cfgsvr-host"
PROXY_HOST = "proxy-host"

CFGSVR_PORT = 6378
PROXY_PORT = 6379

SHARDSVR1_HOST = "shardsvr1-host"
SHARDSVR2_HOST = "shardsvr2-host"
SHARDSVR_PORT = 6378
SHARDSVR1_PORT = SHARDSVR_PORT
SHARDSVR2_PORT = SHARDSVR_PORT

SHARDSVR_NODES = [
    {
        "name": "shardsvr1",
        "host": SHARDSVR1_HOST,
        "port": SHARDSVR1_PORT,
    },
    {
        "name": "shardsvr2",
        "host": SHARDSVR2_HOST,
        "port": SHARDSVR2_PORT,
    },
]

SHARDSVR_PORTS = [
    node["port"]
    for node in SHARDSVR_NODES
]

# 兼容旧代码路径。cluster 模式不要把 port 当作 shardsvr 唯一标识；
# 真正的节点身份是 SHARDSVR_NODES 里的 host:port。
REDIS_HOST = PROXY_HOST

# 如果 cfgsvr/proxy 返回的 owner host 与配置不一致，可在这里显式覆盖。
NODE_HOSTS_BY_PORT = {
    CFGSVR_PORT: CFGSVR_HOST,
    PROXY_PORT: PROXY_HOST,
}

# 优先选择哪个 shardsvr 节点上的 partition 做故障注入。
PREFERRED_TARGET_SHARDSVR_NAME = "shardsvr1"
PREFERRED_TARGET_SHARDSVR_OWNER = ""
PREFERRED_TARGET_SHARDSVR_PORT = SHARDSVR_PORT


# =============================================================================
# HDFS RocksDB 路径配置
# =============================================================================

# 示例：
#   /redis/5562a7c61b7a4f209f9caa75a17fd5a8in12/rocksdbdata/db
BASE_PATH = "/redis/<cluster-id>/rocksdbdata/db"

# 你的 HDFS 示例里 partition 目录直接位于 BASE_PATH 下：
#   BASE_PATH/<xxxxx-partitionid>/CURRENT
# 因此默认设为空字符串。若实际仍有 shdsvrdb 子目录，可改为 "shdsvrdb"。
SHARDSVR_DB_SUBDIR = ""
CFGSVR_DB_SUBDIR = "cfgsvrdb"

# HDFS partition 目录名模板。支持 {partition_id}。
# 如果真实目录就是 partition id，保持默认。
# 如果真实目录有固定前缀，例如 "rocks-{partition_id}"，在这里修改。
HDFS_PARTITION_DIR_TEMPLATE = "{partition_id}"

HDFS_DFS_COMMAND = [
    "/usr/sbin/chroot",
    "--userspec=Ruby:Ruby",
    "/var/chroot/gemini/",
    "/opt/stream/hadoop-2.7.3/bin/hdfs",
    "dfs",
]

# HDFS 文件先镜像到本地，测试用例仍用 open/os.path/glob 访问本地镜像。
LOCAL_STAGING_DIR = "/tmp/dbrepair_at_hdfs_staging"

# Hadoop 2.7 支持 -put -f；若环境不支持，可置为 False，后端会先 rm 再 put。
HDFS_PUT_SUPPORTS_FORCE = True


# =============================================================================
# shardsvr 进程控制配置
# =============================================================================

# HA 关闭后，start_shardsvr() 会先同步 HDFS 修改，然后在控制台提示
# 操作者到目标 host 手工拉起 shardsvr，并等待输入 yes 后继续。
# cluster 模式不使用 START_SHARDSVR_COMMANDS 自动拉起进程。
START_SHARDSVR_COMMANDS = {}

# kill_shardsvr() 通过 Redis 协议向目标 shardsvr 发送无参数 shutdown。
# 如果 HA 已关闭，建议设为 True，确保 shutdown 后目标 shardsvr 确实停止。
CLUSTER_WAIT_PORT_DOWN_TIMEOUT_SEC = 1.0
CLUSTER_REQUIRE_PORT_DOWN_AFTER_KILL = True

# 人工确认启动后等待 shardsvr ping 成功的超时时间。
CLUSTER_START_WAIT_PING_TIMEOUT_SEC = 60.0


# =============================================================================
# 集群模式能力开关
# =============================================================================

# HDFS 通常无法表达本地 chmod 后 RocksDB open 的同等语义。S11 默认跳过。
CLUSTER_SUPPORTS_CHMOD_FAULT = False
