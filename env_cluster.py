# env_cluster.py
#
# DBRepair AT 分布式 / HDFS / HA 场景配置。
#
# 本文件只放环境配置，不放测试逻辑。
# cluster 模式配置与 env_local.py 独立维护，不从 env_local.py 继承默认值。


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

# 当前 cluster 环境 partition 数量。必须按真实 cfgsvr route 配置填写。
EXPECTED_PARTITION_COUNT = 3

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


# =============================================================================
# flushmem 命令配置
# =============================================================================

# flushmem 是发给目标 partition owner shardsvr 的命令，用于把 memtable flush 成 SST。
#
# 如果真实命令是：
#   flushmem <partition>
# 保持默认：
#   ["flushmem", "{partition_id}"]
#
# 如果真实命令是：
#   flushmem
# 改成：
#   ["flushmem"]
#
# 支持占位符：
#   {partition_id}
#   {shard_port}
FLUSHMEM_COMMAND_TEMPLATE = ["flushmem", "{partition_id}"]

# flushmem 后等待 SST 生成的超时时间。
WAIT_SST_TIMEOUT_SEC = 30


# =============================================================================
# compact 命令配置
# =============================================================================

# compact 是发给 shardsvr 的命令，用于构造 L1/L2 等 lower-level SST。
#
# 如果真实命令是：
#   compactmem <partition>
# 则配置：
#   ["compactmem", "{partition_id}"]
#
# 如果真实命令是：
#   compact <partition>
# 则配置：
#   ["compact", "{partition_id}"]
#
# 如果没有 compact 接口，保持空列表。
# S8 lower-level SST 用例依赖这个配置。
COMPACT_COMMAND_TEMPLATE = []

# compact 后等待 SST 集合变化的超时时间。
WAIT_COMPACT_TIMEOUT_SEC = 60


# =============================================================================
# MANIFEST 元数据编辑工具配置
# =============================================================================

# M16 依赖 lower-level MANIFEST 编辑工具，在当前 MANIFEST 中定向篡改 SST
# metadata，例如引用不存在 SST、错误 file number、错误 level、错误 key range。
#
# 如果没有这类工具，保持空列表，M16 会 pytest.skip。
#
# 示例：
# MANIFEST_METADATA_TAMPER_COMMAND_TEMPLATE = [
#     "/path/to/manifest_tamper",
#     "--case", "{case_name}",
#     "--manifest", "{manifest_path}",
#     "--sst-file-number", "{sst_file_number}",
#     "--partition", "{partition_id}",
# ]
#
# 支持占位符：
#   {case_name}
#   {partition_id}
#   {partition_db_dir}
#   {manifest_path}
#   {manifest_name}
#   {sst_path}
#   {sst_name}
#   {sst_file_number}
#   {shard_port}
MANIFEST_METADATA_TAMPER_COMMAND_TEMPLATE = []


# =============================================================================
# 默认写入参数
# =============================================================================

# 通用写入参数。
DEFAULT_WRITE_COUNT = 1024
DEFAULT_VALUE_SIZE = 2048

# 为 SST 用例准备目标 SST 的默认写入量。
SST_PREPARE_WRITE_COUNT = 1024
SST_PREPARE_VALUE_SIZE = 2048

# data block 损坏用例需要较大的 SST。
SST_DATA_BLOCK_WRITE_COUNT = 4096
SST_DATA_BLOCK_VALUE_SIZE = 2048
SST_DATA_BLOCK_MIN_FILE_SIZE = 64 * 1024


# =============================================================================
# WAL 故障注入参数
# =============================================================================

# WAL 中间丢失注入参数。
# 每个增长 WAL 会删除多个中间片段。
WAL_MIDDLE_LOSS_GAP_COUNT = 3
WAL_MIDDLE_LOSS_GAP_SIZE = 4096

# 写入后 WAL 至少增长多少字节，才认为目标写入确实进入了目标 partition 的 WAL。
MIN_TARGET_WAL_GROWTH = 16 * 1024


# =============================================================================
# S7：L0 多 SST 部分损坏
# =============================================================================

# 构造多少个 L0 SST。
S7_L0_FILE_COUNT = 3

# 每个 L0 SST 写入多少 key。
S7_WRITE_COUNT_PER_FILE = 256
S7_VALUE_SIZE = 1024


# =============================================================================
# S8：L1/L2 SST 损坏
# =============================================================================

# compact 前的 base 数据。
S8_BASE_WRITE_COUNT = 2048
S8_BASE_VALUE_SIZE = 1024

# compact 后的 overlay 数据。
S8_OVERLAY_WRITE_COUNT = 256
S8_OVERLAY_VALUE_SIZE = 512


# =============================================================================
# S9 / S10：修复风险用例开关
# =============================================================================

# S9：一个 key 的新版本 SST 损坏后，旧版本是否复活。
#
# False：
#   只打印风险，不让用例失败。
#
# True：
#   如果观察到旧版本复活，则用例失败。
S9_FAIL_ON_OLD_VALUE_RESURRECTION = False

# S10：Delete tombstone SST 损坏后，被删除 key 是否复活。
#
# False：
#   只打印风险，不让用例失败。
#
# True：
#   如果观察到删除 key 复活，则用例失败。
S10_FAIL_ON_DELETE_TOMBSTONE_RESURRECTION = False

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
