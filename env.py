# env.py
#
# DBRepair AT 配置文件。
#
# 这个文件只放环境配置，不放测试逻辑。
# 每个环境运行前，需要手动确认并填写：
#   1. BASE_PATH
#   2. START_SHARDSVR_COMMANDS
#   3. 如果需要 S8 compact 用例，则填写 COMPACT_COMMAND_TEMPLATE
#
# 目录结构要求：
#
# BASE_PATH/
#   cfgsvrdb/
#     route/
#       CURRENT
#       MANIFEST-xxxx
#       *.log
#   shdsvrdb/
#     <partition-id>/
#       CURRENT
#       MANIFEST-xxxx
#       OPTIONS-xxxx
#       *.log
#       *.sst / *.ldb


# =============================================================================
# 基础路径
# =============================================================================

# DB 根目录。
# 示例：
#   /data/DFV-Redis/build/db
BASE_PATH = "/data/DFV-Redis/build/db"

# shardsvr 的 RocksDB 目录名。
# 按当前目录结构：
#   BASE_PATH/shdsvrdb/<partition-id>
SHARDSVR_DB_SUBDIR = "shdsvrdb"

# cfgsvr 的 RocksDB 目录名。
# 当前用例一般不直接操作 cfgsvrdb，但保留配置。
CFGSVR_DB_SUBDIR = "cfgsvrdb"


# =============================================================================
# Redis 协议连接配置
# =============================================================================

REDIS_HOST = "127.0.0.1"
PASSWORD = "a"

CFGSVR_PORT = 6378
PROXY_PORT = 6379

SHARDSVR1_PORT = 6381
SHARDSVR2_PORT = 6382

SHARDSVR_PORTS = [
    SHARDSVR1_PORT,
    SHARDSVR2_PORT,
]


# =============================================================================
# 集群 partition 配置
# =============================================================================

# 当前 AT 集群 partition 数量。
EXPECTED_PARTITION_COUNT = 3

# 默认优先选择哪个 shardsvr 上的 partition 做故障注入。
PREFERRED_TARGET_SHARDSVR_PORT = SHARDSVR1_PORT


# =============================================================================
# shardsvr 启动命令
# =============================================================================

# 启动 shardsvr 的命令。
#
# 需要手动填写。
#
# 注意：
#   1. 命令应该能单独启动对应端口的 shardsvr。
#   2. 不需要在命令末尾加 &。
#   3. 测试框架内部会用 subprocess.Popen 拉起。
#
# 示例：
# START_SHARDSVR_COMMANDS = {
#     6381: "cd /data/DFV-Redis/geminiredis && ./gemini-redis-server AT/shdsvr1.conf",
#     6382: "cd /data/DFV-Redis/geminiredis && ./gemini-redis-server AT/shdsvr2.conf",
# }
START_SHARDSVR_COMMANDS = {
    6381: "",
    6382: "",
}


# =============================================================================
# flushmem 命令配置
# =============================================================================

# flushmem 是发给 shardsvr 的命令，用于把目标 partition 的 memtable flush 成 SST。
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
