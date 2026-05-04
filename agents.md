# 项目说明

本项目是 DB repair 自动化测试辅助库，核心文件是 `dbrepair_at_lib.py`，配置入口是 `env.py`，接口说明在 `interface.md`。

`dbrepair_at_lib.py` 封装了连接 cfgsvr、proxy、shardsvr，查询 partition 路由和状态，构造落入指定 partition 的 hashtag，写入和校验测试数据，控制 shardsvr 进程，定位 RocksDB partition 目录，注入 CURRENT、MANIFEST、WAL、SST 文件级故障，以及执行 `dbrepair auto <partition-id>` 等能力。

## 开发约定

- 保持 Python 代码兼容当前项目风格：标准库优先，函数和变量使用 snake_case，错误检查主要使用 `assert` 给出明确失败信息。
- 修改 `dbrepair_at_lib.py` 的外部接口时，同步更新 `interface.md`，确保测试用例只读接口文档也能正确调用。
- `env.py` 只放运行环境配置，不放测试逻辑。不要把本地私有路径、密码或一次性调试命令硬编码进库代码。
- 文件损坏、进程停止、repair 等操作具有破坏性。新增逻辑时要明确目标 partition，并避免影响非目标 partition。
- 离线损坏 shardsvr 文件前，优先使用 `RepairAT.heartbeat_disabled()`，避免 cfgsvr heartbeat 触发 partition 迁移。
- 对 WAL 损坏后的数据校验，允许 WAL-only 数据丢失，但不允许读出错误值，优先使用 `assert_values_missing_or_exact()`。
- 对非目标 partition 的 guard key，应该使用强校验，优先使用 `assert_values_exact()`。

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
- 如需 S8 lower-level SST 用例，配置 `COMPACT_COMMAND_TEMPLATE`

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

## 重要文件

- `dbrepair_at_lib.py`：DB repair AT 辅助库实现。
- `interface.md`：外部接口说明，供测试用例作者使用。
- `env.py`：环境配置模板和默认参数。

