# DBRepair AT 接口入口

当前项目支持两套运行模式，由 `dbrepair_mode.py` 统一选择：

```python
DBREPAIR_AT_MODE = "local"    # 单机 / 本地文件系统
DBREPAIR_AT_MODE = "cluster"  # 分布式 / HDFS / HA 自动拉起
```

测试用例继续使用原有 import：

```python
import env as test_env
from dbrepair_at_lib import RepairAT
```

`env.py` 和 `dbrepair_at_lib.py` 是分发层：

- `local` 模式加载 `env_local.py`、`dbrepair_at_lib_local.py`。
- `cluster` 模式加载 `env_cluster.py`、`dbrepair_at_lib_cluster.py`。

两套模式的详细接口分别见：

- `interface_local.md`：原单机模式完整接口说明。
- `interface_cluster.md`：分布式/HDFS/HA 模式新增配置和行为差异。
