# env.py
#
# 环境配置分发层。只根据 dbrepair_mode.py 选择单机或集群配置。

from dbrepair_mode import DBREPAIR_AT_MODE


if DBREPAIR_AT_MODE == "local":
    from env_local import *  # noqa: F401,F403
elif DBREPAIR_AT_MODE == "cluster":
    from env_cluster import *  # noqa: F401,F403
else:
    raise AssertionError(
        "unsupported DBREPAIR_AT_MODE={!r}, expected 'local' or 'cluster'".format(
            DBREPAIR_AT_MODE,
        )
    )
