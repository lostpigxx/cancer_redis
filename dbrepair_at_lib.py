# dbrepair_at_lib.py
#
# RepairAT 实现分发层。测试用例继续 `from dbrepair_at_lib import RepairAT`。

from dbrepair_mode import DBREPAIR_AT_MODE


if DBREPAIR_AT_MODE == "local":
    from dbrepair_at_lib_local import *  # noqa: F401,F403
elif DBREPAIR_AT_MODE == "cluster":
    from dbrepair_at_lib_cluster import *  # noqa: F401,F403
else:
    raise AssertionError(
        "unsupported DBREPAIR_AT_MODE={!r}, expected 'local' or 'cluster'".format(
            DBREPAIR_AT_MODE,
        )
    )
