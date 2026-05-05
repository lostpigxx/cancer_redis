# sitecustomize.py

try:
    from dbrepair_mode import DBREPAIR_AT_MODE
except Exception:
    DBREPAIR_AT_MODE = None


if DBREPAIR_AT_MODE == "cluster":
    from cluster_pytest_console import install

    install()
