# dbrepair_at_lib_cluster.py

import os
import subprocess
from typing import Any, Dict, List, Optional, Set

import redis

import env as test_env
from dbrepair_at_lib_local import (
    CORRUPTED_STATES,
    OPEN_STATES,
    REPAIRING_STATES,
    Partition,
    RepairAT as LocalRepairAT,
    WalInjectRecord,
    WalMeta,
)
from hdfs_file_backend import HdfsFileBackend, LocalMeta


class RepairAT(LocalRepairAT):
    """
    分布式 / HDFS / HA 场景 RepairAT。

    对测试用例保持与单机版相同的接口：
      1. Redis 连接按端口映射到不同 host；
      2. RocksDB 文件从 HDFS 镜像到本地 staging 目录；
      3. 测试仍可用 open/os.path/glob 修改 staging 文件；
      4. start_shardsvr() 前统一把 staging 变化同步回 HDFS；
      5. start_shardsvr() 通过 ssh 到目标节点执行启动命令；
      6. START_SHARDSVR_COMMANDS 为空时，兼容等待 HA 自动拉起。
    """

    def __init__(self) -> None:
        self.hdfs = HdfsFileBackend(
            dfs_command=test_env.HDFS_DFS_COMMAND,
            remote_base_path=test_env.BASE_PATH,
            shard_subdir=test_env.SHARDSVR_DB_SUBDIR,
            local_staging_dir=test_env.LOCAL_STAGING_DIR,
            partition_dir_template=getattr(
                test_env,
                "HDFS_PARTITION_DIR_TEMPLATE",
                "{partition_id}",
            ),
            put_supports_force=getattr(test_env, "HDFS_PUT_SUPPORTS_FORCE", True),
        )
        self._in_fault_window = False
        self._mirrored_partitions: Set[str] = set()
        self._fault_window_snapshots: Dict[str, Dict[str, LocalMeta]] = {}
        super().__init__()

    # ------------------------------------------------------------------
    # Redis 连接
    # ------------------------------------------------------------------

    def redis_conn(self, port: int) -> redis.Redis:
        host = self._host_for_port(port)
        r = redis.Redis(
            host=host,
            port=port,
            password=test_env.PASSWORD,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )

        r.set_response_callback("INFO", lambda response, **kwargs: response)
        return r

    def _host_for_port(self, port: int) -> str:
        if port == test_env.CFGSVR_PORT:
            return getattr(test_env, "CFGSVR_HOST", test_env.REDIS_HOST)

        if port == test_env.PROXY_PORT:
            return getattr(test_env, "PROXY_HOST", test_env.REDIS_HOST)

        endpoints = getattr(test_env, "SHARDSVR_ENDPOINTS", {})
        if port in endpoints:
            return endpoints[port][0]

        hosts_by_port = getattr(test_env, "NODE_HOSTS_BY_PORT", {})
        if port in hosts_by_port:
            return hosts_by_port[port]

        return test_env.REDIS_HOST

    # ------------------------------------------------------------------
    # shardsvr 进程控制 / HA 拉起
    # ------------------------------------------------------------------

    def kill_shardsvr(self, port: int) -> None:
        cmd = self._kill_command(port)
        print("kill shardsvr: port={}, cmd={}".format(port, cmd))

        if isinstance(cmd, str):
            subprocess.check_call(cmd, shell=True)
        else:
            subprocess.check_call(cmd)

        self._in_fault_window = True
        self._capture_fault_window_snapshots()

        timeout = getattr(test_env, "CLUSTER_WAIT_PORT_DOWN_TIMEOUT_SEC", 1.0)
        require_down = getattr(test_env, "CLUSTER_REQUIRE_PORT_DOWN_AFTER_KILL", False)

        try:
            self._wait_port_down(port, timeout_sec=timeout)
        except AssertionError as exc:
            if require_down:
                raise
            print(
                "port did not stay down after kill, probably HA already restarted: "
                "port={}, error={}".format(port, exc)
            )

    def start_shardsvr(self, port: int) -> None:
        """
        集群模式先同步 HDFS 故障文件，再启动 shardsvr。

        HA 关闭时，通过 env_cluster.START_SHARDSVR_COMMANDS ssh 到目标节点
        执行启动命令；若命令为空，则兼容旧行为，只等待 HA 自动拉起。
        """
        self._sync_fault_window_changes()

        cmd = self._start_command(port)

        if cmd:
            print("start shardsvr: port={}, cmd={}".format(port, cmd))

            if isinstance(cmd, str):
                subprocess.check_call(cmd, shell=True)
            else:
                subprocess.check_call(cmd)

            timeout = getattr(test_env, "CLUSTER_START_WAIT_PING_TIMEOUT_SEC", 60.0)
            print(
                "wait started shardsvr ping: port={}, timeout={}".format(
                    port,
                    timeout,
                )
            )
            self.wait_ping(port, timeout_sec=timeout)
            return

        timeout = getattr(test_env, "CLUSTER_HA_WAIT_PING_TIMEOUT_SEC", 60.0)
        print(
            "START_SHARDSVR_COMMANDS[{}] is empty, wait HA to start shardsvr: "
            "timeout={}".format(port, timeout)
        )
        self.wait_ping(port, timeout_sec=timeout)

    def _kill_command(self, port: int) -> Any:
        commands = getattr(test_env, "KILL_SHARDSVR_COMMANDS", {})
        template = commands.get(port)

        assert template, (
            "missing KILL_SHARDSVR_COMMANDS[{}] in env_cluster.py".format(port)
        )

        return self._render_node_command(template, port)

    def _start_command(self, port: int) -> Optional[Any]:
        commands = getattr(test_env, "START_SHARDSVR_COMMANDS", {})
        template = commands.get(port)

        if not template:
            return None

        return self._render_node_command(template, port)

    def _render_node_command(self, template: Any, port: int) -> Any:
        context = {
            "host": self._host_for_port(port),
            "owner_host": self._host_for_port(port),
            "port": port,
        }

        if isinstance(template, str):
            return template.format(**context)

        return [str(item).format(**context) for item in template]

    # ------------------------------------------------------------------
    # HDFS staging 文件视图
    # ------------------------------------------------------------------

    def partition_db_dir(self, target: Partition) -> str:
        partition_id = target.partition_id

        if not self._in_fault_window or partition_id not in self._mirrored_partitions:
            local_dir = self.hdfs.sync_partition_to_local(partition_id)
            self._mirrored_partitions.add(partition_id)
        else:
            local_dir = self.hdfs.local_partition_dir(partition_id)

        if self._in_fault_window and partition_id not in self._fault_window_snapshots:
            self._fault_window_snapshots[partition_id] = (
                self.hdfs.snapshot_local_partition(partition_id)
            )

        assert os.path.isdir(local_dir), (
            "partition db dir staging not found: partition={}, local_path={}, remote_path={}".format(
                partition_id,
                local_dir,
                self.hdfs.remote_partition_dir(partition_id),
            )
        )

        return local_dir

    def _capture_fault_window_snapshots(self) -> None:
        for partition_id in sorted(self._mirrored_partitions):
            self._fault_window_snapshots[partition_id] = (
                self.hdfs.snapshot_local_partition(partition_id)
            )

    def _sync_fault_window_changes(self) -> None:
        if not self._in_fault_window and not self._fault_window_snapshots:
            return

        for partition_id, before in sorted(self._fault_window_snapshots.items()):
            print(
                "sync HDFS partition changes: partition={}, remote_dir={}".format(
                    partition_id,
                    self.hdfs.remote_partition_dir(partition_id),
                )
            )
            self.hdfs.sync_local_changes_to_hdfs(partition_id, before)

        self._fault_window_snapshots.clear()
        self._in_fault_window = False

    def chmod_sst_file(self, path: str, mode: int) -> int:
        if getattr(test_env, "CLUSTER_SUPPORTS_CHMOD_FAULT", False):
            return super().chmod_sst_file(path, mode)

        try:
            import pytest

            pytest.skip("cluster/HDFS mode does not support chmod SST fault")
        except ImportError:
            raise AssertionError("cluster/HDFS mode does not support chmod SST fault")


__all__ = [
    "OPEN_STATES",
    "CORRUPTED_STATES",
    "REPAIRING_STATES",
    "Partition",
    "WalMeta",
    "WalInjectRecord",
    "RepairAT",
]
