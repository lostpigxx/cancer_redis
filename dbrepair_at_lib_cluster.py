# dbrepair_at_lib_cluster.py

import os
import time
from typing import Any, Dict, List, Optional, Set, Tuple

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
      1. Redis 连接按节点 owner(host:port) 映射到不同 host；
      2. RocksDB 文件从 HDFS 镜像到本地 staging 目录；
      3. 测试仍可用 open/os.path/glob 修改 staging 文件；
      4. start_shardsvr() 前统一把 staging 变化同步回 HDFS；
      5. start_shardsvr() 打印人工启动提示并等待输入 yes；
      6. 用户手工拉起 shardsvr 后继续等待 ping 和后续断言。
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
        self._last_target_partition: Optional[Partition] = None
        self._last_killed_node: Optional[Dict[str, Any]] = None
        super().__init__()

    # ------------------------------------------------------------------
    # Redis 连接
    # ------------------------------------------------------------------

    def redis_conn(self, port: int) -> redis.Redis:
        host = self._host_for_port(port)
        return self._redis_conn_to(host, port)

    def _redis_conn_to(self, host: str, port: int) -> redis.Redis:
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

        hosts_by_port = getattr(test_env, "NODE_HOSTS_BY_PORT", {})
        if port in hosts_by_port:
            return hosts_by_port[port]

        nodes = self._nodes_for_port(port)
        if len(nodes) == 1:
            return nodes[0]["host"]

        assert not nodes, (
            "ambiguous shardsvr port {} maps to multiple hosts: {}. "
            "Use target owner or SHARDSVR_NODES node name instead of bare port.".format(
                port,
                [node["owner"] for node in nodes],
            )
        )

        return test_env.REDIS_HOST

    def shard_conn(self, port: int) -> redis.Redis:
        node = self._resolve_node(port)
        return self._redis_conn_to(node["host"], node["port"])

    def _owner_key(self, host: str, port: int) -> str:
        return "{}:{}".format(host, int(port))

    def _split_owner(self, owner: str) -> Tuple[str, int]:
        host, port_text = owner.rsplit(":", 1)
        return host, int(port_text)

    def _configured_nodes(self) -> List[Dict[str, Any]]:
        nodes = []
        raw_nodes = getattr(test_env, "SHARDSVR_NODES", [])

        for index, raw in enumerate(raw_nodes):
            name = str(raw.get("name") or "shardsvr{}".format(index + 1))
            host = str(raw["host"])
            port = int(raw["port"])
            nodes.append(
                {
                    "name": name,
                    "host": host,
                    "port": port,
                    "owner": self._owner_key(host, port),
                }
            )

        if nodes:
            return nodes

        endpoints = getattr(test_env, "SHARDSVR_ENDPOINTS", {})
        if isinstance(endpoints, dict):
            for index, (key, endpoint) in enumerate(endpoints.items()):
                host, port = endpoint
                nodes.append(
                    {
                        "name": str(key),
                        "host": str(host),
                        "port": int(port),
                        "owner": self._owner_key(str(host), int(port)),
                    }
                )
            return nodes

        for index, endpoint in enumerate(endpoints):
            host, port = endpoint
            nodes.append(
                {
                    "name": "shardsvr{}".format(index + 1),
                    "host": str(host),
                    "port": int(port),
                    "owner": self._owner_key(str(host), int(port)),
                }
            )

        return nodes

    def _nodes_for_port(self, port: int) -> List[Dict[str, Any]]:
        return [
            node
            for node in self._configured_nodes()
            if node["port"] == int(port)
        ]

    def _node_for_owner(self, owner: str) -> Dict[str, Any]:
        host, port = self._split_owner(owner)
        owner_key = self._owner_key(host, port)

        for node in self._configured_nodes():
            if node["owner"] == owner_key:
                return node

        return {
            "name": owner_key,
            "host": host,
            "port": port,
            "owner": owner_key,
        }

    def _resolve_node(self, ref: Any) -> Dict[str, Any]:
        if isinstance(ref, Partition):
            return self._node_for_owner(ref.owner)

        if isinstance(ref, str):
            if ":" in ref:
                return self._node_for_owner(ref)

            for node in self._configured_nodes():
                if ref in (node["name"], node["host"]):
                    return node

            raise AssertionError("unknown shardsvr node reference: {}".format(ref))

        port = int(ref)

        if self._last_killed_node is not None and self._last_killed_node["port"] == port:
            return self._last_killed_node

        if (
            self._last_target_partition is not None
            and self._last_target_partition.shard_port == port
        ):
            return self._node_for_owner(self._last_target_partition.owner)

        nodes = self._nodes_for_port(port)

        if len(nodes) == 1:
            return nodes[0]

        assert nodes, "no shardsvr node configured for port {}".format(port)
        raise AssertionError(
            "ambiguous shardsvr port {} maps to multiple nodes: {}. "
            "Pass Partition/owner/node name, or call pick_target_partition() before "
            "kill_shardsvr(target.shard_port).".format(
                port,
                [node["owner"] for node in nodes],
            )
        )

    # ------------------------------------------------------------------
    # shardsvr 进程控制 / HA 拉起
    # ------------------------------------------------------------------

    def kill_shardsvr(self, port: int) -> None:
        node = self._resolve_node(port)
        print("shutdown shardsvr: node={}".format(node["owner"]))
        self._send_shutdown(
            self._redis_conn_to(node["host"], node["port"]),
            "node={}".format(node["owner"]),
        )

        self._last_killed_node = node
        self._in_fault_window = True
        self._capture_fault_window_snapshots()

        timeout = getattr(test_env, "CLUSTER_WAIT_PORT_DOWN_TIMEOUT_SEC", 1.0)
        require_down = getattr(test_env, "CLUSTER_REQUIRE_PORT_DOWN_AFTER_KILL", False)

        try:
            self._wait_node_down(node, timeout_sec=timeout)
        except AssertionError as exc:
            if require_down:
                raise
            print(
                "port did not stay down after kill, probably HA already restarted: "
                "node={}, error={}".format(node["owner"], exc)
            )

    def start_shardsvr(self, port: int) -> None:
        """
        集群模式先同步 HDFS 故障文件，再等待人工启动 shardsvr。

        HA 关闭时，框架无法可靠代替运维 agent 拉起进程，所以这里会打印
        英文提示，要求操作者在目标 host 上拉起 shardsvr 后输入 yes。
        """
        changed_by_partition = self._sync_fault_window_changes()
        node = self._resolve_node(port)
        self._prompt_manual_shardsvr_start(node, changed_by_partition)

        timeout = getattr(test_env, "CLUSTER_START_WAIT_PING_TIMEOUT_SEC", 60.0)
        print(
            "wait manually started shardsvr ping: node={}, timeout={}".format(
                node["owner"],
                timeout,
            )
        )
        self._wait_ping_node(node, timeout_sec=timeout)
        self._last_killed_node = None

    def _prompt_manual_shardsvr_start(
        self,
        node: Dict[str, Any],
        changed_by_partition: Dict[str, List[str]],
    ) -> None:
        partitions = sorted(changed_by_partition.keys())
        files = []

        for partition_id in partitions:
            files.extend(changed_by_partition[partition_id])

        print("")
        print("RocksDB fault injection has been completed and synced to HDFS.")
        print("Related partition(s): {}".format(", ".join(partitions) or "<unknown>"))
        print("Related file(s):")

        if files:
            for path in files:
                print("  - {}".format(path))
        else:
            print("  - <no file changes recorded>")

        print("Please start the shardsvr process manually.")
        print("Target host: {}".format(node["host"]))
        print("Target owner: {}".format(node["owner"]))
        print("After the shardsvr process is started, type 'yes' and press Enter.")

        while True:
            try:
                answer = input("Continue after manual shardsvr start? [yes/no]: ")
            except EOFError:
                raise AssertionError(
                    "manual shardsvr start confirmation requires console input"
                )

            answer = answer.strip().lower()

            if answer in ("yes", "y"):
                return

            if answer in ("no", "n"):
                raise AssertionError("manual shardsvr start was not confirmed")

            print("Please type 'yes' after starting shardsvr, or 'no' to abort.")

    def _wait_ping_node(self, node: Dict[str, Any], timeout_sec: float) -> None:
        deadline = time.time() + timeout_sec
        last_error = None

        while time.time() < deadline:
            try:
                assert self._redis_conn_to(node["host"], node["port"]).ping() is True
                return
            except Exception as e:
                last_error = e
                time.sleep(0.2)

        raise AssertionError(
            "redis node {} not alive, last_error={}".format(
                node["owner"],
                last_error,
            )
        )

    def wait_ping(self, port: int, timeout_sec: float = 15.0) -> None:
        node = self._resolve_node(port)
        self._wait_ping_node(node, timeout_sec=timeout_sec)

    def wait_all_shards_ping(self, timeout_sec: float = 10.0) -> None:
        nodes = self._configured_nodes()

        if not nodes:
            owners = {
                p.owner
                for p in self.query_partitions()
            }
            nodes = [
                self._node_for_owner(owner)
                for owner in sorted(owners)
            ]

        for node in nodes:
            self._wait_ping_node(node, timeout_sec=timeout_sec)

    def _wait_node_down(self, node: Dict[str, Any], timeout_sec: float) -> None:
        deadline = time.time() + timeout_sec

        while time.time() < deadline:
            try:
                self._redis_conn_to(node["host"], node["port"]).ping()
            except Exception:
                return

            time.sleep(0.2)

        raise AssertionError("{} is still alive after kill".format(node["owner"]))

    def pick_target_partition(self) -> Partition:
        chunks = self.chunksmap()
        cfg_by_pid = {
            p.partition_id: p
            for p in self.query_partitions()
        }

        preferred_owner = getattr(test_env, "PREFERRED_TARGET_SHARDSVR_OWNER", "")
        preferred_name = getattr(test_env, "PREFERRED_TARGET_SHARDSVR_NAME", "")
        preferred_port = getattr(test_env, "PREFERRED_TARGET_SHARDSVR_PORT", None)
        preferred_node = None

        if preferred_owner:
            preferred_node = self._node_for_owner(preferred_owner)
        elif preferred_name:
            try:
                preferred_node = self._resolve_node(preferred_name)
            except AssertionError:
                preferred_node = None

        for p in chunks:
            cfg_p = cfg_by_pid[p.partition_id]
            if cfg_p.state not in OPEN_STATES:
                continue

            if preferred_node and p.owner == preferred_node["owner"]:
                p.state = cfg_p.state
                self._last_target_partition = p
                return p

        for p in chunks:
            cfg_p = cfg_by_pid[p.partition_id]
            if cfg_p.state not in OPEN_STATES:
                continue

            if preferred_port is not None and p.shard_port == int(preferred_port):
                p.state = cfg_p.state
                self._last_target_partition = p
                return p

        for p in chunks:
            cfg_p = cfg_by_pid[p.partition_id]
            if cfg_p.state in OPEN_STATES:
                p.state = cfg_p.state
                self._last_target_partition = p
                return p

        raise AssertionError("no opened partition found")

    def flushmem(self, target: Partition):
        cmd = []

        for item in test_env.FLUSHMEM_COMMAND_TEMPLATE:
            cmd.append(
                item.format(
                    partition_id=target.partition_id,
                    shard_port=target.shard_port,
                )
            )

        assert cmd, "FLUSHMEM_COMMAND_TEMPLATE is empty"

        node = self._node_for_owner(target.owner)
        shard = self._redis_conn_to(node["host"], node["port"])
        resp = shard.execute_command(*cmd)

        print(
            "flushmem response: partition={}, owner={}, cmd={}, resp={!r}".format(
                target.partition_id,
                target.owner,
                cmd,
                resp,
            )
        )

        return resp

    def repair_partition(self, target: Partition):
        node = self._node_for_owner(target.owner)
        shard = self._redis_conn_to(node["host"], node["port"])

        resp = shard.execute_command(
            "dbrepair",
            "auto",
            target.partition_id,
        )

        print("dbrepair auto response: {!r}".format(resp))
        return resp

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

    def _sync_fault_window_changes(self) -> Dict[str, List[str]]:
        if not self._in_fault_window and not self._fault_window_snapshots:
            return {}

        changed_by_partition: Dict[str, List[str]] = {}
        for partition_id, before in sorted(self._fault_window_snapshots.items()):
            print(
                "sync HDFS partition changes: partition={}, remote_dir={}".format(
                    partition_id,
                    self.hdfs.remote_partition_dir(partition_id),
                )
            )
            changed_by_partition[partition_id] = (
                self.hdfs.sync_local_changes_to_hdfs(partition_id, before)
            )

        self._fault_window_snapshots.clear()
        self._in_fault_window = False
        return changed_by_partition

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
