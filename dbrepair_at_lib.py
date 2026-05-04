# dbrepair_at_lib.py

import glob
import os
import re
import signal
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import redis

import env as test_env


OPEN_STATES = {"opened", "open"}
CORRUPTED_STATES = {"corrupted"}
REPAIRING_STATES = {"repairing"}


@dataclass
class Partition:
    owner: str
    shard_port: int
    start: int
    end: int
    partition_id: str
    state: Optional[str] = None


@dataclass
class WalMeta:
    path: str
    size: int
    mtime: float


@dataclass
class WalInjectRecord:
    path: str
    old_size: int
    final_size: int
    segments: List[Tuple[int, int]]


class RepairAT:
    """
    DB repair AT 的最小通用接口。

    用例只需要调用这里的方法，不需要关心：
      1. redis-py 如何连接；
      2. cfgsvr query partitions 如何解析；
      3. info chunksmap 如何解析；
      4. whereis 如何解析；
      5. partition DB 目录如何定位；
      6. WAL 文件如何选择和注入；
      7. flushmem 与 SST 文件如何构造、选择、破坏。
    """

    def __init__(self) -> None:
        self.cfg = self.redis_conn(test_env.CFGSVR_PORT)
        self.proxy = self.redis_conn(test_env.PROXY_PORT)

    # ----------------------------------------------------------------------
    # Redis 连接与基础命令
    # ----------------------------------------------------------------------

    def redis_conn(self, port: int) -> redis.Redis:
        r = redis.Redis(
            host=test_env.REDIS_HOST,
            port=port,
            password=test_env.PASSWORD,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )

        # redis-py 默认可能会把 INFO 解析成 dict。
        # 本测试需要 INFO chunksmap 的原始 bulk string。
        r.set_response_callback("INFO", lambda response, **kwargs: response)

        return r

    def shard_conn(self, port: int) -> redis.Redis:
        return self.redis_conn(port)

    def enable_heartbeat(self) -> None:
        self.cfg.execute_command("cfgsvr", "heartbeat", "enable")

    def disable_heartbeat(self) -> None:
        self.cfg.execute_command("cfgsvr", "heartbeat", "disable")

    @contextmanager
    def heartbeat_disabled(self):
        """
        临时关闭 cfgsvr heartbeat。

        用途：
          停止某个 shardsvr 做离线文件损坏时，避免 cfgsvr 触发 partition 迁移。
        """
        self.disable_heartbeat()
        try:
            yield
        finally:
            self.enable_heartbeat()

    # ----------------------------------------------------------------------
    # Partition 查询与解析
    # ----------------------------------------------------------------------

    def parse_chunksmap(self, raw: str) -> List[Partition]:
        """
        解析 proxy INFO chunksmap。

        返回 bulk string 示例：

          # ChunksMap
                  172.17.0.2:6381,0,262143,c96d6ef78a54a16a
                  172.17.0.2:6382,262144,524287,13de029c342c5aa4
        """
        if isinstance(raw, bytes):
            raw = raw.decode()

        partitions = []

        for line in raw.splitlines():
            line = line.strip()

            if not line:
                continue

            if line.startswith("#"):
                continue

            parts = line.split(",")
            if len(parts) != 4:
                continue

            owner, start, end, partition_id = parts
            shard_port = int(owner.rsplit(":", 1)[1])

            partitions.append(
                Partition(
                    owner=owner,
                    shard_port=shard_port,
                    start=int(start),
                    end=int(end),
                    partition_id=partition_id,
                )
            )

        assert partitions, "failed to parse chunksmap: {!r}".format(raw)
        return partitions

    def chunksmap(self) -> List[Partition]:
        raw = self.proxy.execute_command("INFO", "chunksmap")
        return self.parse_chunksmap(raw)

    def query_partitions(self) -> List[Partition]:
        """
        解析 cfgsvr query partitions。

        返回示例：

        [
          [
            "172.17.0.2:6381",
            [
              "0:262143:a8767dbb7e73b29c:opened"
            ]
          ]
        ]
        """
        raw = self.cfg.execute_command("cfgsvr", "query", "partitions")

        partitions = []

        for node in raw:
            owner = node[0]
            shard_port = int(owner.rsplit(":", 1)[1])

            for item in node[1]:
                start, end, partition_id, state = item.split(":")

                partitions.append(
                    Partition(
                        owner=owner,
                        shard_port=shard_port,
                        start=int(start),
                        end=int(end),
                        partition_id=partition_id,
                        state=state,
                    )
                )

        return partitions

    def get_partition(self, partition_id: str) -> Partition:
        for p in self.query_partitions():
            if p.partition_id == partition_id:
                return p

        raise AssertionError("partition not found: {}".format(partition_id))

    def assert_all_partitions_opened(self) -> None:
        partitions = self.query_partitions()

        assert len(partitions) == test_env.EXPECTED_PARTITION_COUNT, (
            "unexpected partition count: actual={}, expected={}".format(
                len(partitions),
                test_env.EXPECTED_PARTITION_COUNT,
            )
        )

        for p in partitions:
            assert p.state in OPEN_STATES, (
                "partition not opened: {}".format(p)
            )

    def wait_state_in(
        self,
        partition_id: str,
        expected_states: Set[str],
        timeout_sec: float = 30.0,
    ) -> Partition:
        deadline = time.time() + timeout_sec
        last = None

        while time.time() < deadline:
            last = self.get_partition(partition_id)

            if last.state in expected_states:
                return last

            time.sleep(0.2)

        raise AssertionError(
            "partition {} did not become one of {}, last={}".format(
                partition_id,
                expected_states,
                last,
            )
        )

    def wait_opened(self, target: Partition, timeout_sec: float = 60.0) -> Partition:
        return self.wait_state_in(
            partition_id=target.partition_id,
            expected_states=OPEN_STATES,
            timeout_sec=timeout_sec,
        )

    def wait_corrupted(self, target: Partition, timeout_sec: float = 30.0) -> Partition:
        return self.wait_state_in(
            partition_id=target.partition_id,
            expected_states=CORRUPTED_STATES,
            timeout_sec=timeout_sec,
        )

    def pick_target_partition(self) -> Partition:
        """
        选择目标 partition。

        优先选择 env.py 中 PREFERRED_TARGET_SHARDSVR_PORT 上的 opened partition。
        如果没有，则选择任意 opened partition。
        """
        chunks = self.chunksmap()
        cfg_by_pid = {
            p.partition_id: p
            for p in self.query_partitions()
        }

        preferred_port = test_env.PREFERRED_TARGET_SHARDSVR_PORT

        for p in chunks:
            cfg_p = cfg_by_pid[p.partition_id]
            if p.shard_port == preferred_port and cfg_p.state in OPEN_STATES:
                p.state = cfg_p.state
                return p

        for p in chunks:
            cfg_p = cfg_by_pid[p.partition_id]
            if cfg_p.state in OPEN_STATES:
                p.state = cfg_p.state
                return p

        raise AssertionError("no opened partition found")

    def snapshot_owners(self) -> Dict[str, str]:
        return {
            p.partition_id: p.owner
            for p in self.query_partitions()
        }

    def assert_pinned(self, target: Partition) -> None:
        current = self.get_partition(target.partition_id)

        assert current.owner == target.owner, (
            "partition owner changed: partition={}, before={}, after={}".format(
                target.partition_id,
                target.owner,
                current.owner,
            )
        )

        assert current.shard_port == target.shard_port, (
            "partition shard_port changed: partition={}, before={}, after={}".format(
                target.partition_id,
                target.shard_port,
                current.shard_port,
            )
        )

    def assert_owners_unchanged(self, owners_before: Dict[str, str]) -> None:
        owners_now = self.snapshot_owners()

        for partition_id, owner_before in owners_before.items():
            assert owners_now[partition_id] == owner_before, (
                "partition owner changed unexpectedly: "
                "partition={}, before={}, after={}".format(
                    partition_id,
                    owner_before,
                    owners_now[partition_id],
                )
            )

    # ----------------------------------------------------------------------
    # Hashtag 与数据写入校验
    # ----------------------------------------------------------------------

    def parse_whereis_hash(self, raw: str) -> int:
        if isinstance(raw, bytes):
            raw = raw.decode()

        if isinstance(raw, (list, tuple)):
            raw = " ".join(str(x) for x in raw)

        m = re.search(r"Hash:\s*(\d+)", raw)
        assert m, "failed to parse whereis hash: {!r}".format(raw)

        return int(m.group(1))

    def whereis_hash(self, key: str) -> int:
        raw = self.proxy.execute_command("whereis", key)
        return self.parse_whereis_hash(raw)

    def assert_key_routes_to_partition(self, key: str, target: Partition) -> None:
        slot = self.whereis_hash(key)

        assert target.start <= slot <= target.end, (
            "key does not route to target partition: "
            "key={}, slot={}, target=[{}, {}]".format(
                key,
                slot,
                target.start,
                target.end,
            )
        )

    def hashtag_for(
        self,
        partition: Partition,
        prefix: str,
        max_try: int = 200000,
    ) -> str:
        """
        构造一个落在指定 partition 上的 hashtag。

        返回形式：
          {prefix:N}
        """
        for i in range(max_try):
            tag = "{{{}:{}}}".format(prefix, i)
            slot = self.whereis_hash(tag)

            if partition.start <= slot <= partition.end:
                return tag

        raise AssertionError(
            "failed to find hashtag for partition: "
            "partition_id={}, range=[{}, {}]".format(
                partition.partition_id,
                partition.start,
                partition.end,
            )
        )

    def write_strings(
        self,
        tag: str,
        key_prefix: str,
        count: Optional[int] = None,
        value_size: Optional[int] = None,
    ) -> Dict[str, str]:
        """
        通过 proxy 写入一批 string key。

        key 形态：
          <key_prefix>:<i>:<tag>

        返回：
          {key: value}
        """
        if count is None:
            count = test_env.DEFAULT_WRITE_COUNT

        if value_size is None:
            value_size = test_env.DEFAULT_VALUE_SIZE

        expected = {}
        payload = "x" * value_size

        for i in range(count):
            key = "{}:{}:{}".format(key_prefix, i, tag)
            value = "value:{}:{}".format(i, payload)

            assert self.proxy.set(key, value) is True
            expected[key] = value

        return expected

    def write_one_and_assert(self, tag: str, key_prefix: str, value: str) -> str:
        key = "{}:{}".format(key_prefix, tag)

        assert self.proxy.set(key, value) is True
        assert self.proxy.get(key) == value

        return key

    def write_guard_strings(
        self,
        exclude_partition_id: str,
        prefix: str,
    ) -> Dict[str, str]:
        """
        给非目标 partition 写入 guard key。

        用途：
          验证目标 partition 故障没有扩散到其他 partition。
        """
        expected = {}

        for p in self.chunksmap():
            if p.partition_id == exclude_partition_id:
                continue

            tag = self.hashtag_for(
                partition=p,
                prefix="{}-{}".format(prefix, p.partition_id),
            )

            key = "{}:{}:{}".format(prefix, p.partition_id, tag)
            value = "guard-value:{}".format(p.partition_id)

            assert self.proxy.set(key, value) is True
            expected[key] = value

        return expected

    def assert_values_exact(self, expected: Dict[str, str]) -> None:
        """
        强校验：
          key 必须存在，value 必须正确。
        """
        for key, value in expected.items():
            actual = self.proxy.get(key)
            assert actual == value, (
                "value mismatch: key={}, expected={}, actual={}".format(
                    key,
                    value,
                    actual,
                )
            )

    def assert_values_missing_or_exact(self, expected: Dict[str, str]) -> None:
        """
        弱校验：
          key 可以不存在；
          但如果存在，value 必须正确。

        用途：
          WAL 损坏修复后，WAL-only 数据允许丢失，但不能读出错误值。
        """
        kept = 0
        lost = 0

        for key, value in expected.items():
            actual = self.proxy.get(key)

            if actual is None:
                lost += 1
                continue

            assert actual == value, (
                "value mismatch: key={}, expected={}, actual={}".format(
                    key,
                    value,
                    actual,
                )
            )
            kept += 1

        print(
            "values missing-or-exact result: kept={}, lost={}, total={}".format(
                kept,
                lost,
                len(expected),
            )
        )

    # ----------------------------------------------------------------------
    # shardsvr 进程控制
    # ----------------------------------------------------------------------

    def kill_shardsvr(self, port: int) -> None:
        pids = self._find_listen_pids(port)
        assert pids, "no process is listening on port {}".format(port)

        for pid in pids:
            print("kill shardsvr: port={}, pid={}".format(port, pid))
            os.kill(pid, signal.SIGKILL)

        self._wait_port_down(port, timeout_sec=10)

    def start_shardsvr(self, port: int) -> None:
        cmd = test_env.START_SHARDSVR_COMMANDS.get(port)

        assert cmd, "missing START_SHARDSVR_COMMANDS[{}] in env.py".format(port)

        print("start shardsvr: port={}, cmd={}".format(port, cmd))

        subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid,
        )

        self.wait_ping(port, timeout_sec=30)

    def wait_ping(self, port: int, timeout_sec: float = 15.0) -> None:
        deadline = time.time() + timeout_sec
        last_error = None

        while time.time() < deadline:
            try:
                assert self.redis_conn(port).ping() is True
                return
            except Exception as e:
                last_error = e
                time.sleep(0.2)

        raise AssertionError(
            "redis node {} not alive, last_error={}".format(
                port,
                last_error,
            )
        )

    def wait_all_shards_ping(self, timeout_sec: float = 10.0) -> None:
        for port in test_env.SHARDSVR_PORTS:
            self.wait_ping(port, timeout_sec=timeout_sec)

    def _find_listen_pids(self, port: int) -> List[int]:
        cmds = [
            "lsof -tiTCP:{} -sTCP:LISTEN 2>/dev/null || true".format(port),
            "ss -ltnp 2>/dev/null | grep ':{} ' | sed -n 's/.*pid=\\([0-9]\\+\\).*/\\1/p' || true".format(port),
            "netstat -ltnp 2>/dev/null | grep ':{} ' | awk '{{print $7}}' | cut -d/ -f1 || true".format(port),
        ]

        pids = set()

        for cmd in cmds:
            out = subprocess.check_output(cmd, shell=True, text=True)

            for line in out.splitlines():
                line = line.strip()
                if line.isdigit():
                    pids.add(int(line))

            if pids:
                break

        return sorted(pids)

    def _wait_port_down(self, port: int, timeout_sec: float) -> None:
        deadline = time.time() + timeout_sec

        while time.time() < deadline:
            try:
                self.redis_conn(port).ping()
            except Exception:
                return

            time.sleep(0.2)

        raise AssertionError("port {} is still alive after kill".format(port))

    # ----------------------------------------------------------------------
    # DB 文件路径与通用文件损坏
    # ----------------------------------------------------------------------

    def partition_db_dir(self, target: Partition) -> str:
        """
        返回目标 partition 的 RocksDB 目录：

          BASE_PATH/shdsvrdb/<partition-id>
        """
        path = os.path.join(
            test_env.BASE_PATH,
            test_env.SHARDSVR_DB_SUBDIR,
            target.partition_id,
        )

        assert os.path.isdir(path), (
            "partition db dir not found: partition={}, path={}".format(
                target.partition_id,
                path,
            )
        )

        return path

    def current_file_path(self, target: Partition) -> str:
        return os.path.join(self.partition_db_dir(target), "CURRENT")

    def read_current_manifest_name(self, target: Partition) -> str:
        path = self.current_file_path(target)

        with open(path, "r") as f:
            return f.read().strip()

    def current_manifest_path(self, target: Partition) -> str:
        manifest_name = self.read_current_manifest_name(target)
        return os.path.join(self.partition_db_dir(target), manifest_name)

    def delete_file(self, path: str) -> None:
        assert os.path.exists(path), "file not found: {}".format(path)
        os.remove(path)

    def overwrite_file_middle(self, path: str, length: int = 4096) -> Tuple[int, int, int]:
        """
        覆盖文件中间一段，制造 checksum/格式损坏。

        返回：
          old_size, offset, length
        """
        old_size = os.path.getsize(path)

        assert old_size > length * 2, (
            "file too small to overwrite middle: path={}, size={}, length={}".format(
                path,
                old_size,
                length,
            )
        )

        offset = old_size // 2
        bad = (b"DBREPAIR_AT_CORRUPTION!" * (length // 23 + 1))[:length]

        with open(path, "r+b") as f:
            f.seek(offset)
            f.write(bad)
            f.flush()
            os.fsync(f.fileno())

        return old_size, offset, length

    def break_current_to_missing_manifest(
        self,
        target: Partition,
        manifest_name: str = "MANIFEST-999999",
    ) -> None:
        """
        把 CURRENT 改成指向不存在的 MANIFEST。

        用途：
          稳定制造 RocksDB Open 失败。
        """
        path = self.current_file_path(target)

        with open(path, "w") as f:
            f.write(manifest_name)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())

    def delete_current_file(self, target: Partition) -> None:
        self.delete_file(self.current_file_path(target))

    def delete_current_manifest_file(self, target: Partition) -> None:
        self.delete_file(self.current_manifest_path(target))

    # ----------------------------------------------------------------------
    # WAL 快照与 WAL 中间丢失注入
    # ----------------------------------------------------------------------

    def wal_snapshot(self, target: Partition) -> Dict[str, WalMeta]:
        wal_dir = self.partition_db_dir(target)
        result = {}

        for path in sorted(glob.glob(os.path.join(wal_dir, "*.log"))):
            st = os.stat(path)
            result[path] = WalMeta(
                path=path,
                size=st.st_size,
                mtime=st.st_mtime,
            )

        return result

    def print_wal_snapshot(self, title: str, snapshot: Dict[str, WalMeta]) -> None:
        print(title)

        if not snapshot:
            print("  <no wal files>")
            return

        for path, meta in sorted(snapshot.items()):
            print("  {} size={} mtime={}".format(path, meta.size, meta.mtime))

    def select_grown_wals(
        self,
        before: Dict[str, WalMeta],
        after: Dict[str, WalMeta],
    ) -> List[str]:
        candidates = []

        for path, after_meta in after.items():
            before_meta = before.get(path)

            if before_meta is None:
                if after_meta.size >= test_env.MIN_TARGET_WAL_GROWTH:
                    candidates.append(path)
                continue

            growth = after_meta.size - before_meta.size

            if growth >= test_env.MIN_TARGET_WAL_GROWTH:
                candidates.append(path)

        return candidates

    def inject_wal_middle_loss(
        self,
        target: Partition,
        before: Dict[str, WalMeta],
        after: Dict[str, WalMeta],
    ) -> List[WalInjectRecord]:
        """
        向目标 partition 的 WAL 注入“中间丢失”。

        只选择写入后确实增长过的 WAL。
        如果没有增长过的 WAL，直接失败。
        """
        candidates = self.select_grown_wals(before, after)

        if not candidates:
            self.print_wal_snapshot("WAL before target writes:", before)
            self.print_wal_snapshot("WAL after target writes:", after)

            raise AssertionError(
                "no WAL file grew enough after target writes: "
                "partition={}, min_growth={}".format(
                    target.partition_id,
                    test_env.MIN_TARGET_WAL_GROWTH,
                )
            )

        records = []

        for path in candidates:
            before_meta = before.get(path)
            after_meta = after[path]

            record = self._inject_middle_loss_to_wal_file(
                path=path,
                before_meta=before_meta,
                after_meta=after_meta,
            )

            records.append(record)

        self.print_wal_injection_records(records)
        return records

    def print_wal_injection_records(self, records: List[WalInjectRecord]) -> None:
        print("WAL middle loss injection records:")

        for r in records:
            print(
                "  path={}, old_size={}, final_size={}, segments={}".format(
                    r.path,
                    r.old_size,
                    r.final_size,
                    r.segments,
                )
            )

    def _inject_middle_loss_to_wal_file(
        self,
        path: str,
        before_meta: Optional[WalMeta],
        after_meta: WalMeta,
    ) -> WalInjectRecord:
        old_size = os.path.getsize(path)

        if before_meta is None:
            region_start = 0
        else:
            region_start = before_meta.size

        region_end = old_size

        # 如果增长区间过小，退化为破坏整个 WAL 中间区域。
        if region_end - region_start < test_env.MIN_TARGET_WAL_GROWTH:
            region_start = 0
            region_end = old_size

        segments = self._make_loss_segments(
            file_size=old_size,
            region_start=region_start,
            region_end=region_end,
            gap_count=test_env.WAL_MIDDLE_LOSS_GAP_COUNT,
            gap_size=test_env.WAL_MIDDLE_LOSS_GAP_SIZE,
        )

        assert segments, (
            "failed to build WAL middle-loss segments: "
            "path={}, old_size={}, region=[{}, {})".format(
                path,
                old_size,
                region_start,
                region_end,
            )
        )

        with open(path, "rb") as f:
            data = bytearray(f.read())

        # 从后往前删除，避免前一次删除影响后续 offset。
        for offset, length in sorted(segments, reverse=True):
            del data[offset:offset + length]

        with open(path, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

        final_size = os.path.getsize(path)

        return WalInjectRecord(
            path=path,
            old_size=old_size,
            final_size=final_size,
            segments=segments,
        )

    def _make_loss_segments(
        self,
        file_size: int,
        region_start: int,
        region_end: int,
        gap_count: int,
        gap_size: int,
    ) -> List[Tuple[int, int]]:
        assert 0 <= region_start < region_end <= file_size

        region_len = region_end - region_start
        actual_gap_size = min(gap_size, max(512, region_len // 8))

        if region_len < actual_gap_size * 3:
            return []

        ratios = [0.40, 0.50, 0.60, 0.33, 0.66]
        segments = []

        for ratio in ratios[:gap_count]:
            center = region_start + int(region_len * ratio)
            offset = center - actual_gap_size // 2

            offset = max(region_start + 512, offset)
            offset = min(region_end - actual_gap_size - 512, offset)

            if offset <= region_start:
                continue

            if offset + actual_gap_size >= region_end:
                continue

            segments.append((offset, actual_gap_size))

        dedup = []
        seen = set()

        for item in segments:
            if item in seen:
                continue

            seen.add(item)
            dedup.append(item)

        return dedup

    # ----------------------------------------------------------------------
    # flushmem 与 SST 构造
    # ----------------------------------------------------------------------

    def flushmem(self, target: Partition):
        """
        对目标 partition 所在 shardsvr 执行 flushmem。

        命令格式由 env.py 的 FLUSHMEM_COMMAND_TEMPLATE 控制。

        例：
          ["flushmem", "{partition_id}"]
        会渲染为：
          flushmem <partition-id>
        """
        cmd = []

        for item in test_env.FLUSHMEM_COMMAND_TEMPLATE:
            cmd.append(
                item.format(
                    partition_id=target.partition_id,
                    shard_port=target.shard_port,
                )
            )

        assert cmd, "FLUSHMEM_COMMAND_TEMPLATE is empty"

        shard = self.shard_conn(target.shard_port)
        resp = shard.execute_command(*cmd)

        print(
            "flushmem response: partition={}, shard_port={}, cmd={}, resp={!r}".format(
                target.partition_id,
                target.shard_port,
                cmd,
                resp,
            )
        )

        return resp

    def flushmem_all_partitions(self) -> None:
        """
        对当前所有 opened partition 执行 flushmem。

        用途：
          guard key 写入后，将所有 opened partition 的数据落成 SST。
        """
        for p in self.query_partitions():
            if p.state in OPEN_STATES:
                self.flushmem(p)

    def flushmem_partitions_except(self, excluded_partition_ids: Set[str]) -> None:
        """
        对除了 excluded_partition_ids 之外的 opened partition 执行 flushmem。

        用途：
          guard key 写入后，只 flush 非目标 partition，
          避免目标 partition 在选定待损坏 SST 前后又产生额外 SST/compaction。
        """
        for p in self.query_partitions():
            if p.partition_id in excluded_partition_ids:
                continue

            if p.state in OPEN_STATES:
                self.flushmem(p)

    def sst_files(self, target: Partition) -> List[str]:
        """
        返回目标 partition 目录下所有 SST 文件。

        RocksDB 表文件常见后缀：
          .sst
          .ldb
        """
        db_dir = self.partition_db_dir(target)

        files = []
        files.extend(glob.glob(os.path.join(db_dir, "*.sst")))
        files.extend(glob.glob(os.path.join(db_dir, "*.ldb")))

        return sorted(files)

    def sst_snapshot(self, target: Partition) -> Dict[str, Tuple[int, float]]:
        """
        返回目标 partition 的 SST 快照：
          path -> (size, mtime)
        """
        result = {}

        for path in self.sst_files(target):
            st = os.stat(path)
            result[path] = (st.st_size, st.st_mtime)

        return result

    def wait_new_sst_files(
        self,
        target: Partition,
        before_snapshot: Dict[str, Tuple[int, float]],
        timeout_sec: Optional[float] = None,
    ) -> List[str]:
        """
        等待 flushmem 后出现新的 SST 文件。

        返回新出现的 SST 文件列表。
        """
        if timeout_sec is None:
            timeout_sec = test_env.WAIT_SST_TIMEOUT_SEC

        before_paths = set(before_snapshot.keys())
        deadline = time.time() + timeout_sec
        last_files = []

        while time.time() < deadline:
            after_files = self.sst_files(target)
            last_files = after_files

            new_files = [
                p for p in after_files
                if p not in before_paths
            ]

            if new_files:
                print("new SST files: {}".format(new_files))
                return sorted(new_files)

            # 如果之前没有 SST，但现在有 SST，也接受。
            if not before_paths and after_files:
                print("SST files appeared: {}".format(after_files))
                return sorted(after_files)

            time.sleep(0.2)

        raise AssertionError(
            "no new SST file generated after flushmem: "
            "partition={}, before={}, last_files={}".format(
                target.partition_id,
                before_snapshot,
                last_files,
            )
        )

    def prepare_sst_for_partition(
        self,
        target: Partition,
        tag: str,
        key_prefix: str,
        count: Optional[int] = None,
        value_size: Optional[int] = None,
    ) -> Tuple[Dict[str, str], List[str]]:
        """
        在目标 partition 内主动构造 SST。

        流程：
          1. 记录当前 SST 快照；
          2. 通过 proxy 写入目标 partition 数据；
          3. 对目标 partition 执行 flushmem；
          4. 等待新 SST 文件出现；
          5. 验证写入数据可读。

        返回：
          expected, new_sst_files
        """
        if count is None:
            count = test_env.SST_PREPARE_WRITE_COUNT

        if value_size is None:
            value_size = test_env.SST_PREPARE_VALUE_SIZE

        before = self.sst_snapshot(target)

        expected = self.write_strings(
            tag=tag,
            key_prefix=key_prefix,
            count=count,
            value_size=value_size,
        )

        self.flushmem(target)

        new_ssts = self.wait_new_sst_files(
            target=target,
            before_snapshot=before,
        )

        # flush 后数据应仍然可读。
        self.assert_values_exact(expected)

        return expected, new_ssts

    def pick_largest_sst_from(
        self,
        sst_files: List[str],
        min_size: int = 1,
    ) -> str:
        """
        从给定 SST 文件列表里选择当前仍然存在的最大文件。

        注意：
          flushmem 生成的新 SST 可能很快被 compaction / purge 删除。
          所以这里必须过滤已经不存在的文件。
        """
        assert sst_files, "empty SST file list"

        live_files = [
            p for p in sst_files
            if os.path.exists(p)
        ]

        assert live_files, (
            "all candidate SST files have disappeared. "
            "original_candidates={}".format(sst_files)
        )

        candidates = [
            p for p in live_files
            if os.path.getsize(p) >= min_size
        ]

        assert candidates, (
            "no live SST file larger than min_size={}. files={}".format(
                min_size,
                [(p, os.path.getsize(p)) for p in live_files],
            )
        )

        return max(candidates, key=os.path.getsize)

    def pick_largest_live_sst(
        self,
        target: Partition,
        preferred_files: Optional[List[str]] = None,
        min_size: int = 1,
    ) -> str:
        """
        选择目标 partition 当前仍然存在的 live SST 文件。

        preferred_files:
          优先从 prepare_sst_for_partition 返回的新 SST 里选；
          如果这些 SST 已经被 compaction/purge 删除，则 fallback 到当前目录下所有 live SST。

        这个方法应尽量在 kill_shardsvr() 之后调用。
        因为 shardsvr 停止后，SST 文件集合才相对稳定。
        """
        if preferred_files:
            live_preferred = [
                p for p in preferred_files
                if os.path.exists(p) and os.path.getsize(p) >= min_size
            ]

            if live_preferred:
                return max(live_preferred, key=os.path.getsize)

        live_all = [
            p for p in self.sst_files(target)
            if os.path.exists(p) and os.path.getsize(p) >= min_size
        ]

        assert live_all, (
            "no live SST file found for partition={}, min_size={}. "
            "preferred_files={}, current_sst_files={}".format(
                target.partition_id,
                min_size,
                preferred_files,
                self.sst_files(target),
            )
        )

        return max(live_all, key=os.path.getsize)

    # ----------------------------------------------------------------------
    # SST 故障注入
    # ----------------------------------------------------------------------

    def delete_sst_file(self, path: str) -> None:
        """
        删除 SST 文件。
        """
        assert os.path.exists(path), "SST file not found: {}".format(path)

        os.remove(path)
        print("deleted SST file: {}".format(path))

    def truncate_sst_file_to_half(self, path: str) -> Tuple[int, int]:
        """
        将 SST 文件截断为原来的一半。
        """
        old_size = os.path.getsize(path)

        assert old_size > 4096, (
            "SST file too small to truncate: path={}, size={}".format(
                path,
                old_size,
            )
        )

        new_size = max(1, old_size // 2)

        with open(path, "r+b") as f:
            f.truncate(new_size)
            f.flush()
            os.fsync(f.fileno())

        print(
            "truncated SST file: path={}, old_size={}, new_size={}".format(
                path,
                old_size,
                new_size,
            )
        )

        return old_size, new_size

    def zero_sst_file(self, path: str) -> int:
        """
        将 SST 文件内容清零，保留文件名和文件大小。
        """
        old_size = os.path.getsize(path)

        assert old_size > 0, "SST file is empty: {}".format(path)

        chunk = b"\x00" * 1024 * 1024
        remaining = old_size

        with open(path, "r+b") as f:
            f.seek(0)

            while remaining > 0:
                write_len = min(len(chunk), remaining)
                f.write(chunk[:write_len])
                remaining -= write_len

            f.flush()
            os.fsync(f.fileno())

        new_size = os.path.getsize(path)
        assert new_size == old_size, (
            "SST file size changed after zeroing: path={}, old_size={}, new_size={}".format(
                path,
                old_size,
                new_size,
            )
        )

        print(
            "zeroed SST file: path={}, size={}".format(
                path,
                old_size,
            )
        )

        return old_size

    def corrupt_sst_tail(
        self,
        path: str,
        length: int = 4096,
    ) -> Tuple[int, int, int]:
        """
        覆盖 SST 文件尾部。

        用于模拟 footer / metaindex / index 损坏。
        """
        old_size = os.path.getsize(path)

        assert old_size > length * 2, (
            "SST file too small to corrupt tail: path={}, size={}, length={}".format(
                path,
                old_size,
                length,
            )
        )

        actual_len = min(length, old_size // 4)
        offset = old_size - actual_len

        bad = (
            b"SST_TAIL_CORRUPTION!" *
            (actual_len // len(b"SST_TAIL_CORRUPTION!") + 1)
        )[:actual_len]

        with open(path, "r+b") as f:
            f.seek(offset)
            f.write(bad)
            f.flush()
            os.fsync(f.fileno())

        print(
            "corrupted SST tail: path={}, size={}, offset={}, length={}".format(
                path,
                old_size,
                offset,
                actual_len,
            )
        )

        return old_size, offset, actual_len

    def corrupt_sst_data_block_area(
        self,
        path: str,
        length: int = 4096,
    ) -> Tuple[int, int, int]:
        """
        覆盖 SST 文件前中部，尽量命中 data block 区域。

        注意：
          data block 损坏不一定在 Open 阶段暴露。
          有些 RocksDB 配置只会在 Get/Iterator/Compaction 读取该 block 时发现 checksum mismatch。
        """
        old_size = os.path.getsize(path)
        min_size = test_env.SST_DATA_BLOCK_MIN_FILE_SIZE

        assert old_size >= min_size, (
            "SST file too small for data block corruption: "
            "path={}, size={}, min_size={}".format(
                path,
                old_size,
                min_size,
            )
        )

        # 避开尾部 footer/index/metaindex，尽量打到 data block。
        offset = old_size // 3
        actual_len = min(length, old_size // 16)

        assert actual_len >= 512
        assert offset + actual_len < old_size - 8192, (
            "invalid SST data block corruption range: "
            "path={}, size={}, offset={}, length={}".format(
                path,
                old_size,
                offset,
                actual_len,
            )
        )

        bad = (
            b"SST_DATA_BLOCK_CORRUPTION!" *
            (actual_len // len(b"SST_DATA_BLOCK_CORRUPTION!") + 1)
        )[:actual_len]

        with open(path, "r+b") as f:
            f.seek(offset)
            f.write(bad)
            f.flush()
            os.fsync(f.fileno())

        print(
            "corrupted SST data block area: "
            "path={}, size={}, offset={}, length={}".format(
                path,
                old_size,
                offset,
                actual_len,
            )
        )

        return old_size, offset, actual_len

    def _decode_varint32(
        self,
        data: bytes,
        offset: int,
        limit: int,
    ) -> Tuple[int, int]:
        value = 0
        shift = 0
        pos = offset

        while pos < limit and shift <= 28:
            b = data[pos]
            pos += 1
            value |= (b & 0x7f) << shift

            if not b & 0x80:
                return value, pos

            shift += 7

        raise AssertionError(
            "failed to decode varint32: offset={}, limit={}".format(
                offset,
                limit,
            )
        )

    def _decode_varint64(
        self,
        data: bytes,
        offset: int,
        limit: int,
    ) -> Tuple[int, int]:
        value = 0
        shift = 0
        pos = offset

        while pos < limit and shift <= 63:
            b = data[pos]
            pos += 1
            value |= (b & 0x7f) << shift

            if not b & 0x80:
                return value, pos

            shift += 7

        raise AssertionError(
            "failed to decode varint64: offset={}, limit={}".format(
                offset,
                limit,
            )
        )

    def _decode_sst_block_handle(
        self,
        data: bytes,
        offset: int,
        limit: int,
    ) -> Tuple[int, int, int]:
        block_offset, pos = self._decode_varint64(data, offset, limit)
        block_size, pos = self._decode_varint64(data, pos, limit)

        return block_offset, block_size, pos

    def _assert_sst_block_handle_in_file(
        self,
        data: bytes,
        block_offset: int,
        block_size: int,
        handle_name: str,
    ) -> None:
        file_size = len(data)

        assert block_size > 0, (
            "invalid SST {} handle: offset={}, size={}".format(
                handle_name,
                block_offset,
                block_size,
            )
        )
        assert block_offset + block_size + 5 <= file_size, (
            "SST {} handle points outside file: "
            "file_size={}, offset={}, size={}".format(
                handle_name,
                file_size,
                block_offset,
                block_size,
            )
        )

    def _decode_sst_footer_metaindex_handle(
        self,
        data: bytes,
    ) -> Tuple[int, int]:
        """
        Decode the metaindex block handle using RocksDB 9.2.1 footer rules.

        Footer version >= 6 does not store a metaindex handle. It stores
        metaindex_size, and the metaindex block is immediately before footer.
        """
        legacy_magic = 0xdb4775248b80fb57
        block_based_magic = 0x88e241b785f4cff7
        legacy_footer_size = 48
        new_footer_size = 53
        block_handle_area_size = 40
        file_size = len(data)

        assert file_size > legacy_footer_size, (
            "SST file too small to decode RocksDB 9.2.1 footer: size={}".format(
                file_size,
            )
        )

        magic = int.from_bytes(data[-8:], byteorder="little")

        if magic == legacy_magic:
            footer_offset = file_size - legacy_footer_size
            metaindex_offset, metaindex_size, _ = self._decode_sst_block_handle(
                data,
                footer_offset,
                footer_offset + block_handle_area_size,
            )
            self._assert_sst_block_handle_in_file(
                data,
                metaindex_offset,
                metaindex_size,
                "legacy metaindex block",
            )

            return metaindex_offset, metaindex_size

        assert magic == block_based_magic, (
            "unsupported SST table magic number for RocksDB 9.2.1 "
            "block-based table: magic=0x{:016x}".format(magic)
        )

        assert file_size > new_footer_size, (
            "SST file too small to decode RocksDB 9.2.1 new footer: size={}".format(
                file_size,
            )
        )

        footer_offset = file_size - new_footer_size
        footer_version_offset = footer_offset + 1 + block_handle_area_size
        footer_version = int.from_bytes(
            data[footer_version_offset:footer_version_offset + 4],
            byteorder="little",
        )

        if footer_version <= 5:
            metaindex_offset, metaindex_size, _ = self._decode_sst_block_handle(
                data,
                footer_offset + 1,
                footer_offset + 1 + block_handle_area_size,
            )
            self._assert_sst_block_handle_in_file(
                data,
                metaindex_offset,
                metaindex_size,
                "metaindex block",
            )

            return metaindex_offset, metaindex_size

        metaindex_size_offset = footer_offset + 13
        metaindex_size = int.from_bytes(
            data[metaindex_size_offset:metaindex_size_offset + 4],
            byteorder="little",
        )
        metaindex_offset = footer_offset - metaindex_size - 5

        self._assert_sst_block_handle_in_file(
            data,
            metaindex_offset,
            metaindex_size,
            "format_version>=6 metaindex block",
        )

        return metaindex_offset, metaindex_size

    def _decode_sst_block_entries(
        self,
        data: bytes,
        block_offset: int,
        block_size: int,
        block_name: str,
    ) -> List[Tuple[bytes, bytes]]:
        block_end = block_offset + block_size
        compression_type = data[block_end]

        assert compression_type == 0, (
            "SST {} block is compressed or uses unsupported compression: "
            "offset={}, size={}, compression_type={}".format(
                block_name,
                block_offset,
                block_size,
                compression_type,
            )
        )

        block = data[block_offset:block_end]
        assert len(block) >= 8, (
            "SST {} block too small: offset={}, size={}".format(
                block_name,
                block_offset,
                block_size,
            )
        )

        restart_count = int.from_bytes(block[-4:], byteorder="little")
        restarts_size = restart_count * 4
        restarts_offset = len(block) - 4 - restarts_size

        assert restart_count > 0 and restarts_offset > 0, (
            "invalid SST {} restart array: "
            "offset={}, size={}, restart_count={}".format(
                block_name,
                block_offset,
                block_size,
                restart_count,
            )
        )

        entries: List[Tuple[bytes, bytes]] = []
        last_key = b""
        pos = 0

        while pos < restarts_offset:
            entry_start = pos

            try:
                shared, pos = self._decode_varint32(block, pos, restarts_offset)
                non_shared, pos = self._decode_varint32(block, pos, restarts_offset)
                value_len, pos = self._decode_varint32(block, pos, restarts_offset)
            except AssertionError as exc:
                if restarts_offset - entry_start <= 2:
                    break

                raise AssertionError(
                    "failed to decode SST {} entry header: "
                    "entry_start={}, restarts_offset={}, error={}".format(
                        block_name,
                        entry_start,
                        restarts_offset,
                        exc,
                    )
                )

            assert shared <= len(last_key), (
                "invalid SST {} entry shared prefix: shared={}, last_key_len={}".format(
                    block_name,
                    shared,
                    len(last_key),
                )
            )
            assert pos + non_shared + value_len <= restarts_offset, (
                "invalid SST {} entry: pos={}, shared={}, "
                "non_shared={}, value_len={}, restarts_offset={}".format(
                    block_name,
                    pos,
                    shared,
                    non_shared,
                    value_len,
                    restarts_offset,
                )
            )

            key_delta = block[pos:pos + non_shared]
            pos += non_shared

            value = block[pos:pos + value_len]
            pos += value_len

            key = last_key[:shared] + key_delta
            entries.append((key, value))
            last_key = key

        return entries

    def _decode_sst_meta_block_handle(
        self,
        data: bytes,
        key_part: bytes,
        block_name: str,
    ) -> Tuple[bytes, int, int]:
        metaindex_offset, metaindex_size = self._decode_sst_footer_metaindex_handle(
            data,
        )
        meta_entries = self._decode_sst_block_entries(
            data,
            metaindex_offset,
            metaindex_size,
            "metaindex",
        )

        block_handles: List[Tuple[bytes, int, int]] = []
        key_part_lower = key_part.lower()

        for key, value in meta_entries:
            if key_part_lower not in key.lower():
                continue

            try:
                block_offset, block_size, pos = self._decode_sst_block_handle(
                    value,
                    0,
                    len(value),
                )
            except AssertionError:
                continue

            if pos != len(value):
                continue

            try:
                self._assert_sst_block_handle_in_file(
                    data,
                    block_offset,
                    block_size,
                    "{} block {!r}".format(block_name, key),
                )
            except AssertionError:
                continue

            block_handles.append((key, block_offset, block_size))

        assert block_handles, (
            "no {} block handle found in SST metaindex".format(block_name)
        )

        return sorted(block_handles, key=lambda item: item[0])[0]

    def _decode_sst_filter_block_handle(self, data: bytes) -> Tuple[bytes, int, int]:
        return self._decode_sst_meta_block_handle(
            data,
            b"filter",
            "filter",
        )

    def _decode_sst_properties_block_handle(
        self,
        data: bytes,
    ) -> Tuple[bytes, int, int]:
        return self._decode_sst_meta_block_handle(
            data,
            b"properties",
            "properties",
        )

    def corrupt_sst_checksum_area(self, path: str) -> Tuple[int, int, int]:
        """
        解析 SST footer，找到 metaindex block，并翻转其 trailer 中的 checksum。

        RocksDB block trailer 结构为：
          1 byte compression type + 4 byte checksum
        """
        with open(path, "rb") as f:
            data = f.read()

        old_size = len(data)
        block_offset, block_size = self._decode_sst_footer_metaindex_handle(data)
        offset = block_offset + block_size + 1
        actual_len = 4

        assert offset + actual_len <= old_size, (
            "invalid SST checksum corruption range: "
            "path={}, size={}, block_offset={}, block_size={}, "
            "offset={}, length={}".format(
                path,
                old_size,
                block_offset,
                block_size,
                offset,
                actual_len,
            )
        )

        with open(path, "r+b") as f:
            f.seek(offset)
            checksum = f.read(actual_len)

            assert len(checksum) == actual_len, (
                "failed to read SST checksum bytes: path={}, offset={}".format(
                    path,
                    offset,
                )
            )

            bad = bytes(b ^ 0xff for b in checksum)

            f.seek(offset)
            f.write(bad)
            f.flush()
            os.fsync(f.fileno())

        print(
            "corrupted SST checksum area: "
            "path={}, size={}, metaindex_block_offset={}, metaindex_block_size={}, "
            "offset={}, length={}".format(
                path,
                old_size,
                block_offset,
                block_size,
                offset,
                actual_len,
            )
        )

        return old_size, offset, actual_len

    def corrupt_sst_filter_block_area(self, path: str) -> Tuple[int, int, int]:
        """
        解析 SST metaindex，找到 filter block，并翻转该 block trailer 中的 checksum。

        RocksDB block trailer 结构为：
          1 byte compression type + 4 byte checksum
        """
        with open(path, "rb") as f:
            data = f.read()

        old_size = len(data)
        filter_key, filter_offset, filter_size = self._decode_sst_filter_block_handle(
            data,
        )
        offset = filter_offset + filter_size + 1
        actual_len = 4

        assert offset + actual_len <= old_size, (
            "invalid SST filter block corruption range: "
            "path={}, size={}, filter_key={!r}, filter_offset={}, "
            "filter_size={}, offset={}, length={}".format(
                path,
                old_size,
                filter_key,
                filter_offset,
                filter_size,
                offset,
                actual_len,
            )
        )

        with open(path, "r+b") as f:
            f.seek(offset)
            checksum = f.read(actual_len)

            assert len(checksum) == actual_len, (
                "failed to read SST filter checksum bytes: "
                "path={}, offset={}".format(
                    path,
                    offset,
                )
            )

            bad = bytes(b ^ 0xff for b in checksum)

            f.seek(offset)
            f.write(bad)
            f.flush()
            os.fsync(f.fileno())

        print(
            "corrupted SST filter block area: "
            "path={}, size={}, filter_key={!r}, filter_offset={}, "
            "filter_size={}, offset={}, length={}".format(
                path,
                old_size,
                filter_key,
                filter_offset,
                filter_size,
                offset,
                actual_len,
            )
        )

        return old_size, offset, actual_len

    def corrupt_sst_properties_block_area(self, path: str) -> Tuple[int, int, int]:
        """
        解析 SST metaindex，找到 properties block，并翻转该 block trailer 中的 checksum。

        RocksDB block trailer 结构为：
          1 byte compression type + 4 byte checksum
        """
        with open(path, "rb") as f:
            data = f.read()

        old_size = len(data)
        properties_key, properties_offset, properties_size = (
            self._decode_sst_properties_block_handle(data)
        )
        offset = properties_offset + properties_size + 1
        actual_len = 4

        assert offset + actual_len <= old_size, (
            "invalid SST properties block corruption range: "
            "path={}, size={}, properties_key={!r}, properties_offset={}, "
            "properties_size={}, offset={}, length={}".format(
                path,
                old_size,
                properties_key,
                properties_offset,
                properties_size,
                offset,
                actual_len,
            )
        )

        with open(path, "r+b") as f:
            f.seek(offset)
            checksum = f.read(actual_len)

            assert len(checksum) == actual_len, (
                "failed to read SST properties checksum bytes: "
                "path={}, offset={}".format(
                    path,
                    offset,
                )
            )

            bad = bytes(b ^ 0xff for b in checksum)

            f.seek(offset)
            f.write(bad)
            f.flush()
            os.fsync(f.fileno())

        print(
            "corrupted SST properties block area: "
            "path={}, size={}, properties_key={!r}, properties_offset={}, "
            "properties_size={}, offset={}, length={}".format(
                path,
                old_size,
                properties_key,
                properties_offset,
                properties_size,
                offset,
                actual_len,
            )
        )

        return old_size, offset, actual_len

    # ----------------------------------------------------------------------
    # Repair
    # ----------------------------------------------------------------------

    def repair_partition(self, target: Partition):
        """
        向目标 partition 所属 shardsvr 发送：

          dbrepair auto <partition-id>
        """
        shard = self.shard_conn(target.shard_port)

        resp = shard.execute_command(
            "dbrepair",
            "auto",
            target.partition_id,
        )

        print("dbrepair auto response: {!r}".format(resp))
        return resp

    def repair_and_wait_opened(self, target: Partition, timeout_sec: float = 60.0) -> Partition:
        self.repair_partition(target)

        repaired = self.wait_opened(target, timeout_sec=timeout_sec)

        assert repaired.owner == target.owner
        assert repaired.shard_port == target.shard_port

        return repaired
