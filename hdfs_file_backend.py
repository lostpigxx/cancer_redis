# hdfs_file_backend.py

import hashlib
import os
import posixpath
import shutil
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class HdfsEntry:
    path: str
    name: str
    kind: str
    size: int
    mtime: float = 0.0


LocalMeta = Tuple[str, int, str]


class HdfsFileBackend:
    """
    HDFS RocksDB 文件后端。

    对外暴露本地 staging 路径，测试用例可以继续使用 open/os.path/glob。
    故障注入完成后，通过 sync_local_changes_to_hdfs() 把 staging 变化写回 HDFS。
    """

    def __init__(
        self,
        dfs_command: List[str],
        remote_base_path: str,
        shard_subdir: str,
        local_staging_dir: str,
        partition_dir_template: str = "{partition_id}",
        put_supports_force: bool = True,
    ) -> None:
        self.dfs_command = list(dfs_command)
        self.remote_base_path = remote_base_path.rstrip("/")
        self.shard_subdir = shard_subdir.strip("/")
        self.local_staging_dir = os.path.abspath(local_staging_dir)
        self.partition_dir_template = partition_dir_template
        self.put_supports_force = put_supports_force

        assert self.dfs_command, "HDFS_DFS_COMMAND must not be empty"
        assert self.remote_base_path.startswith("/"), (
            "BASE_PATH must be an absolute HDFS path: {}".format(remote_base_path)
        )

        os.makedirs(self.local_staging_dir, exist_ok=True)

    def remote_partition_dir(self, partition_id: str) -> str:
        partition_dir_name = self.partition_dir_template.format(
            partition_id=partition_id,
        )

        assert "/" not in partition_dir_name, (
            "HDFS_PARTITION_DIR_TEMPLATE must produce one directory name: {}".format(
                partition_dir_name,
            )
        )

        if self.shard_subdir:
            return posixpath.join(
                self.remote_base_path,
                self.shard_subdir,
                partition_dir_name,
            )

        return posixpath.join(self.remote_base_path, partition_dir_name)

    def local_partition_dir(self, partition_id: str) -> str:
        return os.path.join(self.local_staging_dir, partition_id)

    def remote_path_for_local(self, local_path: str) -> str:
        local_path = os.path.abspath(local_path)
        rel = os.path.relpath(local_path, self.local_staging_dir)
        parts = rel.split(os.sep)

        assert len(parts) >= 2, (
            "local path is not inside a partition staging dir: {}".format(local_path)
        )

        partition_id = parts[0]
        return posixpath.join(
            self.remote_partition_dir(partition_id),
            *parts[1:],
        )

    def _run(
        self,
        args: List[str],
        check: bool = True,
        text: bool = True,
    ) -> subprocess.CompletedProcess:
        cmd = self.dfs_command + args
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=text,
        )

        if check and proc.returncode != 0:
            raise AssertionError(
                "HDFS command failed: cmd={}, returncode={}, stdout={!r}, stderr={!r}".format(
                    cmd,
                    proc.returncode,
                    proc.stdout,
                    proc.stderr,
                )
            )

        return proc

    def test(self, flag: str, path: str) -> bool:
        proc = self._run(["-test", flag, path], check=False)
        return proc.returncode == 0

    def exists(self, path: str) -> bool:
        return self.test("-e", path)

    def ls(self, path_pattern: str, allow_missing: bool = False) -> List[HdfsEntry]:
        proc = self._run(["-ls", path_pattern], check=False)

        if proc.returncode != 0:
            if allow_missing:
                return []

            raise AssertionError(
                "HDFS ls failed: path={}, stdout={!r}, stderr={!r}".format(
                    path_pattern,
                    proc.stdout,
                    proc.stderr,
                )
            )

        entries: List[HdfsEntry] = []

        for raw_line in proc.stdout.splitlines():
            line = raw_line.strip()

            if not line or line.startswith("Found "):
                continue

            parts = line.split(None, 7)
            if len(parts) != 8:
                continue

            mode, _, _, _, size_text, _, _, hdfs_path = parts
            kind = "dir" if mode.startswith("d") else "file"
            entries.append(
                HdfsEntry(
                    path=hdfs_path,
                    name=posixpath.basename(hdfs_path.rstrip("/")),
                    kind=kind,
                    size=int(size_text),
                )
            )

        return sorted(entries, key=lambda item: item.path)

    def get_file(self, remote_path: str, local_path: str) -> None:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        if os.path.exists(local_path):
            if os.path.isdir(local_path) and not os.path.islink(local_path):
                shutil.rmtree(local_path)
            else:
                os.remove(local_path)

        self._run(["-get", remote_path, local_path])

    def mkdir(self, remote_path: str) -> None:
        self._run(["-mkdir", "-p", remote_path])

    def rm(self, remote_path: str, ignore_missing: bool = True) -> None:
        proc = self._run(
            ["-rm", "-r", "-skipTrash", remote_path],
            check=False,
        )

        if proc.returncode == 0:
            return

        if ignore_missing and self._is_missing_path_error(proc.stderr):
            return

        raise AssertionError(
            "HDFS rm failed: path={}, stdout={!r}, stderr={!r}".format(
                remote_path,
                proc.stdout,
                proc.stderr,
            )
        )

    def put_file(self, local_path: str, remote_path: str) -> None:
        parent = posixpath.dirname(remote_path)
        self.mkdir(parent)

        if self.put_supports_force:
            self._run(["-put", "-f", local_path, remote_path])
            return

        self.rm(remote_path, ignore_missing=True)
        self._run(["-put", local_path, remote_path])

    def sync_partition_to_local(self, partition_id: str) -> str:
        remote_dir = self.remote_partition_dir(partition_id)
        local_dir = self.local_partition_dir(partition_id)

        entries = self.ls(remote_dir)
        if os.path.exists(local_dir):
            shutil.rmtree(local_dir)

        os.makedirs(local_dir, exist_ok=True)

        for entry in entries:
            local_path = os.path.join(local_dir, entry.name)

            if entry.kind == "dir":
                os.makedirs(local_path, exist_ok=True)
                continue

            self.get_file(entry.path, local_path)

        return local_dir

    def snapshot_local_partition(self, partition_id: str) -> Dict[str, LocalMeta]:
        local_dir = self.local_partition_dir(partition_id)
        result: Dict[str, LocalMeta] = {}

        if not os.path.isdir(local_dir):
            return result

        for entry in os.scandir(local_dir):
            if entry.is_dir(follow_symlinks=False):
                result[entry.name] = ("dir", 0, "")
                continue

            if not entry.is_file(follow_symlinks=False):
                continue

            result[entry.name] = (
                "file",
                entry.stat().st_size,
                self._sha256_file(entry.path),
            )

        return result

    def sync_local_changes_to_hdfs(
        self,
        partition_id: str,
        before: Dict[str, LocalMeta],
    ) -> None:
        remote_dir = self.remote_partition_dir(partition_id)
        after = self.snapshot_local_partition(partition_id)

        for name in sorted(set(before) - set(after)):
            self.rm(posixpath.join(remote_dir, name), ignore_missing=True)

        for name, meta in sorted(after.items()):
            remote_path = posixpath.join(remote_dir, name)
            local_path = os.path.join(local_dir, name)
            before_meta: Optional[LocalMeta] = before.get(name)

            if meta == before_meta:
                continue

            if meta[0] == "dir":
                self.rm(remote_path, ignore_missing=True)
                self.mkdir(remote_path)
                continue

            self.rm(remote_path, ignore_missing=True)
            self.put_file(local_path, remote_path)

    def _sha256_file(self, path: str) -> str:
        digest = hashlib.sha256()

        with open(path, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)

        return digest.hexdigest()

    def _is_missing_path_error(self, stderr: str) -> bool:
        stderr = stderr or ""
        missing_markers = [
            "No such file or directory",
            "does not exist",
            "File does not exist",
        ]

        return any(marker in stderr for marker in missing_markers)
