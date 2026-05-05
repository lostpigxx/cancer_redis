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
        self.chroot_dir = self._detect_chroot_dir(self.dfs_command)
        self.command_local_staging_dir = os.path.abspath(local_staging_dir)
        self.local_staging_dir = self._host_local_path(
            self.command_local_staging_dir,
        )
        self.partition_dir_template = partition_dir_template
        self.put_supports_force = put_supports_force

        assert self.dfs_command, "HDFS_DFS_COMMAND must not be empty"
        assert self.remote_base_path.startswith("/"), (
            "BASE_PATH must be an absolute HDFS path: {}".format(remote_base_path)
        )

        self._ensure_local_dir(self.local_staging_dir)

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
        local_dir = os.path.dirname(local_path)
        self._ensure_local_dir(local_dir)

        if os.path.exists(local_path):
            if os.path.isdir(local_path) and not os.path.islink(local_path):
                shutil.rmtree(local_path)
            else:
                os.remove(local_path)

        downloaded_path = os.path.join(local_dir, posixpath.basename(remote_path))
        if downloaded_path != local_path and os.path.exists(downloaded_path):
            if os.path.isdir(downloaded_path) and not os.path.islink(downloaded_path):
                shutil.rmtree(downloaded_path)
            else:
                os.remove(downloaded_path)

        self._run(["-get", remote_path, self._command_local_path(local_dir)])

        assert os.path.isfile(downloaded_path), (
            "HDFS get did not create local file: remote_path={}, expected_local_path={}".format(
                remote_path,
                downloaded_path,
            )
        )

        if downloaded_path != local_path:
            os.rename(downloaded_path, local_path)

        self._assert_downloaded_file(remote_path, local_path)

    def get_dir(self, remote_dir: str, local_dir: str) -> None:
        local_parent = os.path.dirname(local_dir)
        downloaded_dir = os.path.join(
            local_parent,
            posixpath.basename(remote_dir.rstrip("/")),
        )

        self._ensure_local_dir(local_parent)

        if os.path.exists(local_dir):
            shutil.rmtree(local_dir)

        if downloaded_dir != local_dir and os.path.exists(downloaded_dir):
            shutil.rmtree(downloaded_dir)

        self._run(["-get", remote_dir, self._command_local_path(local_parent)])

        assert os.path.isdir(downloaded_dir), (
            "HDFS get did not create local directory: remote_dir={}, expected_local_dir={}".format(
                remote_dir,
                downloaded_dir,
            )
        )

        if downloaded_dir != local_dir:
            os.rename(downloaded_dir, local_dir)

        self._assert_partition_staging_dir(remote_dir, local_dir)

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
        command_local_path = self._command_local_path(local_path)

        if self.put_supports_force:
            self._run(["-put", "-f", command_local_path, remote_path])
            return

        self.rm(remote_path, ignore_missing=True)
        self._run(["-put", command_local_path, remote_path])

    def sync_partition_to_local(self, partition_id: str) -> str:
        remote_dir = self.remote_partition_dir(partition_id)
        local_dir = self.local_partition_dir(partition_id)

        self.get_dir(remote_dir, local_dir)
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
    ) -> List[str]:
        local_dir = self.local_partition_dir(partition_id)
        remote_dir = self.remote_partition_dir(partition_id)
        after = self.snapshot_local_partition(partition_id)
        changed_paths: List[str] = []

        for name in sorted(set(before) - set(after)):
            remote_path = posixpath.join(remote_dir, name)
            self.rm(remote_path, ignore_missing=True)
            changed_paths.append(remote_path)

        for name, meta in sorted(after.items()):
            remote_path = posixpath.join(remote_dir, name)
            local_path = os.path.join(local_dir, name)
            before_meta: Optional[LocalMeta] = before.get(name)

            if meta == before_meta:
                continue

            if meta[0] == "dir":
                self.rm(remote_path, ignore_missing=True)
                self.mkdir(remote_path)
                changed_paths.append(remote_path)
                continue

            self.rm(remote_path, ignore_missing=True)
            self.put_file(local_path, remote_path)
            changed_paths.append(remote_path)

        return changed_paths

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

    def _assert_downloaded_file(
        self,
        remote_path: str,
        local_path: str,
    ) -> None:
        assert os.path.isfile(local_path), (
            "HDFS get did not create final local file: remote_path={}, local_path={}".format(
                remote_path,
                local_path,
            )
        )

    def _assert_partition_staging_dir(self, remote_dir: str, local_dir: str) -> None:
        assert os.path.isdir(local_dir), (
            "HDFS get did not create final local partition dir: remote_dir={}, local_dir={}".format(
                remote_dir,
                local_dir,
            )
        )

        names = set(os.listdir(local_dir))
        assert names, (
            "HDFS get created empty local partition dir: remote_dir={}, local_dir={}".format(
                remote_dir,
                local_dir,
            )
        )

        assert "CURRENT" in names, (
            "HDFS get local partition dir missing CURRENT: remote_dir={}, local_dir={}, files={}".format(
                remote_dir,
                local_dir,
                sorted(names),
            )
        )

        assert any(name.startswith("MANIFEST-") for name in names), (
            "HDFS get local partition dir missing MANIFEST: remote_dir={}, local_dir={}, files={}".format(
                remote_dir,
                local_dir,
                sorted(names),
            )
        )

    def _ensure_local_dir(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)

        if self.chroot_dir:
            os.chmod(path, 0o777)

    def _detect_chroot_dir(self, command: List[str]) -> Optional[str]:
        if not command:
            return None

        if os.path.basename(command[0]) != "chroot":
            return None

        i = 1
        while i < len(command):
            arg = command[i]

            if arg in ("--userspec", "--groups"):
                i += 2
                continue

            if arg.startswith("-"):
                i += 1
                continue

            return os.path.abspath(arg)

        return None

    def _host_local_path(self, command_path: str) -> str:
        command_path = os.path.abspath(command_path)

        if not self.chroot_dir:
            return command_path

        chroot_dir = self.chroot_dir.rstrip(os.sep)
        if command_path == chroot_dir or command_path.startswith(chroot_dir + os.sep):
            return command_path

        return os.path.join(chroot_dir, command_path.lstrip(os.sep))

    def _command_local_path(self, local_path: str) -> str:
        local_path = os.path.abspath(local_path)

        if not self.chroot_dir:
            return local_path

        chroot_dir = self.chroot_dir.rstrip(os.sep)
        assert local_path == chroot_dir or local_path.startswith(chroot_dir + os.sep), (
            "local path is not inside HDFS chroot staging dir: path={}, chroot={}".format(
                local_path,
                chroot_dir,
            )
        )

        rel = os.path.relpath(local_path, chroot_dir)
        if rel == ".":
            return "/"

        return "/" + rel.replace(os.sep, "/")
