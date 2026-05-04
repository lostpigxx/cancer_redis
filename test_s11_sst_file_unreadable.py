# test_s11_sst_file_unreadable.py

import os
import time

import pytest

from dbrepair_at_lib import RepairAT


def test_s11_sst_file_unreadable_and_repair():
    """
    S11：SST 文件权限不可读。

    用例自己构造 SST：
      write -> flushmem -> wait new SST -> chmod 000 SST

    注意：
      测试结束前会尽力恢复文件权限，避免失败时留下 000 文件。
    """
    ctx = RepairAT()
    sst_file = None
    old_mode = None

    try:
        # ---------- T1：初始状态 ----------

        ctx.enable_heartbeat()
        ctx.assert_all_partitions_opened()

        # ---------- T2：选择目标 partition ----------

        target = ctx.pick_target_partition()
        owners_before = ctx.snapshot_owners()

        print("target partition: {}".format(target))

        # ---------- T3：构造目标 partition 的 SST ----------

        tag = ctx.hashtag_for(
            partition=target,
            prefix="s11-target",
        )

        target_expected, new_ssts = ctx.prepare_sst_for_partition(
            target=target,
            tag=tag,
            key_prefix="s11:target",
        )

        print("new SST files after target flush: {}".format(new_ssts))

        # ---------- T4：写入并落盘 guard 数据 ----------

        guards = ctx.write_guard_strings(
            exclude_partition_id=target.partition_id,
            prefix="s11:guard",
        )

        ctx.flushmem_partitions_except({target.partition_id})
        ctx.assert_values_exact(guards)

        # ---------- T5：关闭 heartbeat，停止目标 shardsvr，chmod 000 SST ----------

        with ctx.heartbeat_disabled():
            ctx.kill_shardsvr(target.shard_port)
            ctx.assert_pinned(target)

            sst_file = ctx.pick_largest_live_sst(
                target=target,
                preferred_files=new_ssts,
            )

            print("selected live SST file to chmod 000: {}".format(sst_file))

            old_mode = ctx.chmod_sst_file(sst_file, 0)

            ctx.start_shardsvr(target.shard_port)

        # ---------- T6：等待 corrupted ----------

        corrupted = ctx.wait_corrupted(
            target=target,
            timeout_sec=30,
        )

        assert corrupted.owner == target.owner
        assert corrupted.shard_port == target.shard_port

        # ---------- T7：验证不迁移、不扩散 ----------

        time.sleep(3)

        ctx.assert_pinned(target)
        ctx.assert_owners_unchanged(owners_before)
        ctx.assert_values_exact(guards)
        ctx.wait_all_shards_ping()

        # ---------- T8：repair ----------

        ctx.repair_and_wait_opened(
            target=target,
            timeout_sec=60,
        )

        # ---------- T9：修复后验证 ----------

        ctx.assert_pinned(target)
        ctx.assert_owners_unchanged(owners_before)

        ctx.assert_values_exact(guards)
        ctx.assert_values_missing_or_exact(target_expected)

        ctx.write_one_and_assert(
            tag=tag,
            key_prefix="s11:after-repair",
            value="after-repair-value",
        )

        ctx.assert_all_partitions_opened()
    finally:
        if sst_file is not None and old_mode is not None and os.path.isfile(sst_file):
            os.chmod(sst_file, old_mode)


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
