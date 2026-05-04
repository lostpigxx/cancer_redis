# test_s14_multiple_sst_files_partially_corrupted.py

import os
import time

import pytest

import env as test_env
from dbrepair_at_lib import RepairAT


def test_s14_multiple_sst_files_partially_corrupted_and_repair():
    """
    S14：多个 SST 同时部分损坏。

    用例自己构造多个 SST：
      one SST footer corrupted + one SST data block corrupted
    """
    ctx = RepairAT()

    # ---------- T1：初始状态 ----------

    ctx.enable_heartbeat()
    ctx.assert_all_partitions_opened()

    # ---------- T2：选择目标 partition ----------

    target = ctx.pick_target_partition()
    owners_before = ctx.snapshot_owners()

    print("target partition: {}".format(target))

    # ---------- T3：构造目标 partition 的多个 SST ----------

    data_tag = ctx.hashtag_for(
        partition=target,
        prefix="s14-data-target",
    )

    data_expected, data_ssts = ctx.prepare_sst_for_partition(
        target=target,
        tag=data_tag,
        key_prefix="s14:data-target",
        count=test_env.SST_DATA_BLOCK_WRITE_COUNT,
        value_size=test_env.SST_DATA_BLOCK_VALUE_SIZE,
    )

    footer_tag = ctx.hashtag_for(
        partition=target,
        prefix="s14-footer-target",
    )

    footer_expected, footer_ssts = ctx.prepare_sst_for_partition(
        target=target,
        tag=footer_tag,
        key_prefix="s14:footer-target",
    )

    target_expected = {}
    target_expected.update(data_expected)
    target_expected.update(footer_expected)
    new_ssts = data_ssts + footer_ssts

    print("new SST files after target flushes: {}".format(new_ssts))

    # ---------- T4：写入并落盘 guard 数据 ----------

    guards = ctx.write_guard_strings(
        exclude_partition_id=target.partition_id,
        prefix="s14:guard",
    )

    ctx.flushmem_partitions_except({target.partition_id})
    ctx.assert_values_exact(guards)

    # ---------- T5：关闭 heartbeat，停止目标 shardsvr，损坏两个 SST ----------

    with ctx.heartbeat_disabled():
        ctx.kill_shardsvr(target.shard_port)
        ctx.assert_pinned(target)

        sst_files = ctx.pick_largest_live_ssts(
            target=target,
            preferred_files=new_ssts,
            count=2,
        )

        data_sst = None
        for sst_file in sst_files:
            if os.path.getsize(sst_file) >= test_env.SST_DATA_BLOCK_MIN_FILE_SIZE:
                data_sst = sst_file
                break

        assert data_sst is not None, (
            "no SST large enough for data block corruption: files={}".format(
                [(p, os.path.getsize(p)) for p in sst_files],
            )
        )

        footer_sst = next(p for p in sst_files if p != data_sst)

        print("selected SST file to corrupt footer: {}".format(footer_sst))
        print("selected SST file to corrupt data block: {}".format(data_sst))

        ctx.corrupt_sst_tail(footer_sst)
        ctx.corrupt_sst_data_block_area(data_sst)

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
        tag=data_tag,
        key_prefix="s14:after-repair",
        value="after-repair-value",
    )

    ctx.assert_all_partitions_read_write(
        prefix="test_s14_multiple_sst_files_partially_corrupted:after-repair:all-partitions",
    )

    ctx.assert_all_partitions_opened()


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
