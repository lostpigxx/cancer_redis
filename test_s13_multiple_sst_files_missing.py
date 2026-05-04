# test_s13_multiple_sst_files_missing.py

import time

import pytest

from dbrepair_at_lib import RepairAT


def test_s13_multiple_sst_files_missing_and_repair():
    """
    S13：多个 SST 同时缺失。

    用例自己构造多个 SST：
      repeat(write -> flushmem -> wait new SST) -> delete multiple SST
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

    target_expected = {}
    new_ssts = []
    tags = []

    for i in range(3):
        tag = ctx.hashtag_for(
            partition=target,
            prefix="s13-target-{}".format(i),
        )
        tags.append(tag)

        expected, generated_ssts = ctx.prepare_sst_for_partition(
            target=target,
            tag=tag,
            key_prefix="s13:target:{}".format(i),
        )

        target_expected.update(expected)
        new_ssts.extend(generated_ssts)

    print("new SST files after target flushes: {}".format(new_ssts))

    # ---------- T4：写入并落盘 guard 数据 ----------

    guards = ctx.write_guard_strings(
        exclude_partition_id=target.partition_id,
        prefix="s13:guard",
    )

    ctx.flushmem_partitions_except({target.partition_id})
    ctx.assert_values_exact(guards)

    # ---------- T5：关闭 heartbeat，停止目标 shardsvr，删除多个 SST ----------

    with ctx.heartbeat_disabled():
        ctx.kill_shardsvr(target.shard_port)
        ctx.assert_pinned(target)

        sst_files = ctx.pick_largest_live_ssts(
            target=target,
            preferred_files=new_ssts,
            count=2,
        )

        print("selected live SST files to delete: {}".format(sst_files))

        for sst_file in sst_files:
            ctx.delete_sst_file(sst_file)

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
        tag=tags[0],
        key_prefix="s13:after-repair",
        value="after-repair-value",
    )

    ctx.assert_all_partitions_opened()


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
