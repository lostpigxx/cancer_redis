# test_s15_complex_type_sst_corrupted.py

import os
import time

import pytest

from dbrepair_at_lib import RepairAT


def test_s15_complex_type_sst_corrupted_and_repair():
    """
    S15：损坏复合类型对应的 SST。

    覆盖 hash / set / zset / list。repair 后允许受损数据丢失；
    但如果复合类型 key 仍存在，必须保持正确 type 和完整内容。
    """
    ctx = RepairAT()

    # ---------- T1：初始状态 ----------

    ctx.enable_heartbeat()
    ctx.assert_all_partitions_opened()

    # ---------- T2：选择目标 partition ----------

    target = ctx.pick_target_partition()
    owners_before = ctx.snapshot_owners()

    print("target partition: {}".format(target))

    # ---------- T3：构造目标 partition 的复合类型 SST ----------

    tag = ctx.hashtag_for(
        partition=target,
        prefix="s15-complex-target",
    )

    before_ssts = ctx.sst_snapshot(target)

    complex_expected = ctx.write_complex_values(
        tag=tag,
        key_prefix="s15:complex-target",
        count=128,
    )

    sample_key = next(iter(complex_expected))
    ctx.assert_key_routes_to_partition(sample_key, target)

    ctx.flushmem(target)

    new_ssts = ctx.wait_new_sst_files(
        target=target,
        before_snapshot=before_ssts,
    )
    time.sleep(1)
    settled_new_ssts = [
        p for p in ctx.sst_files(target)
        if p not in before_ssts
    ]
    if settled_new_ssts:
        new_ssts = settled_new_ssts

    ctx.assert_complex_values_exact(complex_expected)

    print("new SST files after complex target flush: {}".format(new_ssts))

    # ---------- T4：写入并落盘 guard 数据 ----------

    guards = ctx.write_guard_strings(
        exclude_partition_id=target.partition_id,
        prefix="s15:guard",
    )

    ctx.flushmem_partitions_except({target.partition_id})
    ctx.assert_values_exact(guards)

    # ---------- T5：关闭 heartbeat，停止目标 shardsvr，损坏复合类型 SST ----------

    with ctx.heartbeat_disabled():
        ctx.kill_shardsvr(target.shard_port)
        ctx.assert_pinned(target)

        preferred_live_ssts = [
            p for p in new_ssts
            if os.path.isfile(p) and os.path.getsize(p) > 8192
        ]

        if preferred_live_ssts:
            sst_files = sorted(
                preferred_live_ssts,
                key=os.path.getsize,
                reverse=True,
            )
        else:
            sst_files = ctx.pick_largest_live_ssts(
                target=target,
                preferred_files=new_ssts,
                count=1,
                min_size=8193,
            )

        print("selected complex type SST files to corrupt: {}".format(sst_files))

        for sst_file in sst_files:
            ctx.corrupt_sst_tail(sst_file)

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
    ctx.assert_complex_values_missing_or_exact(complex_expected)

    ctx.write_one_and_assert(
        tag=tag,
        key_prefix="s15:after-repair",
        value="after-repair-value",
    )

    ctx.assert_all_partitions_read_write(
        prefix="test_s15_complex_type_sst_corrupted:after-repair:all-partitions",
    )

    ctx.assert_all_partitions_opened()


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
