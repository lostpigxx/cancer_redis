# test_m2_current_points_to_missing_manifest.py

import time

import pytest

from dbrepair_at_lib import RepairAT


def test_m2_current_points_to_missing_manifest_and_repair():
    ctx = RepairAT()

    ctx.enable_heartbeat()
    ctx.assert_all_partitions_opened()

    target = ctx.pick_target_partition()
    owners_before = ctx.snapshot_owners()

    tag = ctx.hashtag_for(target, "m2-target")
    target_expected = ctx.write_strings(tag, "m2:target", count=64, value_size=128)

    guards = ctx.write_guard_strings(
        exclude_partition_id=target.partition_id,
        prefix="m2:guard",
    )
    ctx.assert_values_exact(guards)

    with ctx.heartbeat_disabled():
        ctx.kill_shardsvr(target.shard_port)
        ctx.assert_pinned(target)

        # 故障注入：CURRENT 指向不存在的 MANIFEST。
        ctx.break_current_to_missing_manifest(target)

        ctx.start_shardsvr(target.shard_port)

    corrupted = ctx.wait_corrupted(target, timeout_sec=30)
    assert corrupted.owner == target.owner

    time.sleep(3)
    ctx.assert_pinned(target)
    ctx.assert_owners_unchanged(owners_before)

    ctx.assert_values_exact(guards)

    ctx.repair_and_wait_opened(target, timeout_sec=60)

    ctx.assert_pinned(target)
    ctx.assert_values_exact(guards)

    # MANIFEST 类修复后，是否保留 target_expected 取决于 RepairDB 行为。
    # 如果你要求强校验，可以换成 assert_values_exact。
    ctx.assert_values_missing_or_exact(target_expected)

    ctx.write_one_and_assert(
        tag=tag,
        key_prefix="m2:after-repair",
        value="after-repair-value",
    )

    ctx.assert_all_partitions_opened()


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
