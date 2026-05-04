# test_m5_manifest_middle_corrupted.py

import os
import time

import pytest

from dbrepair_at_lib import RepairAT


def _manifest_middle_corruption_length(path):
    size = os.path.getsize(path)

    assert size > 2, (
        "MANIFEST file too small to corrupt middle: path={}, size={}".format(
            path,
            size,
        )
    )

    return max(1, min(4096, (size - 1) // 3))


def test_m5_manifest_middle_corrupted_and_repair():
    """
    M5：MANIFEST 中间损坏。

    目标：
      1. 覆盖目标 partition 当前 MANIFEST 文件中间一段；
      2. 原 shardsvr 重启时，该 partition Open 失败；
      3. cfgsvr 将该 partition 标记为 corrupted；
      4. corrupted partition pin 在原 shardsvr，不迁移；
      5. dbrepair auto 修复该 partition；
      6. 修复后 partition 恢复 opened，并可继续通过 proxy 写入读取。
    """
    ctx = RepairAT()

    # ---------- T1：确认初始状态 ----------

    ctx.enable_heartbeat()
    ctx.assert_all_partitions_opened()

    # ---------- T2：选择目标 partition，并记录初始 owner ----------

    target = ctx.pick_target_partition()
    owners_before = ctx.snapshot_owners()

    print("target partition: {}".format(target))

    # ---------- T3：构造目标 partition 的 MANIFEST 记录和 SST 数据 ----------

    tag = ctx.hashtag_for(
        partition=target,
        prefix="m5-target",
    )

    target_expected, new_ssts = ctx.prepare_sst_for_partition(
        target=target,
        tag=tag,
        key_prefix="m5:target",
        count=256,
        value_size=512,
    )

    print("target SST files generated before MANIFEST corruption: {}".format(new_ssts))

    manifest_path = ctx.current_manifest_path(target)

    assert os.path.exists(manifest_path), (
        "current MANIFEST should exist before fault injection: {}".format(
            manifest_path,
        )
    )

    # ---------- T4：写入非目标 partition guard 数据，并 flush ----------

    guards = ctx.write_guard_strings(
        exclude_partition_id=target.partition_id,
        prefix="m5:guard",
    )

    ctx.flushmem_partitions_except(
        excluded_partition_ids={target.partition_id}
    )

    ctx.assert_values_exact(guards)

    # ---------- T5：关闭 heartbeat，停止目标 shardsvr，损坏 MANIFEST 中间 ----------

    with ctx.heartbeat_disabled():
        ctx.kill_shardsvr(target.shard_port)
        ctx.assert_pinned(target)

        length = _manifest_middle_corruption_length(manifest_path)
        old_size, offset, actual_length = ctx.overwrite_file_middle(
            manifest_path,
            length=length,
        )

        print(
            "corrupted MANIFEST middle: path={}, size={}, offset={}, length={}".format(
                manifest_path,
                old_size,
                offset,
                actual_length,
            )
        )

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
        key_prefix="m5:after-repair",
        value="after-repair-value",
    )

    ctx.assert_all_partitions_opened()


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
