# test_m1_current_file_missing.py

import os
import time

import pytest

from dbrepair_at_lib import RepairAT


def test_m1_current_file_missing_and_repair():
    """
    M1：CURRENT 文件缺失。

    目标：
      1. 删除目标 partition RocksDB 目录下的 CURRENT 文件；
      2. 原 shardsvr 重启时，该 partition Open 失败；
      3. cfgsvr 将该 partition 标记为 corrupted；
      4. corrupted partition pin 在原 shardsvr，不迁移；
      5. dbrepair auto 修复该 partition；
      6. 修复后 partition 恢复 opened；
      7. 修复后 CURRENT 文件重新生成；
      8. 修复后 partition 可继续通过 proxy 写入读取。
    """
    ctx = RepairAT()

    # ---------- T1：确认初始状态 ----------

    ctx.enable_heartbeat()
    ctx.assert_all_partitions_opened()

    # ---------- T2：选择目标 partition，并记录初始 owner ----------

    target = ctx.pick_target_partition()
    owners_before = ctx.snapshot_owners()

    print("target partition: {}".format(target))

    # ---------- T3：构造目标 partition 的 hashtag ----------

    tag = ctx.hashtag_for(
        partition=target,
        prefix="m1-target",
    )

    # ---------- T4：写入目标数据，并 flush 成 SST ----------

    # CURRENT 缺失本身不会破坏 SST。
    # 为了让修复后有稳定数据可校验，这里主动构造目标 SST。
    target_expected, new_ssts = ctx.prepare_sst_for_partition(
        target=target,
        tag=tag,
        key_prefix="m1:target",
        count=256,
        value_size=512,
    )

    print("target SST files generated before CURRENT deletion: {}".format(new_ssts))

    # ---------- T5：写入非目标 partition guard 数据，并 flush ----------

    guards = ctx.write_guard_strings(
        exclude_partition_id=target.partition_id,
        prefix="m1:guard",
    )

    # guard 数据要尽量落盘，避免后续 kill 影响 guard 校验。
    ctx.flushmem_partitions_except(
        excluded_partition_ids={target.partition_id}
    )

    ctx.assert_values_exact(guards)

    # ---------- T6 ~ T9：关闭 heartbeat，停止目标 shardsvr，删除 CURRENT，原地启动 ----------

    current_path = ctx.current_file_path(target)
    assert os.path.exists(current_path), (
        "CURRENT file should exist before fault injection: {}".format(current_path)
    )

    with ctx.heartbeat_disabled():
        # 关闭 heartbeat 后 kill shardsvr，避免 cfgsvr 触发 partition 迁移。
        ctx.kill_shardsvr(target.shard_port)

        # heartbeat 已关闭，此时目标 partition 不应迁移。
        ctx.assert_pinned(target)

        # 故障注入：删除 CURRENT 文件。
        ctx.delete_current_file(target)

        assert not os.path.exists(current_path), (
            "CURRENT file should be deleted: {}".format(current_path)
        )

        print("deleted CURRENT file: {}".format(current_path))

        # 原地拉起目标 shardsvr。
        ctx.start_shardsvr(target.shard_port)

    # ---------- T10 ~ T11：等待 cfgsvr 感知目标 partition corrupted ----------

    corrupted = ctx.wait_corrupted(
        target=target,
        timeout_sec=30,
    )

    assert corrupted.owner == target.owner
    assert corrupted.shard_port == target.shard_port

    # ---------- T12：验证 corrupted partition 不迁移、不扩散 ----------

    time.sleep(3)

    ctx.assert_pinned(target)
    ctx.assert_owners_unchanged(owners_before)

    # 非目标 partition 数据必须完整。
    ctx.assert_values_exact(guards)

    # 其他 shardsvr 进程也应该仍然存活。
    ctx.wait_all_shards_ping()

    # ---------- T13 ~ T14：执行 dbrepair auto，并等待恢复 opened ----------

    ctx.repair_and_wait_opened(
        target=target,
        timeout_sec=60,
    )

    # ---------- T15：验证 CURRENT 文件重新生成 ----------

    assert os.path.exists(current_path), (
        "CURRENT file should be recreated after repair: {}".format(current_path)
    )

    current_manifest = ctx.read_current_manifest_name(target)

    assert current_manifest.startswith("MANIFEST-"), (
        "CURRENT should point to a MANIFEST file, actual={}".format(current_manifest)
    )

    manifest_path = ctx.current_manifest_path(target)

    assert os.path.exists(manifest_path), (
        "manifest pointed by CURRENT does not exist: {}".format(manifest_path)
    )

    print(
        "CURRENT recreated after repair: CURRENT -> {}, manifest_path={}".format(
            current_manifest,
            manifest_path,
        )
    )

    # ---------- T16：修复后验证 ----------

    ctx.assert_pinned(target)
    ctx.assert_owners_unchanged(owners_before)

    # 非目标 partition 数据必须完整。
    ctx.assert_values_exact(guards)

    # CURRENT 缺失不会产生错误值；但 repair 可能合法重建 metadata 并丢弃
    # 目标 partition 的历史 SST 数据，所以允许缺失、不允许错值。
    ctx.assert_values_missing_or_exact(target_expected)

    # 修复后目标 partition 必须可继续写入读取。
    ctx.write_one_and_assert(
        tag=tag,
        key_prefix="m1:after-repair",
        value="after-repair-value",
    )

    ctx.assert_all_partitions_opened()


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
