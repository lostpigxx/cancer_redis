# test_s4_sst_data_block_corrupted.py

import time

import pytest

import env as test_env
from dbrepair_at_lib import CORRUPTED_STATES, OPEN_STATES, RepairAT


def test_s4_sst_data_block_corrupted_and_repair_if_open_fails():
    """
    S4：SST data block 损坏。

    用例自己构造较大的 SST：
      write large data -> flushmem -> wait new SST -> corrupt data block area

    注意：
      SST data block 损坏不一定在 RocksDB Open 阶段暴露。
      如果当前 RocksDB 配置只在 Get/Iterator 读取 block 时校验 checksum，
      那么重启后 partition 可能仍然是 opened。
      这种情况下本用例会失败并明确提示原因。
    """
    ctx = RepairAT()

    # ---------- T1：初始状态 ----------

    ctx.enable_heartbeat()
    ctx.assert_all_partitions_opened()

    # ---------- T2：选择目标 partition ----------

    target = ctx.pick_target_partition()
    owners_before = ctx.snapshot_owners()

    print("target partition: {}".format(target))

    # ---------- T3：构造目标 partition 的大 SST ----------

    tag = ctx.hashtag_for(
        partition=target,
        prefix="s4-target",
    )

    target_expected, new_ssts = ctx.prepare_sst_for_partition(
        target=target,
        tag=tag,
        key_prefix="s4:target",
        count=test_env.SST_DATA_BLOCK_WRITE_COUNT,
        value_size=test_env.SST_DATA_BLOCK_VALUE_SIZE,
    )

    sst_file = ctx.pick_largest_sst_from(
        new_ssts,
        min_size=test_env.SST_DATA_BLOCK_MIN_FILE_SIZE,
    )

    print("selected SST file to corrupt data block: {}".format(sst_file))

    # ---------- T4：写入并落盘 guard 数据 ----------

    guards = ctx.write_guard_strings(
        exclude_partition_id=target.partition_id,
        prefix="s4:guard",
    )

    ctx.flushmem_all_partitions()
    ctx.assert_values_exact(guards)

    # ---------- T5：关闭 heartbeat，停止目标 shardsvr，损坏 SST data block 区域 ----------

    with ctx.heartbeat_disabled():
        ctx.kill_shardsvr(target.shard_port)
        ctx.assert_pinned(target)

        ctx.corrupt_sst_data_block_area(sst_file)

        ctx.start_shardsvr(target.shard_port)

    # ---------- T6：等待 corrupted 或 opened ----------

    state_after_restart = ctx.wait_state_in(
        partition_id=target.partition_id,
        expected_states=CORRUPTED_STATES | OPEN_STATES,
        timeout_sec=30,
    )

    # data block 损坏可能不是 Open-time corruption。
    # 如果仍然 opened，说明当前配置不适合用 data block 损坏验证 open-fail repair 链路。
    if state_after_restart.state in OPEN_STATES:
        ctx.assert_pinned(target)
        ctx.assert_owners_unchanged(owners_before)
        ctx.assert_values_exact(guards)

        raise AssertionError(
            "SST data block corruption did not make RocksDB Open fail. "
            "partition={}, state={}. "
            "This means current RocksDB/options likely verify this corruption only "
            "when the block is read by Get/Iterator/Compaction, not during Open. "
            "For stable open-fail repair AT, use SST missing/truncate/footer corruption."
            .format(
                target.partition_id,
                state_after_restart.state,
            )
        )

    # ---------- T7：进入 corrupted 后，验证不迁移、不扩散 ----------

    corrupted = state_after_restart

    assert corrupted.owner == target.owner
    assert corrupted.shard_port == target.shard_port

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
        key_prefix="s4:after-repair",
        value="after-repair-value",
    )

    ctx.assert_all_partitions_opened()


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
