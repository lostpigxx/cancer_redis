# test_w3_wal_middle_loss_open_fail_repair.py

import time

import pytest

from dbrepair_at_lib import RepairAT


def test_w3_wal_middle_loss_open_fail_and_repair():
    """
    W3：WAL 中间丢失，要求 RocksDB Open 失败。

    目标：
      1. WAL 中间丢失后，目标 partition open 失败；
      2. cfgsvr 标记该 partition 为 corrupted；
      3. corrupted partition pin 在原 shardsvr，不迁移；
      4. dbrepair auto 修复该 partition；
      5. 修复后 partition 恢复 opened，并且可以继续通过 proxy 写入读取。

    前提：
      测试环境需要使用能让 WAL corruption 导致 Open 失败的 WAL recovery mode。
      例如 kAbsoluteConsistency。
    """
    ctx = RepairAT()

    # ---------- T1：确认集群初始状态 ----------

    ctx.enable_heartbeat()
    ctx.assert_all_partitions_opened()

    # ---------- T2：选择目标 partition，并记录初始 owner ----------

    target = ctx.pick_target_partition()
    owners_before = ctx.snapshot_owners()

    print("target partition: {}".format(target))

    # ---------- T3：构造落在目标 partition 上的 hashtag ----------

    target_tag = ctx.hashtag_for(
        partition=target,
        prefix="w3-target",
    )

    # ---------- T4：记录目标 WAL 写入前快照 ----------

    wal_before = ctx.wal_snapshot(target)
    ctx.print_wal_snapshot("WAL before target writes:", wal_before)

    # ---------- T5：向目标 partition 写入 WAL 负载数据 ----------

    target_expected = ctx.write_strings(
        tag=target_tag,
        key_prefix="w3:target",
    )

    # 确认 hashtag 确实把 key 路由到了目标 partition。
    ctx.assert_key_routes_to_partition(
        key="w3:sample:{}".format(target_tag),
        target=target,
    )

    # ---------- T6：记录目标 WAL 写入后快照 ----------

    wal_after = ctx.wal_snapshot(target)
    ctx.print_wal_snapshot("WAL after target writes:", wal_after)

    # ---------- T7：给非目标 partition 写 guard key ----------

    guards = ctx.write_guard_strings(
        exclude_partition_id=target.partition_id,
        prefix="w3:guard",
    )

    ctx.assert_values_exact(guards)

    # ---------- T8：关闭 heartbeat，停止目标 shardsvr，离线注入 WAL 中间丢失 ----------

    with ctx.heartbeat_disabled():
        # 关闭 heartbeat 后停止 shardsvr，避免 cfgsvr 触发 partition 迁移。
        ctx.kill_shardsvr(target.shard_port)

        # 此时 cfgsvr 不应迁移 partition。
        ctx.assert_pinned(target)

        # 只破坏写入后确实增长过的 WAL。
        ctx.inject_wal_middle_loss(
            target=target,
            before=wal_before,
            after=wal_after,
        )

        # 原地重启目标 shardsvr。
        ctx.start_shardsvr(target.shard_port)

    # ---------- T9：等待 cfgsvr 感知目标 partition corrupted ----------

    corrupted = ctx.wait_corrupted(
        target=target,
        timeout_sec=30,
    )

    assert corrupted.owner == target.owner
    assert corrupted.shard_port == target.shard_port

    # ---------- T10：验证 corrupted partition 不迁移 ----------

    time.sleep(3)

    ctx.assert_pinned(target)
    ctx.assert_owners_unchanged(owners_before)

    # 非目标 partition 仍然可读。
    ctx.assert_values_exact(guards)

    # 所有 shardsvr 进程仍然存活。
    ctx.wait_all_shards_ping()

    # ---------- T11：执行 dbrepair auto，并等待恢复 opened ----------

    ctx.repair_and_wait_opened(
        target=target,
        timeout_sec=60,
    )

    # ---------- T12：修复后再次确认 owner 不变 ----------

    ctx.assert_pinned(target)
    ctx.assert_owners_unchanged(owners_before)

    # ---------- T13：验证数据边界 ----------

    # 非目标 partition 数据必须完整。
    ctx.assert_values_exact(guards)

    # WAL 修复前写入的数据允许丢失，但如果存在，value 必须正确。
    ctx.assert_values_missing_or_exact(target_expected)

    # ---------- T14：验证修复后目标 partition 可继续服务 ----------

    ctx.write_one_and_assert(
        tag=target_tag,
        key_prefix="w3:after-repair",
        value="after-repair-value",
    )

    # ---------- T15：最终确认所有 partition opened ----------

    ctx.assert_all_partitions_read_write(
        prefix="test_w3_wal_middle_loss_open_fail_repair:after-repair:all-partitions",
    )

    ctx.assert_all_partitions_opened()


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
