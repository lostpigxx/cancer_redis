# test_m7_current_random_string.py

import os
import time

import pytest

from dbrepair_at_lib import RepairAT


def _assert_current_points_to_existing_manifest(ctx, target):
    current_manifest = ctx.read_current_manifest_name(target)

    assert current_manifest.startswith("MANIFEST-"), (
        "CURRENT should point to a MANIFEST file after repair, actual={}".format(
            current_manifest,
        )
    )

    manifest_path = ctx.current_manifest_path(target)

    assert os.path.exists(manifest_path), (
        "manifest pointed by CURRENT does not exist after repair: {}".format(
            manifest_path,
        )
    )


def _run_current_fault_case(case_name, inject_fault):
    ctx = RepairAT()

    # ---------- T1: confirm initial state ----------

    ctx.enable_heartbeat()
    ctx.assert_all_partitions_opened()

    # ---------- T2: choose target partition ----------

    target = ctx.pick_target_partition()
    owners_before = ctx.snapshot_owners()

    print("target partition: {}".format(target))

    # ---------- T3: prepare target SST data ----------

    tag = ctx.hashtag_for(
        partition=target,
        prefix="{}-target".format(case_name),
    )

    target_expected, new_ssts = ctx.prepare_sst_for_partition(
        target=target,
        tag=tag,
        key_prefix="{}:target".format(case_name),
        count=256,
        value_size=512,
    )

    print("target SST files generated before CURRENT fault: {}".format(new_ssts))

    current_manifest = ctx.read_current_manifest_name(target)
    current_path = ctx.current_file_path(target)

    assert os.path.exists(current_path), (
        "CURRENT file should exist before fault injection: {}".format(current_path)
    )

    # ---------- T4: write non-target guard data ----------

    guards = ctx.write_guard_strings(
        exclude_partition_id=target.partition_id,
        prefix="{}:guard".format(case_name),
    )

    ctx.flushmem_partitions_except(
        excluded_partition_ids={target.partition_id}
    )

    ctx.assert_values_exact(guards)

    # ---------- T5: stop target shardsvr and inject CURRENT fault ----------

    with ctx.heartbeat_disabled():
        ctx.kill_shardsvr(target.shard_port)
        ctx.assert_pinned(target)

        injected_description = inject_fault(ctx, target, current_manifest)
        print(
            "injected CURRENT fault: case={}, description={}".format(
                case_name,
                injected_description,
            )
        )

        ctx.start_shardsvr(target.shard_port)

    # ---------- T6: wait corrupted ----------

    corrupted = ctx.wait_corrupted(
        target=target,
        timeout_sec=30,
    )

    assert corrupted.owner == target.owner
    assert corrupted.shard_port == target.shard_port

    # ---------- T7: verify no migration or spread ----------

    time.sleep(3)

    ctx.assert_pinned(target)
    ctx.assert_owners_unchanged(owners_before)
    ctx.assert_values_exact(guards)
    ctx.wait_all_shards_ping()

    # ---------- T8: repair ----------

    ctx.repair_and_wait_opened(
        target=target,
        timeout_sec=60,
    )

    # ---------- T9: verify after repair ----------

    _assert_current_points_to_existing_manifest(ctx, target)

    ctx.assert_pinned(target)
    ctx.assert_owners_unchanged(owners_before)
    ctx.assert_values_exact(guards)

    # CURRENT metadata faults may make repair rebuild metadata and drop target
    # historical SST data. Missing is allowed, wrong values are not.
    ctx.assert_values_missing_or_exact(target_expected)

    ctx.write_one_and_assert(
        tag=tag,
        key_prefix="{}:after-repair".format(case_name),
        value="after-repair-value",
    )

    ctx.assert_all_partitions_read_write(
        prefix="{}:after-repair:all-partitions".format(case_name),
    )

    ctx.assert_all_partitions_opened()


def test_m7_current_random_string_and_repair():
    """
    M7: CURRENT content is a random string, not MANIFEST-xxxxxx.
    """

    def inject_fault(ctx, target, current_manifest):
        content = "DBREPAIR_AT_RANDOM_CURRENT_CONTENT\n"
        ctx.write_current_file_content(target, content)
        return "CURRENT content={!r}".format(content)

    _run_current_fault_case("test_m7_current_random_string", inject_fault)


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
