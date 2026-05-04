# current_fault_case_lib.py

import glob
import os
import re
import time

from dbrepair_at_lib import RepairAT


def pick_partition_file(ctx, target, pattern):
    db_dir = ctx.partition_db_dir(target)
    candidates = sorted(
        path for path in glob.glob(os.path.join(db_dir, pattern))
        if os.path.isfile(path)
    )

    assert candidates, (
        "no partition file matched pattern: partition={}, pattern={}, dir={}".format(
            target.partition_id,
            pattern,
            db_dir,
        )
    )

    return candidates[0]


def create_empty_plain_file(ctx, target, file_name):
    path = os.path.join(ctx.partition_db_dir(target), file_name)

    with open(path, "w") as f:
        f.flush()
        os.fsync(f.fileno())

    return path


def create_old_incomplete_manifest(ctx, target, current_manifest):
    db_dir = ctx.partition_db_dir(target)
    m = re.match(r"^MANIFEST-(\d+)$", current_manifest)

    assert m is not None, (
        "CURRENT does not point to a numbered MANIFEST before test: {}".format(
            current_manifest,
        )
    )

    current_number = int(m.group(1))

    for manifest_number in range(current_number - 1, -1, -1):
        name = "MANIFEST-{:06d}".format(manifest_number)
        path = os.path.join(db_dir, name)

        if not os.path.exists(path):
            with open(path, "wb") as f:
                f.write(b"DBREPAIR_AT_INCOMPLETE_MANIFEST")
                f.flush()
                os.fsync(f.fileno())

            return path

    raise AssertionError(
        "no free old MANIFEST name before current MANIFEST: {}".format(
            current_manifest,
        )
    )


def assert_current_points_to_existing_manifest(ctx, target):
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


def run_current_fault_case(case_name, inject_fault):
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

    assert_current_points_to_existing_manifest(ctx, target)

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
