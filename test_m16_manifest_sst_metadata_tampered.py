# test_m16_manifest_sst_metadata_tampered.py

import os
import re
import subprocess
import time

import pytest

import env as test_env
from dbrepair_at_lib import RepairAT
from manifest_fault_common import assert_current_points_to_existing_manifest


_METADATA_TAMPER_CASES = [
    "missing_sst_reference",
    "wrong_file_number",
    "wrong_level",
    "wrong_key_range",
]


def _require_manifest_metadata_tamper_template():
    template = getattr(test_env, "MANIFEST_METADATA_TAMPER_COMMAND_TEMPLATE", [])

    if not template:
        pytest.skip(
            "MANIFEST metadata tamper requires "
            "env.MANIFEST_METADATA_TAMPER_COMMAND_TEMPLATE"
        )

    return template


def _sst_file_number(path):
    m = re.match(r"^(\d+)\.(sst|ldb)$", os.path.basename(path))

    assert m is not None, (
        "SST file name does not contain a RocksDB file number: {}".format(path)
    )

    return str(int(m.group(1)))


def _manifest_metadata_tamper_command(
    case_name,
    target,
    manifest_path,
    sst_path,
):
    template = _require_manifest_metadata_tamper_template()

    context = {
        "case_name": case_name,
        "partition_id": target.partition_id,
        "partition_db_dir": os.path.dirname(manifest_path),
        "manifest_path": manifest_path,
        "manifest_name": os.path.basename(manifest_path),
        "sst_path": sst_path,
        "sst_name": os.path.basename(sst_path),
        "sst_file_number": _sst_file_number(sst_path),
        "shard_port": target.shard_port,
    }

    return [str(item).format(**context) for item in template]


def _run_manifest_sst_metadata_tamper_case(case_name):
    _require_manifest_metadata_tamper_template()

    ctx = RepairAT()

    # ---------- T1: confirm initial state ----------

    ctx.enable_heartbeat()
    ctx.assert_all_partitions_opened()

    # ---------- T2: choose target partition ----------

    target = ctx.pick_target_partition()
    owners_before = ctx.snapshot_owners()

    print("target partition: {}".format(target))

    # ---------- T3: prepare target SST and current MANIFEST ----------

    tag = ctx.hashtag_for(
        partition=target,
        prefix="m16-{}-target".format(case_name),
    )

    target_expected, new_ssts = ctx.prepare_sst_for_partition(
        target=target,
        tag=tag,
        key_prefix="m16:{}:target".format(case_name),
        count=256,
        value_size=512,
    )

    print("target SST files generated before MANIFEST metadata tamper: {}".format(
        new_ssts,
    ))

    manifest_path = ctx.current_manifest_path(target)

    assert os.path.exists(manifest_path), (
        "current MANIFEST should exist before fault injection: {}".format(
            manifest_path,
        )
    )

    # ---------- T4: write non-target guard data ----------

    guards = ctx.write_guard_strings(
        exclude_partition_id=target.partition_id,
        prefix="m16:{}:guard".format(case_name),
    )

    ctx.flushmem_partitions_except(
        excluded_partition_ids={target.partition_id}
    )

    ctx.assert_values_exact(guards)

    # ---------- T5: stop target shardsvr and tamper MANIFEST metadata ----------

    with ctx.heartbeat_disabled():
        ctx.kill_shardsvr(target.shard_port)
        ctx.assert_pinned(target)

        sst_path = ctx.pick_largest_live_sst(
            target=target,
            preferred_files=new_ssts,
        )

        cmd = _manifest_metadata_tamper_command(
            case_name=case_name,
            target=target,
            manifest_path=manifest_path,
            sst_path=sst_path,
        )

        print("run MANIFEST metadata tamper command: {}".format(cmd))
        try:
            subprocess.check_call(cmd)
        finally:
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
    ctx.assert_values_missing_or_exact(target_expected)

    ctx.write_one_and_assert(
        tag=tag,
        key_prefix="m16:{}:after-repair".format(case_name),
        value="after-repair-value",
    )

    ctx.assert_all_partitions_read_write(
        prefix="test_m16_manifest_sst_metadata_tampered:{}:after-repair".format(
            case_name,
        ),
    )

    ctx.assert_all_partitions_opened()


@pytest.mark.parametrize("case_name", _METADATA_TAMPER_CASES)
def test_m16_manifest_sst_metadata_tampered_and_repair(case_name):
    """
    M16: tamper MANIFEST SST metadata with an external lower-level editor.
    """
    _run_manifest_sst_metadata_tamper_case(case_name)


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
