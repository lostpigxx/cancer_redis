# test_m13_manifest_file_empty.py

import os

import pytest

from manifest_fault_common import run_manifest_fault_case


def _truncate_manifest_to_zero(path):
    old_size = os.path.getsize(path)

    assert old_size > 0, (
        "MANIFEST file is already empty before fault injection: {}".format(path)
    )

    with open(path, "r+b") as f:
        f.truncate(0)
        f.flush()
        os.fsync(f.fileno())

    return old_size


def test_m13_manifest_file_empty_and_repair():
    """
    M13: truncate the current MANIFEST file to 0 bytes.
    """

    def inject_fault(ctx, target, current_manifest, manifest_path):
        old_size = _truncate_manifest_to_zero(manifest_path)
        return "truncated MANIFEST {} to 0 bytes: path={}, old_size={}".format(
            current_manifest,
            manifest_path,
            old_size,
        )

    run_manifest_fault_case(
        "test_m13_manifest_file_empty",
        inject_fault,
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
