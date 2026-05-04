# test_m12_manifest_file_missing.py

import pytest

from manifest_fault_common import run_manifest_fault_case


def test_m12_manifest_file_missing_and_repair():
    """
    M12: delete the current MANIFEST file.
    """

    def inject_fault(ctx, target, current_manifest, manifest_path):
        ctx.delete_current_manifest_file(target)
        return "deleted current MANIFEST {} at {}".format(
            current_manifest,
            manifest_path,
        )

    run_manifest_fault_case(
        "test_m12_manifest_file_missing",
        inject_fault,
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
