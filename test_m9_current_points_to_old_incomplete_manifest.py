# test_m9_current_points_to_old_incomplete_manifest.py

import os

import pytest

from current_fault_case_lib import (
    create_old_incomplete_manifest,
    run_current_fault_case,
)


def test_m9_current_points_to_old_incomplete_manifest_and_repair():
    """
    M9: CURRENT points to an old existing MANIFEST with incomplete content.
    """

    def inject_fault(ctx, target, current_manifest):
        path = create_old_incomplete_manifest(ctx, target, current_manifest)
        file_name = os.path.basename(path)
        ctx.write_current_file_content(target, "{}\n".format(file_name))
        return "CURRENT -> old incomplete {}".format(file_name)

    run_current_fault_case(
        "test_m9_current_points_to_old_incomplete_manifest",
        inject_fault,
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
