# test_m8_current_points_to_wrong_file_type.py

import os

import pytest

from current_fault_case_lib import (
    create_empty_plain_file,
    pick_partition_file,
    run_current_fault_case,
)


def test_m8_current_points_to_wrong_file_type_and_repair():
    """
    M8: CURRENT points to an existing file with the wrong type.

    Covers OPTIONS-xxxxxx, 000xxx.log, and an ordinary empty file.
    """

    def inject_options_fault(ctx, target, current_manifest):
        file_name = os.path.basename(pick_partition_file(ctx, target, "OPTIONS-*"))
        ctx.write_current_file_content(target, "{}\n".format(file_name))
        return "CURRENT -> {}".format(file_name)

    def inject_wal_fault(ctx, target, current_manifest):
        file_name = os.path.basename(pick_partition_file(ctx, target, "*.log"))
        ctx.write_current_file_content(target, "{}\n".format(file_name))
        return "CURRENT -> {}".format(file_name)

    def inject_empty_plain_file_fault(ctx, target, current_manifest):
        path = create_empty_plain_file(
            ctx,
            target,
            "DBREPAIR_AT_EMPTY_CURRENT_TARGET",
        )
        file_name = os.path.basename(path)
        ctx.write_current_file_content(target, "{}\n".format(file_name))
        return "CURRENT -> {}".format(file_name)

    cases = [
        ("test_m8_current_points_to_options_file", inject_options_fault),
        ("test_m8_current_points_to_wal_file", inject_wal_fault),
        (
            "test_m8_current_points_to_empty_plain_file",
            inject_empty_plain_file_fault,
        ),
    ]

    for case_name, inject_fault in cases:
        run_current_fault_case(case_name, inject_fault)


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
