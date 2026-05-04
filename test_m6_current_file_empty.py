# test_m6_current_file_empty.py

import pytest

from current_fault_case_lib import run_current_fault_case


def test_m6_current_file_empty_and_repair():
    """
    M6: CURRENT file exists but is empty.
    """

    def inject_fault(ctx, target, current_manifest):
        ctx.write_current_file_content(target, "")
        return "CURRENT content is empty"

    run_current_fault_case("test_m6_current_file_empty", inject_fault)


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
