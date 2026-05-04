# test_m11_current_truncated_manifest_name.py

import pytest

from current_fault_case_lib import run_current_fault_case


def test_m11_current_truncated_manifest_name_and_repair():
    """
    M11: CURRENT is truncated to a partial MANIFEST file name.

    Covers MANIFEST- and MANIFEST-000.
    """

    def inject_manifest_prefix(ctx, target, current_manifest):
        content = "MANIFEST-\n"
        ctx.write_current_file_content(target, content)
        return "CURRENT content={!r}".format(content)

    def inject_manifest_number(ctx, target, current_manifest):
        content = "MANIFEST-000\n"
        ctx.write_current_file_content(target, content)
        return "CURRENT content={!r}".format(content)

    cases = [
        ("test_m11_current_truncated_manifest_prefix", inject_manifest_prefix),
        ("test_m11_current_truncated_manifest_number", inject_manifest_number),
    ]

    for case_name, inject_fault in cases:
        run_current_fault_case(case_name, inject_fault)


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
