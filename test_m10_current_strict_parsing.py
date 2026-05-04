# test_m10_current_strict_parsing.py

import pytest

from current_fault_case_lib import run_current_fault_case


def test_m10_current_strict_parsing_and_repair():
    """
    M10: CURRENT content has boundary syntax variants.

    Covers missing trailing newline, extra whitespace, and multiple lines.
    These cases verify whether RocksDB parses CURRENT strictly.
    """

    def inject_missing_newline(ctx, target, current_manifest):
        ctx.write_current_file_content(target, current_manifest)
        return "CURRENT lacks trailing newline: {}".format(current_manifest)

    def inject_extra_whitespace(ctx, target, current_manifest):
        content = "  {}  \n".format(current_manifest)
        ctx.write_current_file_content(target, content)
        return "CURRENT content={!r}".format(content)

    def inject_multiline(ctx, target, current_manifest):
        content = "{}\nDBREPAIR_AT_EXTRA_CURRENT_LINE\n".format(current_manifest)
        ctx.write_current_file_content(target, content)
        return "CURRENT content={!r}".format(content)

    cases = [
        ("test_m10_current_missing_newline", inject_missing_newline),
        ("test_m10_current_extra_whitespace", inject_extra_whitespace),
        ("test_m10_current_multiline", inject_multiline),
    ]

    for case_name, inject_fault in cases:
        run_current_fault_case(case_name, inject_fault)


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
