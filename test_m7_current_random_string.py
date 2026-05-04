# test_m7_current_random_string.py

import pytest

from current_fault_case_lib import run_current_fault_case


def test_m7_current_random_string_and_repair():
    """
    M7: CURRENT content is a random string, not MANIFEST-xxxxxx.
    """

    def inject_fault(ctx, target, current_manifest):
        content = "DBREPAIR_AT_RANDOM_CURRENT_CONTENT\n"
        ctx.write_current_file_content(target, content)
        return "CURRENT content={!r}".format(content)

    run_current_fault_case("test_m7_current_random_string", inject_fault)


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
