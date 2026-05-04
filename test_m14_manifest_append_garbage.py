# test_m14_manifest_append_garbage.py

import os

import pytest

from manifest_fault_common import run_manifest_fault_case


_LOG_BLOCK_SIZE = 32768
_LOG_HEADER_SIZE = 7


def _append_manifest_garbage(path):
    old_size = os.path.getsize(path)
    block_remainder = old_size % _LOG_BLOCK_SIZE
    trailer_padding = b""

    if block_remainder > _LOG_BLOCK_SIZE - _LOG_HEADER_SIZE:
        trailer_padding = b"\x00" * (_LOG_BLOCK_SIZE - block_remainder)

    # checksum=0, length=1, type=FULL, payload='X' is a complete but invalid
    # RocksDB log record. Appending it makes the tail corruption visible even
    # when the original MANIFEST ends near a block boundary.
    bad_record = b"\x00\x00\x00\x00\x01\x00\x01X"
    garbage = trailer_padding + bad_record + (
        b"DBREPAIR_AT_MANIFEST_GARBAGE_RECORD!"
        b"\x7f\x80\x81\xfe\xff"
        b"bad-manifest-tail"
    ) * 4

    with open(path, "ab") as f:
        f.write(garbage)
        f.flush()
        os.fsync(f.fileno())

    return old_size, len(garbage)


def test_m14_manifest_append_garbage_and_repair():
    """
    M14: append garbage bytes to the current MANIFEST file.
    """

    def inject_fault(ctx, target, current_manifest, manifest_path):
        old_size, appended = _append_manifest_garbage(manifest_path)
        return (
            "appended garbage to MANIFEST {}: path={}, "
            "old_size={}, appended={}"
        ).format(
            current_manifest,
            manifest_path,
            old_size,
            appended,
        )

    run_manifest_fault_case(
        "test_m14_manifest_append_garbage",
        inject_fault,
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
