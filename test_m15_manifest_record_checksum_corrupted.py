# test_m15_manifest_record_checksum_corrupted.py

import os
import struct

import pytest

from manifest_fault_common import run_manifest_fault_case


_LOG_BLOCK_SIZE = 32768
_LOG_HEADER_SIZE = 7
_VALID_RECORD_TYPES = {1, 2, 3, 4, 5, 6, 7, 8}


def _find_manifest_record_header_offset(path):
    with open(path, "rb") as f:
        data = f.read()

    assert len(data) >= _LOG_HEADER_SIZE, (
        "MANIFEST file too small to contain a log record header: "
        "path={}, size={}".format(path, len(data))
    )

    for block_start in range(0, len(data), _LOG_BLOCK_SIZE):
        block_end = min(block_start + _LOG_BLOCK_SIZE, len(data))
        pos = block_start

        while pos + _LOG_HEADER_SIZE <= block_end:
            header = data[pos:pos + _LOG_HEADER_SIZE]

            if header == b"\x00" * _LOG_HEADER_SIZE:
                pos += _LOG_HEADER_SIZE
                continue

            length = struct.unpack_from("<H", header, 4)[0]
            record_type = header[6]
            record_end = pos + _LOG_HEADER_SIZE + length

            if (
                record_type in _VALID_RECORD_TYPES
                and length > 0
                and record_end <= block_end
            ):
                return pos, length, record_type

            pos += 1

    raise AssertionError(
        "failed to locate a valid MANIFEST record header: path={}".format(
            path,
        )
    )


def _corrupt_manifest_record_checksum(path):
    offset, length, record_type = _find_manifest_record_header_offset(path)

    with open(path, "r+b") as f:
        f.seek(offset)
        checksum = f.read(4)

        assert len(checksum) == 4, (
            "failed to read MANIFEST record checksum: path={}, offset={}".format(
                path,
                offset,
            )
        )

        bad = bytes([checksum[0] ^ 0xff]) + checksum[1:]
        f.seek(offset)
        f.write(bad)
        f.flush()
        os.fsync(f.fileno())

    return offset, length, record_type


def test_m15_manifest_record_checksum_corrupted_and_repair():
    """
    M15: corrupt only the checksum field in one MANIFEST physical record header.
    """

    def inject_fault(ctx, target, current_manifest, manifest_path):
        offset, length, record_type = _corrupt_manifest_record_checksum(
            manifest_path
        )
        return (
            "corrupted MANIFEST {} record checksum: path={}, "
            "header_offset={}, length={}, record_type={}"
        ).format(
            current_manifest,
            manifest_path,
            offset,
            length,
            record_type,
        )

    run_manifest_fault_case(
        "test_m15_manifest_record_checksum_corrupted",
        inject_fault,
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main(["-s", __file__]))
