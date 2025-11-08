import mmap
import os
import tempfile
from src.utils.utils import find_valid_sync_positions, split_ranges
from src.utils.log_config import setup_test_logger

logger = setup_test_logger()

SYNC_MARKER = b"\xA3\x95"


def _write_binary_blob(binary_data: bytes) -> str:
    """Write bytes to a temporary .bin file and return its path."""
    file_descriptor, temp_path = tempfile.mkstemp(suffix=".bin")
    with os.fdopen(file_descriptor, "wb") as file_handle:
        file_handle.write(binary_data)
    return temp_path


def test_find_valid_sync_positions_and_split_ranges():
    """Verify sync detection and range splitting behave correctly."""
    message = SYNC_MARKER + bytes([200]) + b"\x00" * 8
    binary_blob = message * 10
    temp_path = _write_binary_blob(binary_blob)

    try:
        with open(temp_path, "rb") as file_handle:
            mapped_log_file = mmap.mmap(file_handle.fileno(), 0, access=mmap.ACCESS_READ)

        fmt_definitions = {200: {"message_length": 11}}
        sync_positions = find_valid_sync_positions(mapped_log_file, fmt_definitions)
        byte_ranges = split_ranges(sync_positions, 3, mapped_log_file.size())

        logger.info(f"Found {len(sync_positions)} syncs, {len(byte_ranges)} split ranges.")

        assert len(sync_positions) == 10
        assert len(byte_ranges) == 3
        assert byte_ranges[0][0] == sync_positions[0]
        assert byte_ranges[-1][1] == mapped_log_file.size()

        mapped_log_file.close()
    finally:
        os.remove(temp_path)


def test_split_ranges_edge_cases():
    """Test edge cases for split_ranges (empty or minimal input)."""
    byte_ranges = split_ranges([], num_parts=4, file_size=1234)
    assert byte_ranges == [(0, 1234)]

    byte_ranges = split_ranges([0, 100], num_parts=8, file_size=1000)
    assert len(byte_ranges) == 2
    logger.info("split_ranges edge cases passed successfully.")
