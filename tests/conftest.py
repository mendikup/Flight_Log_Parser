import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import mmap
import struct
import tempfile
import pytest
from typing import List, Tuple
from src.utils.log_config import setup_test_logger



logger = setup_test_logger()

SYNC_MARKER = b"\xA3\x95"
FMT_TYPE_ID = 0x80
FMT_MESSAGE_LENGTH = 89  # same as in parser


def build_fmt_message(message_type_id: int, name_bytes: bytes, ardu_format: str, field_names_csv: str, total_msg_length: int) -> bytes:
    """Construct an FMT message definition binary block."""
    assert len(name_bytes) == 4, "name_bytes must be exactly 4 bytes"
    format_bytes = ardu_format.encode("ascii") + b"\x00" * (16 - len(ardu_format))
    fields_bytes = field_names_csv.encode("ascii")
    if len(fields_bytes) > 64:
        raise ValueError("fields string too long (max 64 bytes)")
    fields_bytes += b"\x00" * (64 - len(fields_bytes))
    return SYNC_MARKER + bytes([FMT_TYPE_ID, message_type_id, total_msg_length]) + name_bytes + format_bytes + fields_bytes


def build_data_message(message_type_id: int, payload_bytes: bytes) -> bytes:
    """Wrap a raw payload in sync and message ID markers."""
    return SYNC_MARKER + bytes([message_type_id]) + payload_bytes


def make_synthetic_bin(message_list: List[Tuple[int, dict]]) -> bytes:
    """Build a small synthetic binary log for testing BinLogParser."""
    ardu_format = "IffZ"
    struct_format = "<Iff64s"
    struct_size = struct.calcsize(struct_format)
    total_message_length = struct_size + 3
    message_name = b"TST\x00"
    fields_csv = "TimeUS,Val1,Val2,Note"

    fmt_message = build_fmt_message(200, message_name, ardu_format, fields_csv, total_message_length)

    data_messages = []
    for _, values_dict in message_list:
        note_raw = values_dict.get("Note", "").encode("ascii")[:64]
        note_raw += b"\x00" * (64 - len(note_raw))
        payload_bytes = struct.pack(struct_format, values_dict["TimeUS"], values_dict["Val1"], values_dict["Val2"], note_raw)
        data_messages.append(build_data_message(200, payload_bytes))

    logger.info(f"Created synthetic BIN with {len(data_messages)} messages.")
    return fmt_message + b"".join(data_messages)


@pytest.fixture
def tmp_synthetic_file():
    """Create a temporary .bin file with sample messages."""
    synthetic_messages = [
        (200, {"TimeUS": 1000, "Val1": 1.234567, "Val2": -2.7182818, "Note": "hello"}),
        (200, {"TimeUS": 1010, "Val1": 3.141592, "Val2": 0.0001234, "Note": "world"}),
        (200, {"TimeUS": 1020, "Val1": 10.0, "Val2": 20.5, "Note": ""}),
    ]
    binary_data = make_synthetic_bin(synthetic_messages)
    fd, temp_path = tempfile.mkstemp(suffix=".bin")
    with os.fdopen(fd, "wb") as file_handle:
        file_handle.write(binary_data)

    try:
        yield temp_path
    finally:
        try:
            os.remove(temp_path)
        except FileNotFoundError:
            pass


@pytest.fixture
def open_mapped_file(tmp_synthetic_file):
    """Open the synthetic .bin file as an mmap object."""
    with open(tmp_synthetic_file, "rb") as file_handle:
        mapped_log_file = mmap.mmap(file_handle.fileno(), 0, access=mmap.ACCESS_READ)
    try:
        yield mapped_log_file
    finally:
        mapped_log_file.close()
