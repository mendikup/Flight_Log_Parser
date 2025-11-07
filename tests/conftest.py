import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../parser')))

# tests/conftest.py
import os
import mmap
import struct
import tempfile
import pytest
from typing import List, Tuple

SYNC = b"\xA3\x95"
FMT_ID = 0x80
FMT_LEN = 89  # כמו בקוד שלך
# מבנה ה-FMT לפי הקוד: [sync(2)=A3 95][msg_id(1)=80][type_id(1)][msg_len(1)][name(4)][format(16)][fields(64)]

def build_fmt_message(type_id: int, name4: bytes, ardu_fmt: str, field_names_csv: str, msg_len_total: int) -> bytes:
    assert len(name4) == 4, "name4 must be exactly 4 bytes"
    # כריות ל-16 תווים של format
    fmt_bytes = ardu_fmt.encode("ascii") + b"\x00" * (16 - len(ardu_fmt))
    fields_bytes = field_names_csv.encode("ascii")
    if len(fields_bytes) > 64:
        raise ValueError("fields string too long for 64 bytes")
    fields_bytes = fields_bytes + b"\x00" * (64 - len(fields_bytes))
    return (
        SYNC
        + bytes([FMT_ID])
        + bytes([type_id])
        + bytes([msg_len_total])
        + name4
        + fmt_bytes
        + fields_bytes
    )

def build_data_message(type_id: int, payload: bytes) -> bytes:
    return SYNC + bytes([type_id]) + payload

def make_synthetic_bin(messages: List[Tuple[int, dict]]) -> bytes:
    """
    messages: רשימה של (type_id, dict_values). נבנה FMT עבור type_id=200 בשם 'TST\0' עם פורמט IffZ
    שדות: TimeUS(uint32), Val1(float), Val2(float), Note(64s)
    """
    # מיפוי פורמט ArduPilot -> struct (כמו בקוד שלך)
    ardu_fmt = "IffZ"  # Z=64s
    struct_fmt = "<Iff64s"
    struct_size = struct.calcsize(struct_fmt)
    msg_len_total = struct_size + 3  # לפי הקוד: message_length כולל 3 בייטים של sync+id
    name4 = b"TST\x00"
    fields = "TimeUS,Val1,Val2,Note"

    fmt_msg = build_fmt_message(
        type_id=200, name4=name4, ardu_fmt=ardu_fmt,
        field_names_csv=fields, msg_len_total=msg_len_total
    )

    data_msgs = []
    for _, d in messages:
        note_raw = d.get("Note", "").encode("ascii")[:64]
        note_raw = note_raw + b"\x00" * (64 - len(note_raw))
        payload = struct.pack(struct_fmt, d["TimeUS"], d["Val1"], d["Val2"], note_raw)
        data_msgs.append(build_data_message(200, payload))

    return fmt_msg + b"".join(data_msgs)

@pytest.fixture
def tmp_synthetic_file():
    msgs = [
        (200, {"TimeUS": 1000, "Val1": 1.234567, "Val2": -2.7182818, "Note": "hello"}),
        (200, {"TimeUS": 1010, "Val1": 3.141592, "Val2": 0.0001234, "Note": "world"}),
        (200, {"TimeUS": 1020, "Val1": 10.0, "Val2": 20.5, "Note": ""}),
    ]
    blob = make_synthetic_bin(msgs)
    fd, path = tempfile.mkstemp(suffix=".bin")
    with os.fdopen(fd, "wb") as f:
        f.write(blob)
    try:
        yield path
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

@pytest.fixture
def open_mmap(tmp_synthetic_file):
    with open(tmp_synthetic_file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    try:
        yield mm
    finally:
        mm.close()
