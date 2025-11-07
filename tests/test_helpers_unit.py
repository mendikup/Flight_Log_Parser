# tests/test_helpers_unit.py
import mmap
import os
import struct
import tempfile
from utils import find_valid_sync_positions, split_ranges
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../parser")))

SYNC = b"\xA3\x95"

def _write_blob(blob: bytes) -> str:
    fd, path = tempfile.mkstemp(suffix=".bin")
    with os.fdopen(fd, "wb") as f:
        f.write(blob)
    return path

def test_find_valid_sync_positions_and_split_ranges(tmp_path):
    msg = SYNC + bytes([200]) + b"\x00" * 8
    blob = msg * 10
    path = _write_blob(blob)
    try:
        with open(path, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        fmt_defs = {200: {"message_length": 11}}
        syncs = find_valid_sync_positions(mm, fmt_defs)
        assert len(syncs) == 10
        rs = split_ranges(syncs, 3, mm.size())
        assert len(rs) == 3
        assert rs[0][0] == syncs[0]
        assert rs[1][0] == syncs[4]
        assert rs[-1][1] == mm.size()
        mm.close()
    finally:
        try: os.remove(path)
        except FileNotFoundError: pass

def test_split_ranges_edge_cases():
    ranges = split_ranges([], num_parts=4, file_size=1234)
    assert ranges == [(0, 1234)]
    ranges = split_ranges([0,100], num_parts=8, file_size=1000)
    assert len(ranges) == 2
