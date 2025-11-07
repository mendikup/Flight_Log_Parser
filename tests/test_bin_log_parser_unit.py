import sys, os
from src.utils.config_loader import config
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../parser")))



# tests/test_bin_log_parser_unit.py
import math
from bin_log_parser import BinLogParser, FMT_MESSAGE_LENGTH

def test_preload_fmt_and_validation(open_mmap):
    parser = BinLogParser(open_mmap, round_floats=False)
    count = parser.preload_fmt_messages()
    parser.build_structs_for_local_use()
    assert count >= 1
    assert 200 in parser.fmt_definitions
    fmt = parser.fmt_definitions[200]
    assert fmt["name"] == "TST"
    assert fmt["message_length"] == fmt["struct_size"] + 3
    assert fmt["struct_fmt"].startswith("<")

def test_decode_dicts_basic(open_mmap):
    parser = BinLogParser(open_mmap, round_floats=False)
    parser.preload_fmt_messages()
    parser.build_structs_for_local_use()
    msgs = list(parser.parse_messages_in_range(0))
    assert len(msgs) == 3
    m0 = msgs[0]
    assert m0["message_type"] == "TST"
    assert m0["TimeUS"] == 1000
    assert isinstance(m0["Val1"], float) and isinstance(m0["Val2"], float)
    assert m0["Note"] == "hello"


def test_rounding_selected_fields(open_mmap):
    parser = BinLogParser(open_mmap, round_floats=False)
    parser.preload_fmt_messages()
    parser.build_structs_for_local_use()
    msgs = list(parser.parse_messages_in_range(0))
    raw_val = msgs[0]["Val1"]
    assert abs(raw_val - 1.234567) < 1e-6

def test_message_filter(open_mmap):
    parser = BinLogParser(open_mmap, round_floats=False)
    parser.preload_fmt_messages()
    parser.build_structs_for_local_use()
    msgs0 = list(parser.parse_messages_in_range(0, message_filter={"GPS"}))
    assert len(msgs0) == 0
    msgs1 = list(parser.parse_messages_in_range(0, message_filter={"TST"}))
    assert len(msgs1) == 3

def test_skip_unknown_message_type(open_mmap):
    parser = BinLogParser(open_mmap, round_floats=False)
    parser.preload_fmt_messages()
    parser.build_structs_for_local_use()
    msgs = list(parser.parse_messages_in_range(1000))
    assert isinstance(msgs, list)


def test_extract_field_names_handles_nulls():
    import mmap, tempfile, os
    data = b"\x00" * 128
    fd, path = tempfile.mkstemp()
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    try:
        with open(path, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        parser = BinLogParser(mm)
        fields = parser._extract_field_names(b"TimeUS, Val1, Val2, Note\x00\x00\x00abc")
        assert fields == ["TimeUS","Val1","Val2","Note"]
        mm.close()
    finally:
        try: os.remove(path)
        except FileNotFoundError: pass

def _convert_to_struct_format(self, ardu_format: str) -> str:
    mapping = config.parser.ardu_to_struct
    return "<" + "".join(mapping.get(c, "") for c in ardu_format)

