import math
import os
import mmap
import tempfile
from src.bussines_logic.bin_log_parser import BinLogParser, FMT_MESSAGE_LENGTH
from src.utils.config_loader import config
from src.utils.utils import (
    extract_field_names,
    convert_to_struct_format,
    build_structs_for_local_use,
)
from src.utils.log_config import setup_test_logger

logger = setup_test_logger()


def test_preload_fmt_and_validation(open_mapped_file):
    """Ensure that FMT messages are correctly loaded and validated."""
    parser = BinLogParser(open_mapped_file, round_floats=False)
    fmt_message_count = parser.preload_fmt_messages()
    build_structs_for_local_use(parser.fmt_definitions)

    logger.info(f"Preloaded {fmt_message_count} FMT definitions successfully.")

    assert fmt_message_count >= 1
    assert 200 in parser.fmt_definitions

    fmt_definition = parser.fmt_definitions[200]
    assert fmt_definition["name"] == "TST"
    assert fmt_definition["message_length"] == fmt_definition["struct_size"] + 3
    assert fmt_definition["struct_fmt"].startswith("<")


def test_decode_dicts_basic(open_mapped_file):
    """Decode messages and validate that basic fields match expected values."""
    parser = BinLogParser(open_mapped_file, round_floats=False)
    parser.preload_fmt_messages()
    build_structs_for_local_use(parser.fmt_definitions)

    decoded_messages = list(parser.parse_messages_in_range(0))
    logger.info(f"Decoded {len(decoded_messages)} messages from test file.")

    assert len(decoded_messages) == 3

    first_message = decoded_messages[0]
    assert first_message["message_type"] == "TST"
    assert first_message["TimeUS"] == 1000
    assert isinstance(first_message["Val1"], float)
    assert isinstance(first_message["Val2"], float)
    assert first_message["Note"] == "hello"


def test_rounding_selected_fields(open_mapped_file):
    """Ensure that selected fields retain precision when round_floats=False."""
    parser = BinLogParser(open_mapped_file, round_floats=False)
    parser.preload_fmt_messages()
    build_structs_for_local_use(parser.fmt_definitions)

    decoded_messages = list(parser.parse_messages_in_range(0))
    raw_value = decoded_messages[0]["Val1"]
    assert abs(raw_value - 1.234567) < 1e-6
    logger.info(f"Verified raw value precision for Val1 = {raw_value}")


def test_message_filter(open_mapped_file):
    """Verify that message_filter correctly filters message types."""
    parser = BinLogParser(open_mapped_file, round_floats=False)
    parser.preload_fmt_messages()
    build_structs_for_local_use(parser.fmt_definitions)

    gps_messages = list(parser.parse_messages_in_range(0, message_filter={"GPS"}))
    tst_messages = list(parser.parse_messages_in_range(0, message_filter={"TST"}))

    logger.info(f"Filtered GPS messages: {len(gps_messages)}, TST messages: {len(tst_messages)}")

    assert len(gps_messages) == 0
    assert len(tst_messages) == 3


def test_skip_unknown_message_type(open_mapped_file):
    """Ensure parser gracefully skips unknown message types."""
    parser = BinLogParser(open_mapped_file, round_floats=False)
    parser.preload_fmt_messages()
    build_structs_for_local_use(parser.fmt_definitions)

    decoded_messages = list(parser.parse_messages_in_range(1000))
    logger.info(f"Decoded {len(decoded_messages)} messages after skipping unknown types.")
    assert isinstance(decoded_messages, list)


def test_extract_field_names_and_struct_conversion():
    """Ensure extract_field_names and convert_to_struct_format work correctly."""
    raw_bytes = b"TimeUS, Val1, Val2, Note\x00\x00\x00abc"
    ardu_format = "IffZ"
    ardu_to_struct = config.parser.ardu_to_struct

    fields = extract_field_names(raw_bytes)
    struct_format = convert_to_struct_format(ardu_format, ardu_to_struct)

    logger.info(f"Extracted fields: {fields}")
    logger.info(f"Struct format: {struct_format}")

    assert fields == ["TimeUS", "Val1", "Val2", "Note"]
    assert struct_format.startswith("<Iff64s")
