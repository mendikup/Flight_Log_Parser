import re
import struct
import mmap
from typing import Dict, List, Tuple

SYNC_MARKER = b"\xa3\x95"


def find_valid_sync_positions(mapped_log: mmap.mmap, fmt_definitions: Dict[int, Dict]) -> List[int]:
    """Return offsets of valid sync markers where the message type is known."""
    file_size = mapped_log.size()
    position = 0
    positions = []

    while True:
        position = mapped_log.find(SYNC_MARKER, position)
        if position == -1 or position + 3 >= file_size:
            break
        msg_id = mapped_log[position + 2]
        fmt = fmt_definitions.get(msg_id)
        if fmt:
            msg_len = fmt["message_length"]
            if position + msg_len <= file_size:
                positions.append(position)
        position += 1
    return positions


def split_ranges(positions: List[int], num_parts: int, file_size: int) -> List[Tuple[int, int]]:
    """Split the file into balanced non-overlapping ranges based on valid syncs."""
    if not positions:
        return [(0, file_size)]

    num_parts = max(1, min(num_parts, len(positions)))
    per_part = len(positions) // num_parts
    remainder = len(positions) % num_parts

    ranges = []
    index = 0
    for i in range(num_parts):
        take = per_part + (1 if i < remainder else 0)
        start = positions[index]
        index2 = index + take
        end = file_size if index2 >= len(positions) else positions[index2]
        ranges.append((start, end))
        index = index2

    return ranges


# ============================================================
# 🧰 Parser helper utilities (moved from BinLogParser)
# ============================================================

def extract_field_names(raw_bytes: bytes) -> List[str]:
    """Extract and clean field names from raw FMT data."""
    decoded_text = raw_bytes.decode("ascii", "ignore")
    cleaned_text = re.split(r"\x00{2,}", decoded_text)[0].strip("\x00").replace(" ", "")
    return [field_name for field_name in cleaned_text.split(",") if field_name]


def convert_to_struct_format(ardu_format: str, ardu_to_struct: Dict[str, str]) -> str:
    """Convert ArduPilot format string to Python struct format."""
    return "<" + "".join(ardu_to_struct.get(fmt_char, "") for fmt_char in ardu_format)


def build_structs_for_local_use(fmt_definitions: Dict[int, Dict]) -> Dict[int, Dict]:
    """Return a new fmt_definitions dict with struct objects built."""
    for fmt_definition in fmt_definitions.values():
        fmt_definition["struct_obj"] = struct.Struct(fmt_definition["struct_fmt"])
    return fmt_definitions
