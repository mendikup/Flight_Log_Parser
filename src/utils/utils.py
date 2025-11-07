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










