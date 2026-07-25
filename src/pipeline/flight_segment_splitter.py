import mmap
import struct
from typing import Dict, List, Tuple, Any

SYNC_MARKER = b"\xa3\x95"


class FlightSegmentSplitter:
    """
    Utility class responsible for scanning valid synchronization markers
    and splitting a binary flight log into balanced byte ranges for parallel execution.
    """

    @staticmethod
    def find_valid_sync_positions(mapped_log: mmap.mmap, format_definitions: Dict[int, Dict[str, Any]]) -> List[int]:
        """Scan the mapped log and return byte offsets of valid sync markers with known message types."""
        file_size = mapped_log.size()
        position = 0
        valid_positions: List[int] = []

        while True:
            position = mapped_log.find(SYNC_MARKER, position)
            if position == -1 or position + 3 >= file_size:
                break

            message_id = mapped_log[position + 2]
            format_definition = format_definitions.get(message_id)

            if format_definition:
                message_length = format_definition["message_length"]
                if position + message_length <= file_size:
                    valid_positions.append(position)

                    # Optimization: If a valid message is found, skip its entire length
                    # to avoid redundant scanning and improve performance.
                    position += message_length
                    continue

            # Fallback: If the ID is unknown or corrupted, advance by only 1 byte
            # to ensure we do not skip over the next valid synchronization marker.
            position += 1

        return valid_positions

    @staticmethod
    def split_ranges(positions: List[int], num_parts: int, file_size: int) -> List[Tuple[int, int]]:
        """Split the file into balanced non-overlapping byte ranges based on valid sync locations."""
        if not positions:
            return [(0, file_size)]

        num_parts = max(1, min(num_parts, len(positions)))
        messages_per_part = len(positions) // num_parts
        remainder = len(positions) % num_parts

        byte_ranges: List[Tuple[int, int]] = []
        index = 0

        for i in range(num_parts):
            take_count = messages_per_part + (1 if i < remainder else 0)
            start_offset = positions[index]

            next_index = index + take_count
            end_offset = file_size if next_index >= len(positions) else positions[next_index]

            byte_ranges.append((start_offset, end_offset))
            index = next_index

        return byte_ranges

    @staticmethod
    def build_structs_for_local_use(format_definitions: Dict[int, Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
        """Instantiate and attach compiled struct objects to format definitions."""
        for definition in format_definitions.values():
            definition["struct_obj"] = struct.Struct(definition["struct_fmt"])
        return format_definitions