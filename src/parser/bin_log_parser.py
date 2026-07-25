import re
import struct
import mmap
import time
from typing import Dict, List, Optional, Generator, Any, Set

from src.config.config_loader import config
from src.config.log_config import logger

SYNC_MARKER: bytes = b"\xa3\x95"
FMT_TYPE_ID: int = 0x80
FMT_MESSAGE_LENGTH: int = 89


class BinLogParser:
    """
    High-performance binary log parser for ArduPilot .BIN files.
    Manages format definitions, structural compilation, and memory-mapped decoding.
    """

    def __init__(
            self,
            mapped_flight_log: mmap.mmap,
            format_definitions: Optional[Dict[int, Dict[str, Any]]] = None,
            round_floats: bool = False,
            collect_warnings: bool = False,
    ) -> None:
        self.mapped_flight_log = mapped_flight_log
        self.fmt_definitions: Dict[int, Dict[str, Any]] = format_definitions or {}
        self.round_floats = round_floats
        self.collect_warnings = collect_warnings
        self.warnings: List[str] = [] if collect_warnings else []

        self._fields_to_round: Set[str] = set(config.parser.round_fields)
        self._ardu_to_struct: Dict[str, str] = dict(config.parser.ardu_to_struct)
        self._scale_factors: Dict[str, float] = dict(config.parser.scale_factors)

        self._ensure_structs_compiled()

    def _ensure_structs_compiled(self) -> None:
        """
        Ensures all format definitions possess a compiled struct object.
        Crucial for rebuilding objects after multiprocessing unpickling.
        """
        for definition in self.fmt_definitions.values():
            if "struct_fmt" in definition and "struct_obj" not in definition:
                definition["struct_obj"] = struct.Struct(definition["struct_fmt"])

    # ============================================================
    # FMT Loading and Validation (Top-Down Order)
    # ============================================================

    def preload_fmt_messages(self) -> int:
        """Scan the entire log file, parse all FMT definitions, and validate them."""
        file_size: int = self.mapped_flight_log.size()
        logger.debug("Scanning FMT definitions in log file of %s bytes...", f"{file_size:,}")

        fmt_count: int = 0
        for fmt_offset in self._find_fmt_offsets():
            if self._parse_fmt_message(fmt_offset):
                fmt_count += 1

        self._validate_fmt_definitions()
        logger.info("Total FMT definitions successfully loaded: %d", fmt_count)
        return fmt_count

    def _find_fmt_offsets(self) -> Generator[int, None, None]:
        """Yield precise byte offsets where FMT definition messages appear."""
        position: int = 0
        file_size: int = self.mapped_flight_log.size()

        while position < file_size:
            next_fmt_offset: int = self.mapped_flight_log.find(b"\xa3\x95\x80", position)
            if next_fmt_offset == -1:
                break
            yield next_fmt_offset
            position = next_fmt_offset + FMT_MESSAGE_LENGTH

    def _parse_fmt_message(self, offset: int) -> bool:
        """Parse an individual FMT block and register its schema into the dictionary."""
        try:
            mapped_log = self.mapped_flight_log
            message_type_id = mapped_log[offset + 3]
            message_name = mapped_log[offset + 5: offset + 9].decode("ascii", "ignore").strip("\x00")

            if not re.match(r"^[A-Za-z0-9]+$", message_name):
                return False

            ardu_format = mapped_log[offset + 9: offset + 25].decode("ascii", "ignore").strip("\x00")
            raw_field_bytes = mapped_log[offset + 25: offset + 89]

            field_names = self._extract_field_names(raw_field_bytes)
            struct_format = self._convert_to_struct_format(ardu_format)

            self.fmt_definitions[message_type_id] = {
                "id": message_type_id,
                "name": message_name,
                "ardu_format": ardu_format,
                "field_names": field_names,
                "struct_fmt": struct_format,
                "struct_size": struct.calcsize(struct_format),
                "message_length": mapped_log[offset + 4],
                "struct_obj": struct.Struct(struct_format),
            }

            return True

        except Exception as err:
            if self.collect_warnings:
                self.warnings.append(f"Failed to parse FMT at offset {offset}: {err}")
            return False

    @staticmethod
    def _extract_field_names(raw_bytes: bytes) -> List[str]:
        """Sanitize and split comma-separated field names from raw FMT bytes."""
        decoded_text = raw_bytes.decode("ascii", "ignore")
        cleaned_text = re.split(r"\x00{2,}", decoded_text)[0].strip("\x00").replace(" ", "")
        return [name for name in cleaned_text.split(",") if name]

    def _convert_to_struct_format(self, ardu_format: str) -> str:
        """Convert ArduPilot format string into a standard Python struct format."""
        struct_chars = [self._ardu_to_struct.get(char, "") for char in ardu_format]
        return "<" + "".join(struct_chars)

    def _validate_fmt_definitions(self) -> None:
        """Verify structural consistency bounds for all loaded formats."""
        for message_id, definition in self.fmt_definitions.items():
            expected_size = definition["message_length"] - 3
            if definition["struct_size"] > expected_size:
                logger.warning("Struct payload exceeds message length bounds for %s", definition["name"])

    # ============================================================
    # Message Decoding (Top-Down Order)
    # ============================================================

    def parse_messages_in_range(
            self,
            start_offset: int,
            end_offset: Optional[int] = None,
            message_filter: Optional[Set[str]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Decode flight messages sequentially within a specific byte range.
        Uses mathematical jumps to bypass irrelevant messages rapidly.
        """
        end_offset = end_offset or self.mapped_flight_log.size()
        current_position: int = start_offset
        unpack_cache: Dict[int, Any] = {}

        while True:
            next_sync_position: Optional[int] = self._find_next_sync_marker(current_position, end_offset)
            if next_sync_position is None:
                break

            current_position = next_sync_position
            message_id: int = self.mapped_flight_log[current_position + 2]

            if message_id == FMT_TYPE_ID:
                current_position += FMT_MESSAGE_LENGTH
                continue

            format_definition: Optional[Dict[str, Any]] = self.fmt_definitions.get(message_id)
            if not format_definition or "struct_obj" not in format_definition:
                current_position += 1
                continue

            if message_filter and format_definition["name"] not in message_filter:
                current_position += format_definition["message_length"]
                continue

            decoded_message = self._decode_single_message(format_definition, current_position, end_offset, unpack_cache)
            if decoded_message is not None:
                yield decoded_message

            current_position += format_definition["message_length"]

    def _find_next_sync_marker(self, position: int, end_offset: int) -> Optional[int]:
        """Locate the exact index of the next sync marker."""
        next_sync_position = self.mapped_flight_log.find(SYNC_MARKER, position, end_offset)
        if next_sync_position == -1 or next_sync_position + 3 >= end_offset:
            return None
        return next_sync_position

    def _decode_single_message(
            self,
            format_definition: Dict[str, Any],
            position: int,
            end_offset: int,
            unpack_cache: Dict[int, Any],
    ) -> Optional[Dict[str, Any]]:
        """Extract bytes, unpack them, and build the final readable dictionary."""
        payload_start = position + 3
        payload_end = payload_start + format_definition["struct_size"]

        if payload_end > end_offset:
            return None

        try:
            unpacked_values = self._unpack_payload_values(format_definition, payload_start, unpack_cache)
            return self._build_message_dictionary(format_definition, unpacked_values)
        except struct.error:
            return None

    def _unpack_payload_values(
            self,
            format_definition: Dict[str, Any],
            payload_start: int,
            unpack_cache: Dict[int, Any],
    ) -> List[Any]:
        """Unpack raw memory bytes into tuple values using a cached struct object."""
        message_id = format_definition["id"]
        if message_id not in unpack_cache:
            unpack_cache[message_id] = format_definition["struct_obj"].unpack_from
        return list(unpack_cache[message_id](self.mapped_flight_log, payload_start))

    def _build_message_dictionary(
            self,
            format_definition: Dict[str, Any],
            unpacked_values: List[Any],
    ) -> Optional[Dict[str, Any]]:
        """Map values to their string names, applying required rounding and scaling."""
        field_names = format_definition["field_names"]
        ardu_format = format_definition["ardu_format"]

        if len(unpacked_values) != len(field_names):
            return None

        message_record: Dict[str, Any] = {"message_type": format_definition["name"]}

        for field_name, value, format_char in zip(field_names, unpacked_values, ardu_format):
            if isinstance(value, (int, float)) and format_char in self._scale_factors:
                value *= self._scale_factors[format_char]
            elif isinstance(value, (bytes, bytearray)):
                try:
                    value = value.decode("ascii", "ignore").strip("\x00")
                except Exception:
                    pass

            if self.round_floats and field_name in self._fields_to_round and isinstance(value, float):
                value = round(value, 3)

            message_record[field_name] = value

        return message_record