import re
import struct
import mmap
import time
from typing import Dict, List, Optional, Tuple, Union, Generator

from src.utils.config_loader import config
from src.utils.log_config import logger



SYNC_MARKER: bytes = b"\xa3\x95"
FMT_TYPE_ID: int = 0x80
FMT_MESSAGE_LENGTH: int = 89


class BinLogParser:
    def __init__(
        self,
        mapped_flight_log: mmap.mmap,
        format_definitions: Optional[Dict[int, Dict]] = None,
        round_floats: bool = False,
        collect_warnings: bool = False
    ) -> None:

        self.mapped_flight_log = mapped_flight_log
        self.fmt_definitions: Dict[int, Dict] = format_definitions or {}
        self.round_floats = round_floats
        self.collect_warnings = collect_warnings
        self.warnings: list[str] = [] if collect_warnings else None
        self._fields_to_round: set[str] = set(config.parser.round_fields)
        self._ardu_to_struct: Dict[str, str] = dict(config.parser.ardu_to_struct)
        self._scale_factors: Dict[str, float] = dict(config.parser.scale_factors)


    def preload_fmt_messages(self) -> int:
        """
        Scan the log for FMT messages and populate fmt_definitions.
        Then validate all struct formats (without building struct objects).
        """
        file_size: int = self.mapped_flight_log.size()
        logger.debug(f"Scanning FMT messages in file of {file_size:,} bytes...")

        fmt_count: int = 0
        for fmt_offset in self._find_fmt_offsets():
            if self._parse_fmt_message(fmt_offset):  # check if FMT parsed successfully
                fmt_count += 1

        self._validate_fmt_definitions()  # only validation here, no struct creation
        logger.debug(f"Total FMT definitions found: {fmt_count}")
        return fmt_count

    def _find_fmt_offsets(self) -> Generator[int, None, None]:
        """Yield all byte offsets where FMT messages appear."""
        position: int = 0
        file_size: int = self.mapped_flight_log.size()

        while position < file_size:
            next_fmt_offset: int = self.mapped_flight_log.find(b"\xa3\x95\x80", position)
            if next_fmt_offset == -1:
                break  # stop if no more FMT headers found
            yield next_fmt_offset
            position = next_fmt_offset + FMT_MESSAGE_LENGTH  # skip to next possible FMT

    # ---------------------------------------------------------------------
    # Validation function
    # ---------------------------------------------------------------------
    def _validate_fmt_definitions(self) -> None:
        """
        Validate all FMT definitions.
        Checks:
        - struct format can be compiled by Python.
        - struct size matches expected message length.
        """
        for msg_id, fmt_definition in self.fmt_definitions.items():
            try:
                struct_size = struct.calcsize(fmt_definition["struct_fmt"])  # try compiling format
                expected = fmt_definition["message_length"] - 3  # payload length (minus sync + id)

                # Check size consistency between defined and calculated
                if struct_size != fmt_definition["struct_size"]:
                    logger.warning(
                        "Struct size mismatch for %s (ID %s): expected %s bytes, got %s",
                        fmt_definition["name"], msg_id, fmt_definition["struct_size"], struct_size
                    )

                # Check struct does not exceed total message length
                if struct_size > expected:
                    logger.warning(
                        "Struct exceeds message length for %s (ID %s)",
                        fmt_definition["name"], msg_id
                    )

            except struct.error as err:
                logger.warning(
                    "Invalid struct format for %s (ID %s): %s",
                    fmt_definition.get("name", "?"), msg_id, err
                )


    def parse_messages_in_range(
        self,
        start_offset: int,
        end_offset: Optional[int] = None,
        message_filter: Optional[set[str]] = None,
    ) -> Generator[Union[Tuple, Dict], None, None]:
        """Decode all messages in the given byte range."""
        end_offset = end_offset or self.mapped_flight_log.size()
        position: int = start_offset
        unpack_cache: Dict[int, callable] = {}

        total_decoded: int = 0
        start_time: float = time.perf_counter()

        while True:
            next_message_offset: Optional[int] = self._find_next_message(
                self.mapped_flight_log, position, end_offset
            )
            if next_message_offset is None:
                break  # stop when no more valid messages found

            position = next_message_offset
            message_id: int = self.mapped_flight_log[position + 2]

            if message_id == FMT_TYPE_ID:
                position += FMT_MESSAGE_LENGTH  # skip FMT message
                continue

            fmt_definition: Optional[Dict] = self.fmt_definitions.get(message_id)
            if not fmt_definition or "struct_obj" not in fmt_definition:
                self.warnings.append(f"Unknown or uninitialized message ID at offset {position}: {message_id}")
                position += 1  # move forward if unknown message type
                continue

            if message_filter and fmt_definition["name"] not in message_filter:
                position += fmt_definition["message_length"]  # skip unwanted message type
                continue

            decoded_message = self._decode_single_message(
                fmt_definition, position, end_offset, unpack_cache
            )

            if decoded_message is not None:
                yield decoded_message
                total_decoded += 1

            position += fmt_definition["message_length"]  # advance to next message

        total_time = time.perf_counter() - start_time
        logger.debug("Decoded %s messages in %.2fs", total_decoded, total_time)

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------
    def _decode_single_message(
        self,
        fmt_definition: Dict,
        position: int,
        end_offset: int,
        unpack_cache: Dict[int, callable],
    ) -> Optional[Union[Tuple, Dict]]:
        """Decode a single message based on its FMT structure."""
        payload_start: int = position + 3  # skip sync marker + ID
        payload_end: int = payload_start + fmt_definition["struct_size"]

        if payload_end > end_offset:
            self.warnings.append(
                f"Truncated message at offset {position}: expected {fmt_definition['struct_size']} bytes"
            )
            return None  # stop if message goes beyond file end

        try:
            unpacked_values: List = self._unpack_values(fmt_definition, payload_start, unpack_cache)
            scaled_values: List = self._apply_scaling(unpacked_values, fmt_definition["ardu_format"])

            if len(scaled_values) != len(fmt_definition["field_names"]):
                self.warnings.append(
                    f"Field count mismatch for {fmt_definition['name']} at {position}: "
                    f"{len(scaled_values)} values vs {len(fmt_definition['field_names'])} fields"
                )

            return self._build_message_as_dict(fmt_definition, scaled_values)
        except struct.error as err:
            self.warnings.append(f"Unpack failed at offset {position}: {err}")
            return None  # skip malformed message

    def _find_next_message(self, mapped_log: mmap.mmap, position: int, end_offset: int) -> Optional[int]:
        """Find next sync marker within range."""
        next_sync_position: int = mapped_log.find(SYNC_MARKER, position, end_offset)
        if next_sync_position == -1 or next_sync_position + 3 >= end_offset:
            return None  # no valid sync found
        return next_sync_position

    def _unpack_values(self, fmt_definition: Dict, payload_start: int, unpack_cache: Dict[int, callable]) -> List:
        """Unpack binary payload using cached struct definitions."""
        message_id: int = fmt_definition["id"]
        if message_id not in unpack_cache:  # add to cache if not seen before
            unpack_cache[message_id] = fmt_definition["struct_obj"].unpack_from

        return list(unpack_cache[message_id](self.mapped_flight_log, payload_start))

    def _apply_scaling(self, values: List, ardu_format: str) -> List:
        """Apply numeric scaling (GPS and altitude values)."""
        scale_factors: Dict[str, float] = self._scale_factors
        return [
            (val * scale_factors[fmt_char] if fmt_char in scale_factors and isinstance(val, (int, float)) else val)
            for val, fmt_char in zip(values, ardu_format)
        ]

    def _build_message_as_dict(self, fmt_definition: Dict, values: List) -> Dict:
        message: Dict[str, Union[str, float, int]] = dict(zip(fmt_definition["field_names"], values))
        message["message_type"] = fmt_definition["name"]

        # decode byte fields to strings
        for field_name, value in list(message.items()):
            if isinstance(value, (bytes, bytearray)):
                try:
                    message[field_name] = value.decode("ascii", "ignore").strip("\x00")
                except Exception:
                    pass  # ignore decoding errors

        # optionally round selected float fields
        if self.round_floats:
            for field_name in self._fields_to_round:
                if field_name in message and isinstance(message[field_name], float):
                    message[field_name] = round(message[field_name], 3)

        return message

    # ---------------------------------------------------------------------
    # FMT definition handling
    # ---------------------------------------------------------------------
    def _parse_fmt_message(self, offset: int) -> bool:
        """Parse an FMT message and store its structure."""
        try:
            mapped_log = self.mapped_flight_log
            msg_type_id: int = mapped_log[offset + 3]
            msg_name: str = mapped_log[offset + 5 : offset + 9].decode("ascii", "ignore").strip("\x00")

            if not re.match(r"^[A-Za-z0-9]+$", msg_name):
                return False  # skip invalid names (non-ASCII or empty)

            ardu_format: str = mapped_log[offset + 9 : offset + 25].decode("ascii", "ignore").strip("\x00")
            field_names: List[str] = self._extract_field_names(mapped_log[offset + 25 : offset + 89])
            struct_format: str = self._convert_to_struct_format(ardu_format)

            self.fmt_definitions[msg_type_id] = {
                "id": msg_type_id,
                "name": msg_name,
                "ardu_format": ardu_format,
                "field_names": field_names,
                "struct_fmt": struct_format,
                "struct_size": struct.calcsize(struct_format),
                "message_length": mapped_log[offset + 4],
            }

            logger.debug("FMT %-3d %-8s Fields=%s", msg_type_id, msg_name, len(field_names))
            return True

        except Exception as err:
            warning = f"Bad FMT at offset {offset}: {err}"
            self.warnings.append(warning)
            logger.warning(warning)
            return False

    def _extract_field_names(self, raw_bytes: bytes) -> List[str]:
        """Extract and clean field names from raw FMT data."""
        decoded_text: str = raw_bytes.decode("ascii", "ignore")
        cleaned_text: str = re.split(r"\x00{2,}", decoded_text)[0].strip("\x00").replace(" ", "")
        return [field_name for field_name in cleaned_text.split(",") if field_name]

    def _convert_to_struct_format(self, ardu_format: str) -> str:
        """Convert ArduPilot format string to Python struct format."""
        return "<" + "".join(self._ardu_to_struct.get(fmt_char, "") for fmt_char in ardu_format)

    # for testing purposes
    def build_structs_for_local_use(self) -> None:
        """Build struct objects locally (used when running src standalone)."""
        for fmt_definition in self.fmt_definitions.values():
            fmt_definition["struct_obj"] = struct.Struct(fmt_definition["struct_fmt"])

