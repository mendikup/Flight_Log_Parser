import os
import mmap
import struct
import time
import tempfile
import pickle
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Any, Optional, Set

from src.bussines_logic.bin_log_parser import BinLogParser
from src.utils.utils import find_valid_sync_positions, split_ranges
from src.utils.log_config import logger

# Global state for Multiprocessing workers
SHARED_FMT_DEFINITIONS: Dict[int, Dict[str, Any]] = {}
SHARED_FILE_PATH: str = ""


# Multiprocessing Worker Functions
def _init_worker(fmt_definitions: Dict[int, Dict[str, Any]], file_path: str) -> None:
    """
    Initialize each worker process (called once per process).
    Stores format definitions globally and builds struct objects.
    """
    global SHARED_FMT_DEFINITIONS, SHARED_FILE_PATH
    SHARED_FILE_PATH = file_path
    SHARED_FMT_DEFINITIONS = {msg_id: dict(fmt) for msg_id, fmt in fmt_definitions.items()}

    _build_struct_objects(SHARED_FMT_DEFINITIONS)


def _build_struct_objects(fmt_definitions: Dict[int, Dict[str, Any]]) -> None:
    """Build struct.Struct objects for all message types."""
    for fmt in fmt_definitions.values():
        fmt["struct_obj"] = struct.Struct(fmt["struct_fmt"])


def _worker_process_segment(byte_offset_start: int, byte_offset_end: int, round_floats: bool,
                            message_filter: Optional[Set[str]]) -> str:
    """
    Worker function for multiprocessing pool.
    Uses globally shared format definitions.

    Returns:
        Path to temporary pickle file containing decoded messages.
    """
    try:
        messages = _parse_bin_segment(
            SHARED_FILE_PATH,
            SHARED_FMT_DEFINITIONS,
            byte_offset_start,
            byte_offset_end,
            round_floats,
            message_filter
        )
        return _save_messages_to_temp_file(messages)

    except Exception as err:
        logger.error(f"Worker failed ({byte_offset_start:,}-{byte_offset_end:,}): {err}")
        raise


# ThreadPool Worker Functions
def _worker_thread_segment(
        file_path: str,
        fmt_definitions: Dict[int, Dict[str, Any]],
        byte_offset_start: int,
        byte_offset_end: int,
        round_floats: bool,
        message_filter: Optional[Set[str]],
) -> str:
    """
    Worker function for thread pool.
    Receives format definitions as direct parameter.

    Returns:
        Path to temporary pickle file containing decoded messages.
    """
    try:
        messages = _parse_bin_segment(
            file_path,
            fmt_definitions,
            byte_offset_start,
            byte_offset_end,
            round_floats,
            message_filter
        )
        return _save_messages_to_temp_file(messages)

    except Exception as err:
        logger.error(f"Thread failed ({byte_offset_start:,}-{byte_offset_end:,}): {err}")
        raise


# Shared Helper Functions
def _parse_bin_segment(
        file_path: str,
        fmt_definitions: Dict[int, Dict[str, Any]],
        byte_offset_start: int,
        byte_offset_end: int,
        round_floats: bool,
        message_filter: Optional[Set[str]] = None
) -> List[Dict[str, Any]]:
    """
    Core decoding logic shared by both multiprocessing and threading.

    Returns:
        List of decoded messages (excluding FMT messages).
    """
    with open(file_path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_COPY)
        parser = BinLogParser(mm, format_definitions=fmt_definitions, round_floats=round_floats)

        messages = [
            message
            for message in parser.parse_messages_in_range(
                byte_offset_start,
                byte_offset_end,
                message_filter=message_filter
            )
            if message["message_type"] != "FMT"
        ]

        mm.close()

    return messages


def _save_messages_to_temp_file(messages: List[Dict[str, Any]]) -> str:
    """
    Save decoded messages to a temporary pickle file.

    Returns:
        Path to the temporary file.
    """
    temp_file = tempfile.mktemp(suffix=".pkl")
    with open(temp_file, "wb") as out:
        pickle.dump(messages, out, protocol=pickle.HIGHEST_PROTOCOL)
    return temp_file


def _load_and_merge_temp_files(temp_file_paths: List[str]) -> List[Dict[str, Any]]:
    """
        Combined list of all messages, sorted by TimeUS.
    """
    all_messages = []

    for temp_path in temp_file_paths:
        with open(temp_path, "rb") as f:
            all_messages.extend(pickle.load(f))
        os.remove(temp_path)

    all_messages.sort(key=lambda m: m.get("TimeUS", 0))
    return all_messages


# Main Decoder Class
class ParallelBinDecoder:
    """
    Parallel BIN log file decoder.
    Supports both multiprocessing and thread-based execution.
    """

    def __init__(
            self,
            file_path: str,
            num_workers: int = 4,
            round_floats: bool = True,
            running_mode: str = "process",
            message_filter: Optional[Set[str]] = None,
    ) -> None:

        self.file_path = file_path
        self.num_workers = num_workers
        self.round_floats = round_floats
        self.running_mode = running_mode
        self.message_filter = message_filter

    def run(self) -> List[Dict[str, Any]]:
        """
        Run parallel decoding on the BIN log file.

        Returns:
            List of all decoded messages, sorted by timestamp.
        """
        start_time = time.perf_counter()

        fmt_definitions, byte_ranges = self._load_formats_and_calculate_ranges()
        temp_files = self._process_all_segments(fmt_definitions, byte_ranges)
        all_messages = _load_and_merge_temp_files(temp_files)

        elapsed = time.perf_counter() - start_time
        print(f"[SUCCESS] Decoded {len(all_messages):,} messages in {elapsed:.2f}s")

        return all_messages

    def _load_formats_and_calculate_ranges(self) -> Tuple[Dict[int, Dict[str, Any]], List[Tuple[int, int]]]:
        """
        Load FMT definitions and calculate segment ranges for parallel processing.

        Returns:
            Tuple of (format_definitions, byte_ranges)
        """
        with open(self.file_path, "rb") as f:
            mapped = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

            parser = BinLogParser(mapped)
            parser.preload_fmt_messages()
            fmt_definitions = parser.fmt_definitions

            file_size = mapped.size()
            sync_positions = find_valid_sync_positions(mapped, fmt_definitions)
            byte_ranges = split_ranges(sync_positions, self.num_workers, file_size)

            mapped.close()

        return fmt_definitions, byte_ranges

    def _process_all_segments(
            self,
            fmt_definitions: Dict[int, Dict[str, Any]],
            byte_ranges: List[Tuple[int, int]]
    ) -> List[str]:
        """
        Process segments in parallel using either multiprocessing or threading.

        Returns:
            List of temporary file paths containing pickled results.
        """
        if self.running_mode == "process":
            return self._run_with_processes(fmt_definitions, byte_ranges)
        else:
            return self._run_with_threads(fmt_definitions, byte_ranges)

    def _run_with_processes(
            self,
            fmt_definitions: Dict[int, Dict[str, Any]],
            byte_ranges: List[Tuple[int, int]]
    ) -> List[str]:
        """Use multiprocessing pool for parallel processing."""
        logger.info(f" Using Multiprocessing Pool ({self.num_workers} processes)...")

        with Pool(
                processes=self.num_workers,
                initializer=_init_worker,
                initargs=(fmt_definitions, self.file_path),
        ) as pool:
            temp_files = pool.starmap(
                _worker_process_segment,
                [
                    (offset_start, offset_end, self.round_floats, self.message_filter)
                    for offset_start, offset_end in byte_ranges
                ],
            )

        return temp_files

    def _run_with_threads(
            self,
            fmt_definitions: Dict[int, Dict[str, Any]],
            byte_ranges: List[Tuple[int, int]]
    ) -> List[str]:
        """Use thread pool for parallel processing."""
        logger.info(f"Using ThreadPoolExecutor ({self.num_workers} threads)...")

        _build_struct_objects(fmt_definitions)

        temp_files = []
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [
                executor.submit(
                    _worker_thread_segment,
                    self.file_path,
                    fmt_definitions,
                    offset_start,
                    offset_end,
                    self.round_floats,
                    self.message_filter,
                )
                for offset_start, offset_end in byte_ranges
            ]

            for future in as_completed(futures):
                temp_files.append(future.result())

        return temp_files


