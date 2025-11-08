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


#  Global state for Multiprocessing workers
SHARED_FMT_DEFINITIONS: Dict[int, Dict[str, Any]] = {}
SHARED_FILE_PATH: str = ""


# ️ Multiprocessing Worker Initialization
def _init_worker(format_definitions: Dict[int, Dict[str, Any]], file_path: str) -> None:
    """
    Initialize each worker process (called once per process).
    Stores format definitions globally and builds struct objects.
    """
    global SHARED_FMT_DEFINITIONS, SHARED_FILE_PATH
    SHARED_FILE_PATH = file_path
    SHARED_FMT_DEFINITIONS = {message_id: dict(fmt) for message_id, fmt in format_definitions.items()}

    _build_struct_objects(SHARED_FMT_DEFINITIONS)


def _build_struct_objects(format_definitions: Dict[int, Dict[str, Any]]) -> None:
    """Build struct.Struct objects for all message types."""
    for fmt_definition in format_definitions.values():
        fmt_definition["struct_obj"] = struct.Struct(fmt_definition["struct_fmt"])



#  Worker Functions (Processes / Threads)
def _worker_process_segment(
    byte_offset_start: int,
    byte_offset_end: int,
    round_floats: bool,
    message_filter: Optional[Set[str]],
) -> str:
    """
    Worker function for multiprocessing pool.
    Uses globally shared format definitions.

    Returns:
        Path to temporary pickle file containing decoded messages.
    """
    try:
        decoded_messages = _parse_bin_segment(
            SHARED_FILE_PATH,
            SHARED_FMT_DEFINITIONS,
            byte_offset_start,
            byte_offset_end,
            round_floats,
            message_filter,
        )
        return _save_messages_to_temp_file(decoded_messages)

    except Exception as error:
        logger.error(f"Worker failed ({byte_offset_start:,}-{byte_offset_end:,}): {error}")
        raise


def _worker_thread_segment(
    file_path: str,
    format_definitions: Dict[int, Dict[str, Any]],
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
        decoded_messages = _parse_bin_segment(
            file_path,
            format_definitions,
            byte_offset_start,
            byte_offset_end,
            round_floats,
            message_filter,
        )
        return _save_messages_to_temp_file(decoded_messages)

    except Exception as error:
        logger.error(f"Thread failed ({byte_offset_start:,}-{byte_offset_end:,}): {error}")
        raise


#  Shared Helper Functions
def _parse_bin_segment(
    file_path: str,
    format_definitions: Dict[int, Dict[str, Any]],
    byte_offset_start: int,
    byte_offset_end: int,
    round_floats: bool,
    message_filter: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Core decoding logic shared by both multiprocessing and threading.

    Returns:
        List of decoded messages (excluding FMT messages).
    """
    with open(file_path, "rb") as file_handle:
        mapped_log_file = mmap.mmap(file_handle.fileno(), 0, access=mmap.ACCESS_COPY)
        parser = BinLogParser(
            mapped_log_file,
            format_definitions=format_definitions,
            round_floats=round_floats,
        )

        decoded_messages = [
            message
            for message in parser.parse_messages_in_range(
                byte_offset_start,
                byte_offset_end,
                message_filter=message_filter,
            )
            if message["message_type"] != "FMT"
        ]

        mapped_log_file.close()

    return decoded_messages


def _save_messages_to_temp_file(decoded_messages: List[Dict[str, Any]]) -> str:
    """
    Save decoded messages to a temporary pickle file.

    Returns:
        Path to the temporary file.
    """
    temporary_pickle_path = tempfile.mktemp(suffix=".pkl")
    with open(temporary_pickle_path, "wb") as output_file:
        pickle.dump(decoded_messages, output_file, protocol=pickle.HIGHEST_PROTOCOL)
    return temporary_pickle_path


def _load_and_merge_temp_files(temp_file_paths: List[str]) -> List[Dict[str, Any]]:
    """
    Load all temporary pickle files, merge and sort messages by timestamp.
    """
    all_decoded_messages: List[Dict[str, Any]] = []

    for temporary_path in temp_file_paths:
        with open(temporary_path, "rb") as temp_file:
            all_decoded_messages.extend(pickle.load(temp_file))
        os.remove(temporary_path)

    all_decoded_messages.sort(key=lambda message: message.get("TimeUS", 0))
    return all_decoded_messages


#  Main Parallel Decoder Class
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

        format_definitions, byte_ranges = self._load_formats_and_calculate_ranges()
        temporary_file_paths = self._process_all_segments(format_definitions, byte_ranges)
        all_decoded_messages = _load_and_merge_temp_files(temporary_file_paths)

        elapsed_time = time.perf_counter() - start_time
        print(f"[SUCCESS] Decoded {len(all_decoded_messages):,} messages in {elapsed_time:.2f}s")

        return all_decoded_messages

    def _load_formats_and_calculate_ranges(self) -> Tuple[Dict[int, Dict[str, Any]], List[Tuple[int, int]]]:
        """
        Load FMT definitions and calculate segment ranges for parallel processing.

        Returns:
            Tuple of (format_definitions, byte_ranges)
        """
        with open(self.file_path, "rb") as file_handle:
            mapped_log_file = mmap.mmap(file_handle.fileno(), 0, access=mmap.ACCESS_READ)

            parser = BinLogParser(mapped_log_file)
            parser.preload_fmt_messages()
            format_definitions = parser.fmt_definitions

            file_size_bytes = mapped_log_file.size()
            sync_positions = find_valid_sync_positions(mapped_log_file, format_definitions)
            byte_ranges = split_ranges(sync_positions, self.num_workers, file_size_bytes)

            mapped_log_file.close()

        return format_definitions, byte_ranges


    def _process_all_segments(
        self,
        format_definitions: Dict[int, Dict[str, Any]],
        byte_ranges: List[Tuple[int, int]],
    ) -> List[str]:
        """
        Process all segments in parallel using either multiprocessing or threading.

        Returns:
            List of temporary file paths containing pickled results.
        """
        if self.running_mode == "process":
            return self._run_with_processes(format_definitions, byte_ranges)
        else:
            return self._run_with_threads(format_definitions, byte_ranges)


    def _run_with_processes(
        self,
        format_definitions: Dict[int, Dict[str, Any]],
        byte_ranges: List[Tuple[int, int]],
    ) -> List[str]:
        """Use multiprocessing pool for parallel processing."""
        logger.info(f"Using Multiprocessing Pool ({self.num_workers} processes)...")

        with Pool(
            processes=self.num_workers,
            initializer=_init_worker,
            initargs=(format_definitions, self.file_path),
        ) as process_pool:
            temporary_file_paths = process_pool.starmap(
                _worker_process_segment,
                [
                    (range_start, range_end, self.round_floats, self.message_filter)
                    for range_start, range_end in byte_ranges
                ],
            )

        return temporary_file_paths


    def _run_with_threads(
        self,
        format_definitions: Dict[int, Dict[str, Any]],
        byte_ranges: List[Tuple[int, int]],
    ) -> List[str]:
        """Use thread pool for parallel processing."""
        logger.info(f"Using ThreadPoolExecutor ({self.num_workers} threads)...")

        _build_struct_objects(format_definitions)

        temporary_file_paths: List[str] = []
        with ThreadPoolExecutor(max_workers=self.num_workers) as thread_pool:
            futures = [
                thread_pool.submit(
                    _worker_thread_segment,
                    self.file_path,
                    format_definitions,
                    range_start,
                    range_end,
                    self.round_floats,
                    self.message_filter,
                )
                for range_start, range_end in byte_ranges
            ]

            for future in as_completed(futures):
                temporary_file_paths.append(future.result())

        return temporary_file_paths
