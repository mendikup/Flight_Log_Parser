import mmap
import time
from itertools import chain
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Dict, List, Tuple, Any, Optional, Set

from src.parser.bin_log_parser import BinLogParser
from src.pipeline.flight_segment_splitter import FlightSegmentSplitter
from src.config.log_config import logger


class ParallelBinDecoder:
    """
    High-performance parallel decoder for binary flight logs.
    Orchestrates file splitting and delegates segments to isolated processes.
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
        Execute the full parallel decoding pipeline from start to finish.
        Returns a single flattened list of all decoded messages.
        """
        start_time = time.perf_counter()

        format_definitions, byte_ranges = self._load_formats_and_calculate_ranges()
        list_of_message_lists = self._process_all_segments(format_definitions, byte_ranges)
        all_decoded_messages = list(chain.from_iterable(list_of_message_lists))

        elapsed_time = time.perf_counter() - start_time
        logger.info("Successfully decoded %s messages in %.2fs", f"{len(all_decoded_messages):,}", elapsed_time)
        print(f"[SUCCESS] Decoded {len(all_decoded_messages):,} messages in {elapsed_time:.2f}s")

        return all_decoded_messages

    def _load_formats_and_calculate_ranges(
            self,
    ) -> Tuple[Dict[int, Dict[str, Any]], List[Tuple[int, int]]]:
        """Extract FMT rules and divide the file into valid synchronization chunks."""
        with open(self.file_path, "rb") as file_handle:
            with mmap.mmap(file_handle.fileno(), 0, access=mmap.ACCESS_READ) as mapped_flight_log:
                parser = BinLogParser(mapped_flight_log)
                parser.preload_fmt_messages()
                format_definitions = parser.fmt_definitions

                file_size_bytes = mapped_flight_log.size()
                sync_positions = FlightSegmentSplitter.find_valid_sync_positions(mapped_flight_log, format_definitions)
                byte_ranges = FlightSegmentSplitter.split_ranges(sync_positions, self.num_workers, file_size_bytes)

        return format_definitions, byte_ranges

    def _process_all_segments(
            self,
            format_definitions: Dict[int, Dict[str, Any]],
            byte_ranges: List[Tuple[int, int]],
    ) -> List[List[Dict[str, Any]]]:
        """Dispatch segment processing to either multiprocessing or multithreading pools."""
        if self.running_mode == "process":
            return self._run_with_processes(format_definitions, byte_ranges)
        return self._run_with_threads(format_definitions, byte_ranges)

    def _run_with_processes(
            self,
            format_definitions: Dict[int, Dict[str, Any]],
            byte_ranges: List[Tuple[int, int]],
    ) -> List[List[Dict[str, Any]]]:
        """Run parallel decoding using an isolated multiprocessing pool."""
        logger.info("Initializing Multiprocessing Pool with %s workers...", self.num_workers)

        # Strip unpicklable struct objects before passing to workers
        serializable_formats = {
            msg_id: {k: v for k, v in definition.items() if k != "struct_obj"}
            for msg_id, definition in format_definitions.items()
        }

        task_arguments = [
            (
                self.file_path,
                serializable_formats,
                start_offset,
                end_offset,
                self.round_floats,
                self.message_filter,
            )
            for start_offset, end_offset in byte_ranges
        ]

        with Pool(processes=self.num_workers) as process_pool:
            return process_pool.starmap(_worker_process_segment, task_arguments)

    def _run_with_threads(
            self,
            format_definitions: Dict[int, Dict[str, Any]],
            byte_ranges: List[Tuple[int, int]],
    ) -> List[List[Dict[str, Any]]]:
        """Run parallel decoding using a ThreadPoolExecutor (best for lightweight I/O)."""
        logger.info("Initializing ThreadPoolExecutor with %s threads...", self.num_workers)
        results = []

        with ThreadPoolExecutor(max_workers=self.num_workers) as thread_pool:
            futures: List[Future] = [
                thread_pool.submit(
                    _worker_process_segment,
                    self.file_path,
                    format_definitions,
                    start_offset,
                    end_offset,
                    self.round_floats,
                    self.message_filter,
                )
                for start_offset, end_offset in byte_ranges
            ]
            for future in futures:
                results.append(future.result())

        return results


# ============================================================
# Global Worker Function (Isolated for Pickle Compatibility)
# ============================================================

def _worker_process_segment(
        file_path: str,
        format_definitions: Dict[int, Dict[str, Any]],
        byte_offset_start: int,
        byte_offset_end: int,
        round_floats: bool,
        message_filter: Optional[Set[str]],
) -> List[Dict[str, Any]]:
    """
    Isolated worker function. Maps its own memory segment, initializes the parser,
    and returns a clean list of decoded dictionaries.
    """
    try:
        with open(file_path, "rb") as file_handle:
            with mmap.mmap(file_handle.fileno(), 0, access=mmap.ACCESS_READ) as mapped_flight_log:
                # The Parser now handles struct compilation internally via _ensure_structs_compiled
                parser = BinLogParser(
                    mapped_flight_log=mapped_flight_log,
                    format_definitions=format_definitions,
                    round_floats=round_floats,
                )

                decoded_messages = [
                    message
                    for message in parser.parse_messages_in_range(
                        start_offset=byte_offset_start,
                        end_offset=byte_offset_end,
                        message_filter=message_filter,
                    )
                    if message["message_type"] != "FMT"
                ]

        return decoded_messages

    except Exception as error:
        logger.error(
            "Worker process failed in range %s-%s: %s",
            f"{byte_offset_start:,}", f"{byte_offset_end:,}", error
        )
        raise