import mmap
import struct
import time
from itertools import chain
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Dict, List, Tuple, Any, Optional, Set

from src.parser.bin_log_parser import BinLogParser
from src.pipeline.flight_segment_splitter import FlightSegmentSplitter
from src.config.log_config import logger


def _worker_process_segment(
        file_path: str,
        format_definitions: Dict[int, Dict[str, Any]],
        byte_offset_start: int,
        byte_offset_end: int,
        round_floats: bool,
        message_filter: Optional[Set[str]],
) -> List[Dict[str, Any]]:
    """
    Worker function executed in a separate process.
    Opens the file safely using memory-mapping and decodes its assigned segment.
    """
    try:
        with open(file_path, "rb") as file_handle:
            with mmap.mmap(file_handle.fileno(), 0, access=mmap.ACCESS_READ) as mapped_flight_log:

                # Rebuild struct objects locally inside the worker process
                local_format_definitions = {
                    msg_id: dict(definition) for msg_id, definition in format_definitions.items()
                }
                for definition in local_format_definitions.values():
                    if "struct_fmt" in definition:
                        definition["struct_obj"] = struct.Struct(definition["struct_fmt"])

                parser = BinLogParser(
                    mapped_flight_log=mapped_flight_log,
                    format_definitions=local_format_definitions,
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


class ParallelBinDecoder:
    """
    High-performance parallel decoder for binary flight logs.
    Supports both multiprocessing and multithreading execution modes.
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

        # Process all file segments concurrently
        list_of_message_lists = self._process_all_segments(format_definitions, byte_ranges)

        # Flatten results while maintaining original file order
        all_decoded_messages = list(chain.from_iterable(list_of_message_lists))

        elapsed_time = time.perf_counter() - start_time
        logger.info("Successfully decoded %s messages in %.2fs", f"{len(all_decoded_messages):,}", elapsed_time)
        print(f"[SUCCESS] Decoded {len(all_decoded_messages):,} messages in {elapsed_time:.2f}s")

        return all_decoded_messages

    def _load_formats_and_calculate_ranges(
            self,
    ) -> Tuple[Dict[int, Dict[str, Any]], List[Tuple[int, int]]]:
        """Load format definitions and split the file into balanced execution segments."""
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
        """Dispatch segment processing based on the chosen execution mode."""
        if self.running_mode == "process":
            return self._run_with_processes(format_definitions, byte_ranges)
        else:
            return self._run_with_threads(format_definitions, byte_ranges)

    def _run_with_processes(
            self,
            format_definitions: Dict[int, Dict[str, Any]],
            byte_ranges: List[Tuple[int, int]],
    ) -> List[List[Dict[str, Any]]]:
        """Run parallel decoding using a multiprocessing pool without global variables."""
        logger.info("Initializing Multiprocessing Pool with %s workers...", self.num_workers)

        # Strip unpicklable struct objects before passing definitions across processes
        serializable_format_definitions = {
            message_id: {key: value for key, value in definition.items() if key != "struct_obj"}
            for message_id, definition in format_definitions.items()
        }

        task_arguments = [
            (
                self.file_path,
                serializable_format_definitions,
                range_start,
                range_end,
                self.round_floats,
                self.message_filter,
            )
            for range_start, range_end in byte_ranges
        ]

        with Pool(processes=self.num_workers) as process_pool:
            list_of_message_lists = process_pool.starmap(
                _worker_process_segment,
                task_arguments,
            )

        return list_of_message_lists

    def _run_with_threads(
            self,
            format_definitions: Dict[int, Dict[str, Any]],
            byte_ranges: List[Tuple[int, int]],
    ) -> List[List[Dict[str, Any]]]:
        """Run parallel decoding using a thread pool executor."""
        logger.info("Initializing ThreadPoolExecutor with %s threads...", self.num_workers)

        list_of_message_lists: List[List[Dict[str, Any]]] = []

        with ThreadPoolExecutor(max_workers=self.num_workers) as thread_pool:
            futures: List[Future] = [
                thread_pool.submit(
                    _worker_process_segment,
                    self.file_path,
                    format_definitions,
                    range_start,
                    range_end,
                    self.round_floats,
                    self.message_filter,
                )
                for range_start, range_end in byte_ranges
            ]

            for future in futures:
                list_of_message_lists.append(future.result())

        return list_of_message_lists