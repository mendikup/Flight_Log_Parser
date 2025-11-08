import os
import math
import mmap
import time
import numpy as np
from pymavlink import mavutil
from src.bussines_logic.bin_log_parser import BinLogParser
from src.utils.utils import build_structs_for_local_use
from src.utils.log_config import setup_test_logger

logger = setup_test_logger()

TEST_FILE = os.path.join(os.path.dirname(__file__), "../src/log_file_test_01.bin")


def test_compare_against_pymavlink_when_available_and_file_provided():
    """Compare output of our parser vs pymavlink, field by field."""
    if not os.path.exists(TEST_FILE):
        import pytest
        pytest.skip(f"BIN file not found: {TEST_FILE}")

    with open(TEST_FILE, "rb") as file_handle:
        mapped_log_file = mmap.mmap(file_handle.fileno(), 0, access=mmap.ACCESS_READ)
        parser = BinLogParser(mapped_log_file, round_floats=True, collect_warnings=True)
        parser.preload_fmt_messages()
        build_structs_for_local_use(parser.fmt_definitions)

        our_messages = (
            message for message in parser.parse_messages_in_range(0)
            if message["message_type"] != "FMT"
        )

        connection = mavutil.mavlink_connection(TEST_FILE)
        pymav_messages = (
            msg.to_dict()
            for msg in iter(lambda: connection.recv_match(blocking=False), None)
            if msg and msg.get_type() != "FMT"
        )

        total_compared = 0
        match_count = 0
        mismatch_count = 0
        mismatch_samples = []
        total_fields = 0
        missing_summary = []

        for our_msg, their_msg in zip(our_messages, pymav_messages):
            our_keys = set(our_msg.keys())
            their_keys = set(their_msg.keys())
            shared_keys = our_keys & their_keys
            total_fields += len(shared_keys)

            missing_summary.append({
                "missing_in_ours": their_keys - our_keys,
                "missing_in_theirs": our_keys - their_keys,
            })

            for key in shared_keys:
                our_val = our_msg[key]
                their_val = their_msg[key]

                if isinstance(our_val, (int, float)) and isinstance(their_val, (int, float)):
                    if math.isclose(float(our_val), float(their_val), rel_tol=1e-5, abs_tol=1e-3):
                        match_count += 1
                    else:
                        mismatch_count += 1
                        if len(mismatch_samples) < 5:
                            mismatch_samples.append((key, our_val, their_val))
                else:
                    if str(our_val) == str(their_val):
                        match_count += 1
                    else:
                        mismatch_count += 1
                        if len(mismatch_samples) < 5:
                            mismatch_samples.append((key, our_val, their_val))

                total_compared += 1

        match_rate = 100 * match_count / total_fields if total_fields else 0
        logger.info(f"Compared {total_compared:,} fields | Match rate: {match_rate:.2f}% | Mismatches: {mismatch_count:,}")

        for key, ours, theirs in mismatch_samples:
            logger.warning(f"Field '{key}': ours={ours} | theirs={theirs}")

        if mismatch_count > 0 and parser.warnings:
            logger.warning(f"Parser warnings ({len(parser.warnings)} total): {parser.warnings[:5]}")

        assert total_compared > 0, "No comparable fields found between parsers"
        mapped_log_file.close()


def test_speed_all_messages():
    """Measure time to decode all messages using our parser."""
    if not os.path.exists(TEST_FILE):
        import pytest
        pytest.skip(f"BIN file not found: {TEST_FILE}")

    with open(TEST_FILE, "rb") as file_handle:
        mapped_log_file = mmap.mmap(file_handle.fileno(), 0, access=mmap.ACCESS_READ)
        parser = BinLogParser(mapped_log_file, round_floats=False)
        parser.preload_fmt_messages()
        build_structs_for_local_use(parser.fmt_definitions)

        start_time = time.perf_counter()
        total_count = sum(1 for _ in parser.parse_messages_in_range(0))
        elapsed = time.perf_counter() - start_time

        logger.info(f"Decoded {total_count:,} messages in {elapsed:.2f}s (all types)")
        mapped_log_file.close()
