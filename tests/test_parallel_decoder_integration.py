from src.bussines_logic.controller import ParallelBinDecoder
from src.utils.log_config import setup_test_logger

logger = setup_test_logger()


def test_parallel_end_to_end(tmp_synthetic_file):
    """Full integration test for ParallelBinDecoder (multiprocessing mode)."""
    decoder = ParallelBinDecoder(tmp_synthetic_file, num_workers=2, round_floats=True)
    decoded_messages = decoder.run()

    logger.info(f"Decoded {len(decoded_messages)} messages in parallel mode.")
    assert len(decoded_messages) == 3
    time_stamps = [msg.get("TimeUS", 0) for msg in decoded_messages]
    assert time_stamps == sorted(time_stamps)
    assert decoded_messages[0]["Note"] in ("hello", "world", "")


def test_parallel_message_filter(tmp_synthetic_file):
    """Ensure message_filter works as expected for both valid and invalid filters."""
    decoder_valid = ParallelBinDecoder(tmp_synthetic_file, num_workers=2, round_floats=False, message_filter={"TST"})
    decoded_messages = decoder_valid.run()
    logger.info(f"Filtered TST messages: {len(decoded_messages)}")

    assert len(decoded_messages) == 3
    assert all(msg["message_type"] == "TST" for msg in decoded_messages)

    decoder_empty = ParallelBinDecoder(tmp_synthetic_file, num_workers=2, round_floats=False, message_filter={"GPS"})
    empty_messages = decoder_empty.run()
    logger.info(f"Filtered GPS messages: {len(empty_messages)}")

    assert len(empty_messages) == 0


def test_parallel_threadpool_mode(tmp_synthetic_file):
    """Validate that ThreadPool mode produces identical results as process mode."""
    process_decoder = ParallelBinDecoder(tmp_synthetic_file, num_workers=2, round_floats=True, running_mode="process")
    thread_decoder = ParallelBinDecoder(tmp_synthetic_file, num_workers=2, round_floats=True, running_mode="thread")

    process_results = process_decoder.run()
    thread_results = thread_decoder.run()

    logger.info(f"Process vs Thread results: {len(process_results)} / {len(thread_results)} messages.")
    assert len(process_results) == len(thread_results)
    assert [m["TimeUS"] for m in process_results] == [m["TimeUS"] for m in thread_results]


def test_no_fmt_messages_in_results(tmp_synthetic_file):
    """Ensure no FMT messages appear in final merged output."""
    decoder = ParallelBinDecoder(tmp_synthetic_file, num_workers=2)
    decoded_messages = decoder.run()
    assert all(m["message_type"] != "FMT" for m in decoded_messages)
    logger.info("Verified that no FMT messages exist in merged output.")
