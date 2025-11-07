import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../parser')))

from parallal_decoder import ParallelBinDecoder


def test_parallel_end_to_end(tmp_synthetic_file):
    """
    Full end-to-end integration test for the parallel decoder.

    Verifies that:
    - The decoder runs successfully with multiprocessing
    - All 3 synthetic messages are decoded
    - Messages are sorted by TimeUS
    - String fields (e.g. 'Note') are properly decoded
    """
    dec = ParallelBinDecoder(tmp_synthetic_file, num_workers=2, round_floats=True)
    messages = dec.run()

    assert len(messages) == 3, "Expected 3 decoded messages"
    times = [m.get("TimeUS", 0) for m in messages]
    assert times == sorted(times), "Messages must be sorted by TimeUS"
    assert messages[0]["Note"] in ("hello", "world", ""), "String fields should be decoded properly"


def test_parallel_message_filter(tmp_synthetic_file):
    """
    Ensure that message_filter works correctly in parallel decoding.

    Case 1: Filter includes 'TST' → expect messages
    Case 2: Filter includes a nonexistent type → expect 0 results
    """
    # Filter for 'TST' messages only
    dec = ParallelBinDecoder(
        tmp_synthetic_file,
        num_workers=2,
        round_floats=False,
        running_mode="process",
        message_filter={"TST"},
    )
    msgs = dec.run()
    assert len(msgs) == 3, "Expected 3 messages of type 'TST'"
    assert all(m["message_type"] == "TST" for m in msgs), "All messages should be type 'TST'"

    # Filter for nonexistent type (should return empty list)
    dec_empty = ParallelBinDecoder(
        tmp_synthetic_file,
        num_workers=2,
        round_floats=False,
        message_filter={"GPS"},
    )
    msgs_empty = dec_empty.run()
    assert len(msgs_empty) == 0, "Expected no messages for nonexistent filter type"


def test_parallel_threadpool_mode(tmp_synthetic_file):
    """
    Verify that using ThreadPool mode produces the same results
    as multiprocessing mode.
    """
    # Run with process pool
    proc_decoder = ParallelBinDecoder(
        tmp_synthetic_file,
        num_workers=2,
        round_floats=True,
        running_mode=False,
    )
    proc_msgs = proc_decoder.run()

    # Run with thread pool
    thread_decoder = ParallelBinDecoder(
        tmp_synthetic_file,
        num_workers=2,
        round_floats=True,
        running_mode=True,
    )
    thread_msgs = thread_decoder.run()

    # Should produce the same count and sorted results
    assert len(proc_msgs) == len(thread_msgs)
    assert [m["TimeUS"] for m in proc_msgs] == [m["TimeUS"] for m in thread_msgs]


def test_no_fmt_messages_in_results(tmp_synthetic_file):
    """
    Confirm that FMT messages are excluded from final merged output.
    """
    dec = ParallelBinDecoder(tmp_synthetic_file, num_workers=2)
    msgs = dec.run()
    assert all(m["message_type"] != "FMT" for m in msgs), "FMT messages should not appear in final results"
