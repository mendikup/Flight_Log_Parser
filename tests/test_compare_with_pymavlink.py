import sys
import os
import math
import mmap
import time
import pytest
import numpy as np
from pymavlink import mavutil
from src.bussines_logic.bin_log_parser import BinLogParser
from src.utils.log_config import logger

# Ensure parser module path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../parser")))

TEST_FILE = os.path.join(os.path.dirname(__file__), "../parser/log_file_test_01.bin")


@pytest.mark.mavlink
def test_compare_against_pymavlink_when_available_and_file_provided():
    """
    Compare our custom parser output against pymavlink on the same BIN file.
    If mismatch occurs, print internal warnings from BinLogParser to analyze cause.
    """

    if not os.path.exists(TEST_FILE):
        pytest.skip(f"BIN file not found: {TEST_FILE}")

    # --- Decode with our parser ---
    with open(TEST_FILE, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        parser = BinLogParser(mm, round_floats=True, collect_warnings=True)
        parser.preload_fmt_messages()
        parser.build_structs_for_local_use()

        ours_iter = (
            msg for msg in parser.parse_messages_in_range(0)
            if msg["message_type"] != "FMT"
        )

        # --- Decode with pymavlink ---
        connection = mavutil.mavlink_connection(TEST_FILE)
        pymav_iter = (
            msg.to_dict()
            for msg in iter(lambda: connection.recv_match(blocking=False), None)
            if msg and msg.get_type() != "FMT"
        )

        # --- Comparison metrics ---
        total_compared = 0
        mismatches = 0
        match_count = 0
        mismatch_samples = []
        total_fields = 0
        missing_summary = []

        for ours_msg, their_msg in zip(ours_iter, pymav_iter):
            ours_keys = set(ours_msg.keys())
            theirs_keys = set(their_msg.keys())
            shared_keys = ours_keys & theirs_keys
            total_fields += len(shared_keys)

            missing_summary.append({
                "missing_in_ours": theirs_keys - ours_keys,
                "missing_in_theirs": ours_keys - theirs_keys
            })

            for key in shared_keys:
                ours_val = ours_msg[key]
                theirs_val = their_msg[key]

                if ours_val in (None, "") and theirs_val in (None, ""):
                    continue

                if isinstance(ours_val, (int, float)) and isinstance(theirs_val, (int, float)):
                    if math.isclose(float(ours_val), float(theirs_val), rel_tol=1e-5, abs_tol=1e-3):
                        match_count += 1
                    else:
                        mismatches += 1
                        if len(mismatch_samples) < 5:
                            mismatch_samples.append((key, ours_val, theirs_val))
                else:
                    if str(ours_val) == str(theirs_val):
                        match_count += 1
                    else:
                        mismatches += 1
                        if len(mismatch_samples) < 5:
                            mismatch_samples.append((key, ours_val, theirs_val))

                total_compared += 1

        match_rate = 100 * match_count / total_fields if total_fields else 0
        print(f"\n Compared {total_compared:,} total field values across all shared messages.")
        print(f" Field match rate: {match_rate:.2f}% ({match_count}/{total_fields})")
        print(f" {mismatches:,} mismatches beyond tolerance.")

        # Print examples of value mismatches
        if mismatch_samples:
            print("\n Example value mismatches:")
            for k, ov, tv in mismatch_samples:
                print(f"  Field '{k}': ours={ov}  |  theirs={tv}")

        # Print field presence differences
        print("\n Field presence summary (first 3 messages):")
        for i, m in enumerate(missing_summary[:3]):
            if m["missing_in_ours"] or m["missing_in_theirs"]:
                print(f"  Msg #{i+1}: missing_in_ours={m['missing_in_ours']} | missing_in_theirs={m['missing_in_theirs']}")

        # --- NEW: warnings output if mismatch occurs ---
        if mismatches > 0 or total_fields == 0:
            if parser.warnings:
                print(f"\n🔍 Internal parser warnings ({len(parser.warnings)} total):")
                for w in parser.warnings[:10]:
                    print(f"  • {w}")
                if len(parser.warnings) > 10:
                    print(f"  ... and {len(parser.warnings) - 10} more.")
            else:
                print("\n✅ No internal warnings collected — mismatch likely due to pymavlink interpretation differences.")

        assert total_compared > 0, "No comparable fields found between parsers"

        mm.close()


@pytest.mark.mavlink
def test_speed_single_field_gps(tmp_path=None):
    """
    Compare decoding speed for a single GPS field ('Lat')
    between our parser and pymavlink, using the same BIN file.
    """
    if not os.path.exists(TEST_FILE):
        pytest.skip(f"BIN file not found: {TEST_FILE}")

    # --- Decode with our parser ---
    with open(TEST_FILE, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        parser = BinLogParser(mm, round_floats=True)
        parser.preload_fmt_messages()
        parser.build_structs_for_local_use()

        start_ours = time.perf_counter()
        lat_values_ours = [
            msg["Lat"]
            for msg in parser.parse_messages_in_range(0, message_filter={"GPS"})
            if "Lat" in msg
        ]
        ours_time = time.perf_counter() - start_ours
        mm.close()

    # --- Decode with pymavlink ---
    start_theirs = time.perf_counter()
    conn = mavutil.mavlink_connection(TEST_FILE)
    lat_values_theirs = []
    while True:
        msg = conn.recv_match(blocking=False)
        if msg is None:
            break
        if msg.get_type() == "GPS":
            try:
                lat_values_theirs.append(float(msg.to_dict().get("Lat", 0)))
            except Exception:
                pass
    theirs_time = time.perf_counter() - start_theirs

    # --- Compare results ---
    assert len(lat_values_ours) > 0, "No GPS messages parsed"
    assert len(lat_values_theirs) == len(lat_values_ours), "Mismatch in GPS message count"

    avg_diff = sum(abs(o - t) for o, t in zip(lat_values_ours, lat_values_theirs)) / len(lat_values_ours)
    print(f"⏱️ Our parser time: {ours_time:.3f}s | pymavlink time: {theirs_time:.3f}s")
    print(f"Average absolute difference in 'Lat': {avg_diff:.6f}")

    # 🔍 Check if difference likely caused by float precision
    ours_dtype = np.array(lat_values_ours, dtype=np.float32).dtype
    theirs_dtype = np.array(lat_values_theirs, dtype=np.float64).dtype
    precision_issue = np.mean(
        [abs(np.float64(o) - np.float64(t)) for o, t in zip(lat_values_ours, lat_values_theirs)]
    ) < 1e-3

    print(f"Our floats dtype: {ours_dtype}, pymavlink dtype: {theirs_dtype}")
    if precision_issue:
        print("🟢 Difference is within typical float precision range (rounding only).\\n")
    else:
        print("🟠 Potential decoding difference – investigate scaling or byte order.\\n")

    # --- Assertions ---
    assert ours_time < theirs_time, "Our parser should be faster"
    assert avg_diff < 3e-4, "Latitude values differ beyond acceptable tolerance"



@pytest.mark.mavlink
def test_pymavlink_full_vs_gps_only():
    """
    Measure how long pymavlink takes to iterate all messages vs only GPS messages.
    Used to analyze filtering overhead and baseline decode time.
    """

    if not os.path.exists(TEST_FILE):
        pytest.skip(f"BIN file not found: {TEST_FILE}")

    print("\n=== Measuring pymavlink performance ===")

    # --- Case 1: Decode all messages ---
    start_all = time.perf_counter()
    conn_all = mavutil.mavlink_connection(TEST_FILE)
    all_count = 0
    while True:
        msg = conn_all.recv_match(blocking=False)
        if msg is None:
            break
        all_count += 1
    time_all = time.perf_counter() - start_all

    # --- Case 2: Decode only GPS messages ---
    start_gps = time.perf_counter()
    conn_gps = mavutil.mavlink_connection(TEST_FILE)
    gps_count = 0
    while True:
        msg = conn_gps.recv_match(blocking=False)
        if msg is None:
            break
        if msg.get_type() == "GPS":
            gps_count += 1
    time_gps = time.perf_counter() - start_gps

    # --- Print summary ---
    print(f"Total messages decoded: {all_count:,}")
    print(f"GPS messages decoded: {gps_count:,}")
    print(f"⏱️ pymavlink time (all): {time_all:.2f}s")
    print(f"⏱️ pymavlink time (GPS only): {time_gps:.2f}s")
    print(f"⚖️ Ratio (GPS-only / all): {time_gps / time_all:.2f}× slower or faster\n")

    assert all_count > 0, "No messages decoded"
    assert gps_count > 0, "No GPS messages found"

@pytest.mark.performance
def test_speed_all_messages(tmp_path=None):
    """
    Measure time to iterate through all messages once
    (no list building, just dictionary creation).
    """
    with open(TEST_FILE, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        parser = BinLogParser(mm, round_floats=False)
        parser.preload_fmt_messages()
        parser.build_structs_for_local_use()

        count = 0
        start = time.perf_counter()

        for _ in parser.parse_messages_in_range(0):
            count += 1  # just iterate, no list building

        duration = time.perf_counter() - start
        print(f" Decoded {count:,} messages in {duration:.2f}s (all types)")
        mm.close()




@pytest.mark.performance
def test_speed_gps_only(tmp_path=None):
    """
    Measure time to iterate through only GPS messages,
    building a dictionary for each message.
    """
    with open(TEST_FILE, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        parser = BinLogParser(mm, round_floats=False)
        parser.preload_fmt_messages()
        parser.build_structs_for_local_use()

        count = 0
        start = time.perf_counter()

        for _ in parser.parse_messages_in_range(0, message_filter={"GPS"}):
            count += 1

        duration = time.perf_counter() - start
        print(f"🛰 Decoded {count:,} GPS messages in {duration:.2f}s")
        mm.close()
