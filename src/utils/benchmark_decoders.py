# benchmark_decoders.py
import os
import time
import mmap
import struct
import pickle
import tempfile
import argparse
from typing import Dict, List, Tuple, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Pool

from src.bussines_logic.bin_log_parser import BinLogParser
from src.utils.utils import find_valid_sync_positions, split_ranges



def _try_import_pymavlink():
    try:
        from pymavlink import mavutil  # type: ignore
        return mavutil
    except Exception:
        return None


def _ensure_structs_locally(fmt_defs: Dict[int, Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """Make a shallow copy of fmt_defs and ensure each has struct_obj built."""
    copy_defs: Dict[int, Dict[str, Any]] = {k: dict(v) for k, v in fmt_defs.items()}
    for fd in copy_defs.values():
        if "struct_obj" not in fd:
            fd["struct_obj"] = struct.Struct(fd["struct_fmt"])
    return copy_defs


def _build_fmt_and_ranges(file_path: str, num_parts: int) -> Tuple[Dict[int, Dict[str, Any]], List[Tuple[int, int]], int]:
    """Load FMT defs from the BIN and compute balanced ranges."""
    with open(file_path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        parser = BinLogParser(mm)
        parser.preload_fmt_messages()
        fmt_defs = parser.fmt_definitions
        size = mm.size()
        syncs = find_valid_sync_positions(mm, fmt_defs)
        ranges = split_ranges(syncs, num_parts, size)
        mm.close()
    return fmt_defs, ranges, size



def _worker_decode_count(file_path: str, fmt_defs: Dict[int, Dict[str, Any]],
                         start: int, end: int, round_floats: bool) -> int:
    """Decode to dicts but don't store them – count only."""
    with open(file_path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        parser = BinLogParser(mm, format_definitions=fmt_defs, round_floats=round_floats)
        cnt = 0
        for msg in parser.parse_messages_in_range(start, end):
            if msg.get("message_type") != "FMT":
                cnt += 1
        mm.close()
    return cnt


def _worker_decode_collect(file_path: str, fmt_defs: Dict[int, Dict[str, Any]],
                           start: int, end: int, round_floats: bool) -> str:
    """Decode and collect messages into a temp pickle file."""
    with open(file_path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        parser = BinLogParser(mm, format_definitions=fmt_defs, round_floats=round_floats)
        out: List[Dict[str, Any]] = []
        for msg in parser.parse_messages_in_range(start, end):
            if msg.get("message_type") != "FMT":
                out.append(msg)
        mm.close()

    tmp = tempfile.mktemp(suffix=".pkl")
    with open(tmp, "wb") as fh:
        pickle.dump(out, fh, protocol=pickle.HIGHEST_PROTOCOL)
    return tmp


#  GLOBAL WRAPPERS FOR MULTIPROCESSING

def _job_count_global(args):
    fp, defs, ss, ee, rf = args
    local_defs = _ensure_structs_locally(defs)
    return _worker_decode_count(fp, local_defs, ss, ee, rf)


def _job_collect_global(args):
    fp, defs, s, e, rf = args
    local_defs = _ensure_structs_locally(defs)
    return _worker_decode_collect(fp, local_defs, s, e, rf)




def run_mp_decode(file_path: str, fmt_defs: Dict[int, Dict[str, Any]], ranges: List[Tuple[int, int]],
                  round_floats: bool, keep_list: bool, workers: int) -> Tuple[int, Optional[List[Dict[str, Any]]], float]:
    safe_defs = {i: {k: v for k, v in fd.items() if k != "struct_obj"} for i, fd in fmt_defs.items()}
    t0 = time.perf_counter()

    if keep_list:
        jobs = [(file_path, safe_defs, s, e, round_floats) for s, e in ranges]
        with Pool(processes=workers) as pool:
            temp_files = pool.map(_job_collect_global, jobs)

        all_msgs: List[Dict[str, Any]] = []
        for p in temp_files:
            with open(p, "rb") as fh:
                all_msgs.extend(pickle.load(fh))
            os.remove(p)
        all_msgs.sort(key=lambda m: m.get("TimeUS", 0))
        elapsed = time.perf_counter() - t0
        return len(all_msgs), all_msgs, elapsed

    else:
        jobs = [(file_path, safe_defs, s, e, round_floats) for s, e in ranges]
        with Pool(processes=workers) as pool:
            counts = pool.map(_job_count_global, jobs)

        total = sum(counts)
        elapsed = time.perf_counter() - t0
        return total, None, elapsed


def run_tp_decode(file_path: str, fmt_defs: Dict[int, Dict[str, Any]], ranges: List[Tuple[int, int]],
                  round_floats: bool, keep_list: bool, workers: int) -> Tuple[int, Optional[List[Dict[str, Any]]], float]:
    local_defs = _ensure_structs_locally(fmt_defs)
    t0 = time.perf_counter()

    if keep_list:
        temp_files: List[str] = []
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_worker_decode_collect, file_path, local_defs, s, e, round_floats) for s, e in ranges]
            for fu in as_completed(futs):
                temp_files.append(fu.result())

        all_msgs: List[Dict[str, Any]] = []
        for p in temp_files:
            with open(p, "rb") as fh:
                all_msgs.extend(pickle.load(fh))
            os.remove(p)
        all_msgs.sort(key=lambda m: m.get("TimeUS", 0))
        elapsed = time.perf_counter() - t0
        return len(all_msgs), all_msgs, elapsed

    else:
        total = 0
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_worker_decode_count, file_path, local_defs, s, e, round_floats) for s, e in ranges]
            for fu in as_completed(futs):
                total += fu.result()
        elapsed = time.perf_counter() - t0
        return total, None, elapsed


def run_pymavlink(file_path: str, keep_list: bool) -> Tuple[int, Optional[List[Dict[str, Any]]], float, bool]:


    t0 = time.perf_counter()
    count = 0
    out: Optional[List[Dict[str, Any]]] = [] if keep_list else None



    from pymavlink import mavutil
    connection = mavutil.mavlink_connection(file_path)
    while True:
        msg = connection.recv_match(blocking=False)
        if msg is None:
            break
        d = msg.to_dict()
        d["message_type"] = msg.get_type()
        d["TimeUS"] = d.get("TimeUS", d.get("time_us", 0))
        if keep_list:
            out.append(d)
        count += 1

    elapsed = time.perf_counter() - t0
    if keep_list and out:
        out.sort(key=lambda m: m.get("TimeUS", 0))
    return count, out, elapsed, True

# ============================================================
# 🧾 Report helpers
# ============================================================

def _pct_speedup(baseline: float, value: float) -> float:
    if baseline <= 0:
        return 0.0
    return (baseline - value) / baseline * 100.0


# ============================================================
# 🚀 Main
# ============================================================

def main():
    ap = argparse.ArgumentParser(description="Benchmark MP vs ThreadPool vs pymavlink.")
    ap.add_argument("file", help="Path to .BIN log file")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--round-floats", action="store_true", default=True)
    args = ap.parse_args()

    file_path = args.file
    workers = args.workers
    round_floats = args.round_floats

    print(f"🔹 Preparing FMT & ranges (workers={workers})...")
    fmt_defs, ranges, size = _build_fmt_and_ranges(file_path, workers)
    print(f"   File size: {size:,} bytes | Ranges: {len(ranges)} parts")

    results = {}

    # Multiprocessing
    print("\n🚀 Multiprocessing: decode w/o list ...")
    cnt, _, t = run_mp_decode(file_path, fmt_defs, ranges, round_floats, keep_list=False, workers=workers)
    results["mp_no_list"] = (cnt, t)
    print(f"   → decoded={cnt:,} | time={t:.2f}s")

    print("🚀 Multiprocessing: decode WITH list ...")
    cnt, lst, t = run_mp_decode(file_path, fmt_defs, ranges, round_floats, keep_list=True, workers=workers)
    results["mp_with_list"] = (cnt, t)
    print(f"   → decoded={cnt:,} | time={t:.2f}s")

    # ThreadPool
    print("\n🧵 ThreadPool: decode w/o list ...")
    cnt, _, t = run_tp_decode(file_path, fmt_defs, ranges, round_floats, keep_list=False, workers=workers)
    results["tp_no_list"] = (cnt, t)
    print(f"   → decoded={cnt:,} | time={t:.2f}s")

    print("🧵 ThreadPool: decode WITH list ...")
    cnt, lst, t = run_tp_decode(file_path, fmt_defs, ranges, round_floats, keep_list=True, workers=workers)
    results["tp_with_list"] = (cnt, t)
    print(f"   → decoded={cnt:,} | time={t:.2f}s")

    # pymavlink
    print("\n📦 pymavlink: decode w/o list ...")
    cnt, _, t, enabled = run_pymavlink(file_path, keep_list=False)
    if not enabled:
        print("   → pymavlink not installed; skipping baseline.")
        results["mav_no_list"] = (0, 0.0)
        results["mav_with_list"] = (0, 0.0)
    else:
        results["mav_no_list"] = (cnt, t)
        print(f"   → decoded={cnt:,} | time={t:.2f}s")

        print("📦 pymavlink: decode WITH list ...")
        cnt, lst, t, _ = run_pymavlink(file_path, keep_list=True)
        results["mav_with_list"] = (cnt, t)
        print(f"   → decoded={cnt:,} | time={t:.2f}s")

    # Summary
    print("\n==================== Results ====================")
    for tag, (n, sec) in results.items():
        print(f"{tag:>15}: time={sec:8.2f}s | msgs={n:,}")

    baseline_key = "mav_with_list" if results.get("mav_with_list", (0, 0.0))[1] > 0 else "mav_no_list"
    baseline_time = results.get(baseline_key, (0, 0.0))[1]
    if baseline_time <= 0:
        print("\n⚠️ No pymavlink baseline - cannot compute % comparisons.")
        return

    print(f"\nBaseline = {baseline_key} ({baseline_time:.2f}s)")
    for tag in ["mp_with_list", "mp_no_list", "tp_with_list", "tp_no_list"]:
        if tag in results:
            _, sec = results[tag]
            pct = _pct_speedup(baseline_time, sec)
            sign = "faster" if pct > 0 else "slower"
            print(f"{tag:>15}: {abs(pct):6.2f}% {sign} than baseline")

    print("\n📎 Cost of list building (Δtime between WITH and NO list):")
    def delta(a, b): return results[a][1] - results[b][1] if a in results and b in results else None
    for prefix in ["mp", "tp", "mav"]:
        d = delta(f"{prefix}_with_list", f"{prefix}_no_list")
        if d is not None:
            print(f"  {prefix.upper()} Δlist: {d:.2f}s")


if __name__ == "__main__":
    main()
