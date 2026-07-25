# ArduPilot BIN Log Parser

A high-performance, parallel Python parser designed for ArduPilot binary log files (`.BIN` format).  
This project bypasses traditional sequential bottlenecks by leveraging memory-mapped file I/O, mathematical length-jump parsing, and true multiprocessing execution, delivering massive performance improvements over standard alternatives like `pymavlink`.

---

## 🚀 Project Purpose

Processing large ArduPilot flight logs can be computationally expensive. This parser is built to extract and decode telemetry data at maximum speed, making it ideal for:

* High-throughput data analysis pipelines
* Large-scale batch processing of massive log files (millions of messages)
* Seamless integration into advanced flight telemetry and analysis systems

---

## ⚡ Core Architecture & Performance Highlights

1. **Memory-Mapped I/O (`mmap`)**: Eliminates disk I/O overhead by mapping the entire log file directly into system RAM for lightning-fast access.
2. **Mathematical Length-Jumps**: Bypasses full message decoding for filtered-out types by skipping directly using pre-calculated message lengths.
3. **Early-Rejection Filtering**: Evaluates message IDs and filters *before* executing expensive binary unpacking.
4. **Process Isolation (`Multiprocessing`)**: Bypasses the Python GIL by splitting files safely using validated synchronization markers and distributing work across isolated CPU worker processes.

---

## 🛠️ Code Strengths

### 1. **Parallel Processing Architecture**
* Supports both multiprocessing (`mp`) and multithreading (`tp`) execution modes.
* Automatic workload distribution across available CPU cores via safe file segmentation.
* **Anchor-based splitting (`FlightSegmentSplitter`)**: Locates valid sync markers (`\xa3\x95`) and verifies message structures to prevent boundary corruption.
* Zero data loss with robust inter-process task serialization.

### 2. **Memory Efficiency & Caching**
* Zero-copy feel via memory-mapped file access (`mmap.ACCESS_READ`).
* Streaming architecture utilizing Python generators (`Generator`) to handle multi-gigabyte logs without memory bloat.
* Pre-compiled `struct` caching for repeated unpack operations.

### 3. **Performance Optimizations**
* Configurable float rounding and optimized scaling multipliers.
* Streamlined field processing combining decoding, scaling, and rounding into a unified pass.
* Up to **86.5% faster** execution times compared to standard sequential parsers on large logs.

### 4. **Robust Error Handling & Logging**
* Centralized logging infrastructure (`log_config.py`) with automatic unhandled exception hooks.
* Graceful fallback mechanisms and warning collection for corrupted file segments.
* Strict configuration management via JSON and Box dot-notation (`config_loader.py`).

---

## 📁 Project Structure

```text
Flight_Log_Parser/
│
├── logs/                      # Runtime log output directory
├── src/
│   ├── config/
│   │   ├── config.json        # Global system and parser configurations
│   │   ├── config_loader.py   # Dot-notation config loader using Box
│   │   └── log_config.py      # Centralized logging setup & exception hooks
│   │
│   ├── parser/
│   │   └── bin_log_parser.py  # Core high-performance binary log decoder & FMT loader
│   │
│   ├── pipeline/
│   │   ├── flight_segment_splitter.py  # Binary sync-marker scanner and range divider
│   │   └── parallel_bin_decoder.py     # Orchestrator for multiprocessing/threading pools
│   │
│   └── main.py                # Main application entry point
│
├── tests/                     # Unit and integration tests (pytest)
├── .gitignore
└── README.md