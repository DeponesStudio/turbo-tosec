"""
Turbo-TOSEC: High-Performance TOSEC DAT Importer
================================================

This module acts as the Command Line Interface (CLI) entry point.
It parses arguments and orchestrates the ingestion process using the
selected strategy (InMemory, Staged, or Direct).

Architecture & Ingestion Strategies
-----------------------------------
1.  **InMemoryMode (Default):**
    - Loads entire XML into RAM (DOM Parsing).
    - Best for strictly validating small/medium collections.

2.  **StagedMode (--staged):**
    - XML Stream -> Parquet Files (Disk) -> Bulk Load to DB.
    - Formerly known as "Streaming".
    - Best for massive datasets, low RAM usage, and safety (checkpoints).

3.  **DirectMode (--direct):**
    - XML Stream -> Arrow Buffer (RAM) -> Zero-Copy Insert to DB.
    - The "Fastest" path. Minimal Disk I/O, High Throughput.

Usage:
    python tosec_importer.py scan -i "path/to/dats" -w 8 --direct
    python tosec_importer.py scan -i "path/to/dats" --staged
    
Author: Depones Labs
License: GPL v3
"""
import os
import re
import sys
from datetime import datetime
import time
import argparse
import logging
import subprocess
import platform
from multiprocessing import freeze_support

from turbo_tosec.database import DatabaseManager, DBConfig
from turbo_tosec.session import ImportSession
from turbo_tosec.utils import get_dat_files
from turbo_tosec._version import __version__

def setup_logging(log_file: str):
   
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(level=logging.ERROR, 
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[logging.FileHandler(log_file, mode='w', encoding='utf-8')]
    )

def open_file_with_default_app(filepath):
    """Opens a file with the OS default application."""
    try:
        if platform.system() == 'Windows':
            os.startfile(filepath)
        elif platform.system() == 'Darwin': # macOS
            subprocess.call(('open', filepath))
        else: # Linux
            subprocess.call(('xdg-open', filepath))
    except Exception as e:
        print(f"\nCould not open log file automatically: {e}")
        
def extract_tosec_version(path: str) -> str:
    # Example pattern: TOSEC-v2023-08-15
    match = re.search(r"(TOSEC-v\d{4}-\d{2}-\d{2})", path, re.IGNORECASE)
    if match:
        return match.group(1)
    return "Unknown"

def check_system_resources(workers, db_threads):
    """
    Checks system limits and warns if the configuration might cause bottlenecks.
    """
    try:
        cpu_count = os.cpu_count() or 1
        total_requested_threads = workers * db_threads
        
        print(f"System Resources: {cpu_count} CPU Cores detected.")
        
        if total_requested_threads > cpu_count:
            print(f"WARNING: You requested {total_requested_threads} concurrent threads ({workers} workers x {db_threads} db_threads).")
            print(f"Your system only has {cpu_count} cores.")
            print("    -> This may cause 'Context Switching' overhead and SLOW DOWN the process.")
            print("    -> Recommendation: Keep (workers * db_threads) <= CPU Cores.")
        else:
            print(f"Configuration looks good: {total_requested_threads} threads <= {cpu_count} cores.")
            
    except Exception as e:
        print(f"Resource check skipped: {e}")
        
def run_scan_mode(args, log_filename: str):
    """
    Orchestrates the scanning process using ImportSession.
    """
    # 1. Setting up logging
    setup_logging(log_filename)

    start_time = time.time()
    
    # 2. Scan for .dat files
    print(f"Scanning directory: {args.input}...")
    all_dat_files = get_dat_files(args.input)
    
    if not all_dat_files:
        print("No .dat files found. Exiting.")
        return

    # 3. Detect TOSEC Version
    current_version = extract_tosec_version(args.input)
    print(f"Detected Input Version: {current_version}")

    db_config = DBConfig(turbo=(args.workers > 1), memory=args.db_memory, threads=args.db_threads)
    # Database Context Manager for safe handling (auto connect/close)
    with DatabaseManager(args.output, config=db_config) as db:
        
        # Resume / Wipe Decision Logic
        resume_mode = False
        db_version = db.get_metadata_value('tosec_version')
        
        # A. Version Mismatch Check
        if db_version and db_version != current_version:
            print(f"\nWARNING: Version Mismatch! (DB: {db_version} vs Input: {current_version})")
            
            if args.force_new:
                print("--force-new detected. Wiping old database.")
                resume_mode = False
            else:
                q = input("Start FRESH and wipe old database? (Required for new version) [y/N]: ").lower()
                if q != 'y': 
                    print("Operation aborted.")
                    return
                resume_mode = False
        
        # B. Version is compatible, ask about resuming
        else:
            processed_files = db.get_processed_files()
            if processed_files:
                if args.resume:
                    resume_mode = True
                elif args.force_new:
                    resume_mode = False
                else:
                    print(f"\nFound {len(processed_files)} processed files.")
                    q = input("[R]esume or [S]tart fresh? [R/s]: ").lower()
                    resume_mode = (q != 's')
        
        files_to_process = []
        
        if not resume_mode:
            #Start from scratch
            print("Wiping database...")
            db.wipe_database()
            db.set_metadata_value('tosec_version', current_version)
            files_to_process = all_dat_files
        else:
            # Resume from last state
            print("Calculating resume list...")
            processed_set = db.get_processed_files()
            files_to_process = [f for f in all_dat_files if os.path.basename(f) not in processed_set]
            
            skipped = len(all_dat_files) - len(files_to_process)
            print(f"Resuming: {skipped} files skipped. {len(files_to_process)} remaining.")

        if not files_to_process:
            print("Nothing to do. All files processed.")
            return

        db.configure_threads(args.workers)
        
        # Start Session
        # ImportSession will look at args.staged and args.direct to decide the strategy
        #------------------------------------------------------------------------------
        session = ImportSession(args, db, all_dat_files)
        total_roms, error_count = session.run(files_to_process)

    end_time = time.time()
    duration = end_time - start_time
    
    print("\nTransaction completed!")
    print(f"Database: {args.output}")
    print(f"Total ROMs: {total_roms:,}")
    print(f"Elapsed Time: {duration:.2f}s")
    
    if error_count > 0:
        print(f"\nWARNING: {error_count} files failed.")
        if args.open_log: 
            open_file_with_default_app(log_filename)
    else:
        logging.shutdown()
        if os.path.exists(log_filename): 
            try: os.remove(log_filename)
            except: pass
        print("Clean import.")

def run_parquet_mode(args):
    """
    Handles Parquet import/export operations.
    """
    with DatabaseManager(args.db) as db:
        if args.export_file:
            db.export_to_parquet(args.export_file, args.workers)
        elif args.import_file:
            db.import_from_parquet(args.import_file, args.workers)

def main():
    
    # Backward Compatibility Hack
    # If no subcommand given, and not asking for help/version, add 'scan' as default command.
    if len(sys.argv) > 1 and sys.argv[1] not in ['scan', 'parquet', '--help', '-h', '--version', '-v', '--about']:
        sys.argv.insert(1, 'scan')    
    
    parser = argparse.ArgumentParser(description="High-performance TOSEC DAT importer using DuckDB.")
    
    # Global arguments (may apply to all commands)
        # About & Version
    parser.add_argument("--about", action="store_true", help="Show program information, philosophy, and safety defaults.")
    parser.add_argument("--version", "-v", action="version", version=f"{__version__}")

    # Sub-commands for different modes
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Scan Command (Main Mode)
    parser_scan = subparsers.add_parser("scan", help="Scan DAT files and import to DB (Default mode).")
    parser_scan.add_argument("--input", "-i", required=True, help="The main directory path where the TOSEC DAT files are located.")
    parser_scan.add_argument("--output", "-o", default="tosec.duckdb", help="Name/path of the DuckDB file to be created.")
    parser_scan.add_argument("--workers", "-w", type=int, default=1, help="Number of worker threads (Default: 1). Tip: Use 0 to auto-detect CPU count.")
    parser_scan.add_argument("--batch-size", "-b", type=int, default=1000, help="Number of rows to insert per batch transaction (Default: 1000).")
    
    # Strategy Selection
    strategy_group = parser_scan.add_mutually_exclusive_group()
    
    strategy_group.add_argument("--staged", action="store_true", help="[Strategy] Staged Mode (Batch/ETL). XML -> Parquet (Disk) -> DB. Safest for huge sets, supports checkpoints.")
    strategy_group.add_argument("--direct", action="store_true", help="[Strategy] Direct Mode (Stream). XML -> Arrow (RAM) -> DB. Zero-Copy ingestion. Fastest.")
    strategy_group.add_argument("--legacy", action="store_true", help="[Strategy] In-Memory Mode (Old). XML -> DOM (RAM). High memory usage. Deprecated.")

    parser_scan.add_argument("--temp-dir", default="temp_chunks", help="Directory for temporary chunk files (used in --staged mode).")
    
    # Flags
    parser_scan.add_argument("--resume", action="store_true", help="Automatically resume if database exists.")
    parser_scan.add_argument("--force-new", action="store_true", help="Force overwrite existing database.")
    parser_scan.add_argument("--no-open-log", action="store_false", dest="open_log", default=True, help="Do NOT automatically open the log file if errors occur.")
 
    # Parquet Command (Import/Export)
    parser_parquet = subparsers.add_parser("parquet", help="Import/Export data using Parquet files.")
    parser_parquet.add_argument("--db", "-d", default="tosec.duckdb", help="Target/Source DuckDB file.")
    parser_parquet.add_argument("--workers", "-w", type=int, default=1, help="Max threads for DuckDB engine (Default: 1).")
    group = parser_parquet.add_mutually_exclusive_group(required=True)
    group.add_argument("--import-file", "-i", help="Import FROM this Parquet file.")
    group.add_argument("--export-file", "-o", help="Export TO this Parquet file.")
    
    # Advanced Performance Tuning
    parser.add_argument("--db-memory", type=str, default="75%", help="DuckDB memory limit (e.g., '8GB', '75%%'). Default: 75%% of RAM.")
    parser.add_argument("--db-threads", type=int, default=1, help="DuckDB threads per worker process. Default: 1 (Best for bulk insert).")
    
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
        
    args = parser.parse_args()
        
    if args.about:
        print(f"""
 *** turbo-tosec v{__version__} ***
 A high-performance, DuckDB-based importer for TOSEC DAT collections.

  INGESTION STRATEGIES (NEW v1.3)
 ------------------------------------
 1. InMemory (Standard):
    - Default mode. Reads XML into Python objects. 
    - Good for small sets or validation.

 2. Staged (--staged):
    - Replaces old "Streaming" mode.
    - Writes Parquet checkpoints to disk before loading.
    - Robust, resumable, uses less RAM.

 3. Direct (--direct):
    - The "Speedster". Reads XML stream directly into Arrow Tables.
    - Zero-Copy transfer to DuckDB. 
    - Minimal Disk I/O.
 
 USAGE EXAMPLES
 ---------------------------
 $ turbo-tosec scan -i "./dats" --direct
 $ turbo-tosec scan -i "./dats" --staged -w 4
 $ turbo-tosec parquet -o "backup.parquet"

 ---------------------------
 Author:  Depones Labs
 License: GPL v3 (Open Source)
 GitHub:  https://github.com/deponeslabs/turbo-tosec
""")
        return
    
    log_filename = None
    
    try:
        if args.command == "parquet":
            run_parquet_mode(args)
            
        elif args.command == "scan":
            if args.workers > 4:
                print(f"  WARNING: Using {args.workers} threads.")
                print("   If you are using a mechanical HDD, performance may drop due to seek time.")
                print("   Recommended for HDD: 1-4 threads. Recommended for SSD: 4-16 threads.")
            if not args.input:
                parser.error("the following arguments are required: --input/-i")
                
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            os.makedirs("logs", exist_ok=True)
            log_filename = os.path.join("logs", f"tosec_import_log_{timestamp}.log")
            
            run_scan_mode(args, log_filename)
            
    except KeyboardInterrupt:
        print("\n  Process interrupted by user.")
        return
    
    except Exception as error:
        logging.critical(f"FATAL ERROR: {str(error)}", exc_info=True)
        error_msg = str(error)
        
        print(f"\n  Critical Error: {error}")
        print("=" * 60)
        
        if isinstance(error, OSError) and "Disk is full" in error_msg:
            print(f"Error: Disk Storage Full")
            if args.staged:
                print("Tip: --staged mode uses disk space for temp files. Check --temp-dir drive.")
            
        elif isinstance(error, (RuntimeError, MemoryError)):
            print(f"Error: System Resources Exhausted")
            print(f"Details: {error}")
            if not args.staged:
                print(f"Tip: System ran out of RAM. Try using '--staged' mode to offload to disk.")
            else:
                print(f"Tip: Try reducing --workers (current: {args.workers}).")
        
        else:
            print(f"Error: {error}")
            print("Tip: Check the log file for technical details.")
        
        print("-" * 60)
        logging.shutdown()
            
        if args.open_log:
            print(f"Opening log file: {log_filename}")
            try:
                open_file_with_default_app(log_filename)
            except Exception as open_err:
                print(f"Could not open log file automatically: {open_err}")
        else:
            print(f"Log file saved to: {os.path.abspath(log_filename)}")
            
        sys.exit(1)
        
if __name__ == "__main__":
    
    freeze_support() # Missing freeze_support() causes "arguments required" loop
    main()
