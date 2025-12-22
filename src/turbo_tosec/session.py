import os
from typing import List, Tuple
import threading
import concurrent.futures
import time
import shutil
from pathlib import Path
from tqdm import tqdm
import logging
import multiprocessing
import gc
import pyarrow as pa
import xml.etree.ElementTree as ET

from turbo_tosec.database import DatabaseManager
from turbo_tosec.parser import DatFileParser
from turbo_tosec.utils import Console

def worker_parse_task(file_path: str) -> List[Tuple]:
    """
    Worker for InMemoryMode: Parses XML completely into RAM (and returns list of tuples).
    """
    parser = DatFileParser()
    return parser.parse(file_path)

def worker_staged_task(file_path: str, temp_dir: str) -> dict:
    """
    Worker for StagedMode: Parses XML and writes chunks to intermediate Parquet files.
    """
    from turbo_tosec.parser import parse_and_save_chunks
    try:
        result_stats = parse_and_save_chunks(file_path, temp_dir)
        return result_stats
    except Exception as error:
        raise RuntimeError(f"Error in {os.path.basename(file_path)}: {error}")
    
def get_dat_files(root_dir: str) -> List[str]:
    """Finds all .dat files in the specified directory and its subdirectories."""
    dat_files = []
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.lower().endswith(".dat"):
                dat_files.append(os.path.join(root, file))
                
    return dat_files

class ImportSession:
    """
    Orchestrates the ingestion workflow using one of the 3 strategies:
    1. InMemoryMode
    2. StagedMode
    3. DirectMode
    """
    def __init__(self, args, db_manager: DatabaseManager, all_files: List[str]):
        
        self.args = args
        self.db = db_manager
        self.buffer = []
        self.total_roms = 0
        self.error_count = 0
        self.all_files = all_files
        self.stop_monitor = threading.Event()
        self.executor = None # To track active executor for cleanup

        # Strategy Selection
        # ----------------------------------------------------------
        # StagedMode: Uses disk as buffer (safest for huge datasets)
        self.staged = getattr(args, 'staged', False) 
        # DirectMode: Uses RAM buffer + Zero Copy (fastest)
        self.direct = getattr(args, 'direct', False)
        # Temp dir is only relevant for StagedMode
        self.temp_dir = getattr(args, 'temp_dir', 'temp_chunks')

        if self.staged:
            self._prepare_temp_dir()
            
    def _prepare_temp_dir(self):
        """Cleans or creates the temporary staging directory for Parquet chunks."""
        p = Path(self.temp_dir)
        if p.exists():
            try:
                shutil.rmtree(p)
            except Exception as error:
                Console.warning(f"Warning: Could not clean temp dir {p}: {error}")
                
        p.mkdir(parents=True, exist_ok=True)

    def run(self, files_to_process: List[str]):
        """
        Main execution entry point.
        """
         # ASCII Banner
        Console.banner()
        
        Console.info("Calculating dataset metrics...")
        total_bytes = sum(os.path.getsize(f) for f in self.all_files)
        remaining_bytes = sum(os.path.getsize(f) for f in files_to_process)
        initial_bytes = total_bytes - remaining_bytes
        
        # Check CPU core count to not exceed (ProcessPool consumes resources)
        max_cpu = multiprocessing.cpu_count()
        workers = min(self.args.workers, max_cpu) if self.args.workers > 0 else max_cpu

        try:
            # Strategy Routing
            # ---------------------------------------------------------------------
            if self.direct:
                # Strategy 3: Direct Mode
                Console.section(f"Strategy: Direct Mode (Stream)")
                Console.info("Technique : Zero-Copy Ingestion (Apache Arrow)", indent=2)
                Console.info("Threads   : Main Process + DuckDB Internal", indent=2)
                Console.info("I/O Type  : Memory Stream", indent=2)
                self._run_direct_mode(files_to_process, total_bytes, initial_bytes)
                
            elif self.staged:
                # Strategy 2: Staged Mode
                Console.section(f"Strategy: Staged Mode (Batch)")
                Console.info("Technique : ETL (Extract -> Transform -> Load)", indent=2)
                Console.info(f"Staging   : {self.temp_dir}/ (Parquet)", indent=2)
                Console.info(f"Workers   : {workers}", indent=2)
                self._run_staged_mode(files_to_process, workers, total_bytes, initial_bytes)
                
            else:
                # Strategy 1: In-Memory Mode
                Console.section(f"Strategy: In-Memory Mode (Legacy/Standard)")
                Console.info("Technique : DOM Parsing", indent=2)
                Console.info(f"Workers   : {workers}", indent=2)
                self._run_in_memory_mode(files_to_process, workers, total_bytes, initial_bytes)

        except KeyboardInterrupt:
            Console.warning("Process interrupted by user (SIGINT).")
        except Exception as error:
            Console.error(f"System Failure: {error}")
        finally:
             if self.staged and os.path.exists(self.temp_dir):
                Console.info("Cleaning up staging area...")
                shutil.rmtree(self.temp_dir)

        return self.total_roms, self.error_count

    # Strategy 1: In-memory
    def _run_in_memory_mode(self, files, workers, total_bytes, initial_bytes):
        
        with tqdm(total=total_bytes, initial=initial_bytes, unit='B', unit_scale=True, unit_divisor=1024) as pbar:
            
            self._start_monitor(pbar)
            if workers < 2:
                self._run_serial(files, pbar)
            else:
                self._run_parallel(files, workers, pbar)
                
            self._stop_monitor()
        
        self._flush_buffer() # Write any remaining data

    # Strategy 2: Staged 
    def _run_staged_mode(self, files, workers, total_bytes, initial_bytes):
        # Parse -> Parquet Files -> Bulk Import
        with tqdm(total=total_bytes, initial=initial_bytes, unit='B', unit_scale=True, 
                  unit_divisor=1024, desc="Generating Parquet") as pbar:
            
            self._start_monitor(pbar)
            executor = concurrent.futures.ProcessPoolExecutor(max_workers=workers)
            
            try:
                self.executor = executor
                # Call Staging worker
                future_to_file = {executor.submit(worker_staged_task, f, self.temp_dir): f for f in files}
                
                for future in concurrent.futures.as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        stats = future.result() # Return as Dict: {'roms': 500, 'size': 1024}
                        
                        # Check Skipped Files (for Legacy CMP files)
                        if stats.get("skipped"):
                            tqdm.write(f"{Console.SYM_INFO} Skipped: {stats.get('file')} ({stats.get('reason')})")
                            # Push the bar amount of the size of the file so it can reach 100%.
                            try:
                                pbar.update(os.path.getsize(file_path))
                            except:
                                pbar.update(0)
                            continue
                        
                        # Update stats
                        self.total_roms += stats.get('roms', 0)
                        
                        # Update Progress-bar 
                        try:
                            file_size = os.path.getsize(file_path)
                            pbar.update(file_size)
                        except:
                            pbar.update(0)

                        pbar.set_postfix({"ROMs": self.total_roms})

                    except Exception as error:
                        self._handle_error(error, file_path)
            finally:
                self.executor = None
                executor.shutdown(wait=True)
                del executor
                gc.collect()
                self._stop_monitor()

        # Bulk Import into DUCKDB
        if self.total_roms > 0:
            Console.info(f"Bulk loading from staging area: {self.temp_dir}...")
            try:
                # DuckDB's great feature: read_parquet('folder/*.parquet')
                self.db.import_from_parquet_folder(self.temp_dir)
                Console.success("Bulk Import Complete.")
            except Exception as error:
                Console.error(f"Import Failed: {error}")
        else:
            Console.warning("No ROMs found to import.")

    # Strategy 3: Direct Mode
    def _run_direct_mode(self, files, total_bytes, initial_bytes):
        """
        Parses XML stream and injects directly into DuckDB via Arrow.
        Runs in Main Thread to utilize DuckDB's connection safely.
        """
        # Define Schema (The Blueprint)
        schema = pa.schema([
            ('filename', pa.string()), ('platform', pa.string()), ('game_name', pa.string()),
            ('description', pa.string()), ('rom_name', pa.string()), ('size', pa.int64()),
            ('crc', pa.string()), ('md5', pa.string()), ('sha1', pa.string()), 
            ('status', pa.string()), ('system', pa.string())
        ])

        chunk_size = 50000 
        
        with tqdm(total=total_bytes, initial=initial_bytes, unit='B', unit_scale=True, 
                  unit_divisor=1024, desc="Direct Ingestion") as pbar:
            
            self._start_monitor(pbar)
            
            for file_path in files:
                try:
                    # Metadata Extraction
                    dat_filename = os.path.basename(file_path)
                    try:
                        system_name = os.path.basename(os.path.dirname(file_path))
                    except:
                        system_name = "Unknown"
                        
                    platform = dat_filename.split(' - ')[0]
                    
                    buffer = []
                    
                    # XML Stream Parsing (Iterparse)
                    context = ET.iterparse(file_path, events=("end",))
                    for event, elem in context:
                        if elem.tag in ('game', 'machine'):
                            game_name = elem.get('name')
                            desc_node = elem.find('description')
                            description = desc_node.text if desc_node is not None else ""
                            
                            for rom in elem.findall('rom'):
                                # Parsing Size safely
                                try:
                                    s_val = int(rom.get('size', 0))
                                except:
                                    s_val = 0
                                    
                                row = {
                                    'filename': dat_filename,
                                    'platform': platform,
                                    'game_name': game_name,
                                    'description': description,
                                    'rom_name': rom.get('name'),
                                    'size': s_val,
                                    'crc': rom.get('crc'),
                                    'md5': rom.get('md5'),
                                    'sha1': rom.get('sha1'),
                                    'status': rom.get('status', 'good'),
                                    'system': system_name
                                }
                                buffer.append(row)
                            
                            elem.clear() # Memory Cleanup
                            
                            # Flush Buffer to DB via Arrow
                            if len(buffer) >= chunk_size:
                                arrow_table = pa.Table.from_pylist(buffer, schema=schema)
                                self.db.conn.execute("INSERT INTO roms SELECT * FROM arrow_table")
                                self.total_roms += len(buffer)
                                buffer = []
                                pbar.set_postfix({"ROMs": self.total_roms})

                    # Flush Tail
                    if buffer:
                        arrow_table = pa.Table.from_pylist(buffer, schema=schema)
                        self.db.conn.execute("INSERT INTO roms SELECT * FROM arrow_table")
                        self.total_roms += len(buffer)
                    
                    # Update Progress
                    try:
                        pbar.update(os.path.getsize(file_path))
                    except:
                        pbar.update(0)
                        
                    pbar.set_postfix({"ROMs": self.total_roms})
                    
                except Exception as e:
                    self._handle_error(e, file_path)
            
            self._stop_monitor()
 
    def _start_monitor(self, pbar):
        self.stop_monitor.clear()
        def monitor_progress():
            while not self.stop_monitor.is_set():
                time.sleep(1)
                pbar.refresh()
        
        self.monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
        self.monitor_thread.start()

    def _stop_monitor(self):
        self.stop_monitor.set()
        if hasattr(self, 'monitor_thread'):
            self.monitor_thread.join()        
    
    def _flush_buffer(self):
        if self.buffer:
            self.db.insert_batch(self.buffer)
            self.total_roms += len(self.buffer)
            self.buffer.clear()

    def _run_serial(self, files, pbar):
        
        parser = DatFileParser()
        for file_path in files:
            try:
                data = parser.parse(file_path)
                self._process_result(data, file_path, pbar)
                
            except Exception as error:
                self._handle_error(error, file_path)

    def _run_parallel(self, files, workers, pbar):
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_file = {executor.submit(worker_parse_task, f): f for f in files}
            
            for future in concurrent.futures.as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    data = future.result()
                    self._process_result(data, file_path, pbar)
                    
                except Exception as error:
                    self._handle_error(error, file_path)
                    
    def _process_result(self, data, file_path, pbar):
        
        if data:
            self.buffer.extend(data)
            if len(self.buffer) >= self.args.batch_size:
                self._flush_buffer()
        
        # Update stats
        stats = {"ROMs": self.total_roms}
        if self.error_count > 0:
            stats["Errors"] = self.error_count
        pbar.set_postfix(stats)
        
        try:
            pbar.update(os.path.getsize(file_path))
        except:
            pbar.update(0)
                
    def _handle_error(self, error, file_path):
        
        error_msg = str(error).lower()
        if "not enough space" in error_msg or "read-only file system" in error_msg:
             raise OSError("CRITICAL: Disk is full or not writable!") from error
             
        self.error_count += 1
        
        tqdm.write(f"{Console.SYM_FAIL} Failed: {os.path.basename(file_path)} (Check logs)")
        logging.error(f"Failed: {file_path} -> {error}")
    