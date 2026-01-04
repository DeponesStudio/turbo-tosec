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
from turbo_tosec.parser import InMemoryParser, TurboParser, parse_game_info
from turbo_tosec.utils import Console

def worker_parse_task(file_path: str) -> List[Tuple]:
    """
    Worker for InMemoryMode: Parses XML completely into RAM (and returns list of tuples).
    """
    parser = InMemoryParser()
    return parser.parse(file_path)

def worker_staged_task(file_path: str, temp_dir: str) -> dict:
    """
    Worker for StagedMode: Parses XML and writes chunks to intermediate Parquet files.
    """
    try:
        parser = TurboParser()
        result_stats = parser.parse_and_save_chunks(file_path, temp_dir)
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
    Orchestrates the ingestion workflow.
    Can be used via CLI (passing args) or as a Library (passing explicit params).
    Ingests with one of the 3 strategies:
    1. InMemoryMode
    2. StagedMode
    3. DirectMode
    """
    def __init__(self, db_manager: DatabaseManager, args=None,  # Optional for CLI
                 workers: int = 0, temp_dir: str = "temp_chunks", batch_size: int = 1000):
        
        self.args = args
        self.db = db_manager
        self.buffer = []
        self.total_roms = 0
        self.error_count = 0
        self.stop_monitor = threading.Event()
        self.executor = None # To track active executor for cleanup

        # Strategy Selection
        # ----------------------------------------------------------
        # StagedMode: Uses disk as buffer (safest for huge datasets)
        self.staged = getattr(args, 'staged', False) 
        # DirectMode: Uses RAM buffer + Zero Copy (fastest)
        self.direct = getattr(args, 'direct', False)
        # LegacyMode
        self.legacy = getattr(args, 'legacy', False)
        
        # If CLI arguments are provided, use them; otherwise, use manual parameters
        if args:
            self.workers = getattr(args, 'workers', 0)
            self.temp_dir = getattr(args, 'temp_dir', temp_dir)
            self.batch_size = getattr(args, 'batch_size', batch_size)
        else:
            self.workers = workers
            self.temp_dir = temp_dir    # Temp dir is only relevant for Staged Mode
            self.batch_size = batch_size
        
        # CPU core limit check
        max_cpu = multiprocessing.cpu_count()
        if self.workers <= 0 or self.workers > max_cpu:
            self.workers = max_cpu
            
    # *************************************************************************
    # LIBRARY API
    # *************************************************************************
    def ingest(self, files: List[str], mode: str = 'staged', progress_callback = None) -> dict:
        """
        High-level entry point for Library/GUI usage.
        
        Args:
            files: List of file paths to process.
            mode: 'direct' (Recommended), 'staged' (Big Data), 'legacy' (Memory).
            show_progress: If False, disables tqdm (useful for silent workers).
            progress_callback: A function(current, total) to handle GUI updates.
        
        Returns:
            dict: {'total_roms': int, 'errors': int}
        """
        # Return immediately if the file list is empty
        if not files:
            logging.warning("No files provided for ingestion.")
            return {'total_roms': 0, 'errors': 0}

        # Reset statistics (A new task is starting)
        self.total_roms = 0
        self.error_count = 0
        
        # Metrics
        total_bytes = sum(os.path.getsize(f) for f in files)
        
        # Preparing for Staged mode
        if mode == 'staged':
            self._prepare_temp_dir()

        # GUI entegrasyonunda buradaki tqdm'i override etmek gerekebilir
        # ama şimdilik console output varsayıyoruz.
        
        try:
            if mode == 'direct':
                # Direct mode genellikle GUI için en iyisidir (Hızlı geri bildirim)
                self._run_direct_mode(files, total_bytes, 0, progress_callback)
            
            elif mode == 'staged':
                self._run_staged_mode(files, self.workers, total_bytes, 0, progress_callback)
                
            elif mode == 'legacy':
                self._run_in_memory_mode(files, self.workers, total_bytes, 0, progress_callback)
            
            else:
                raise ValueError(f"Unknown ingestion mode: {mode}")
                
        finally:
            # Clean-up
            if mode == 'staged' and os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
        
        return {'total_roms': self.total_roms, 'errors': self.error_count}
    
    # *************************************************************************
    # CLI API
    # *************************************************************************
    def run(self, files_to_process: List[str]):
        """
        Main execution entry point.
        """
        mode = 'legacy'
        if self.args:
            if getattr(self.args, 'direct', False): 
                mode = 'direct'
            elif getattr(self.args, 'staged', False): 
                mode = 'staged'
            
         # ASCII Banner
        Console.banner()
        stats = self.ingest(files_to_process, mode=mode)
        
        return stats['total_roms'], stats['errors']

    # Strategy 1: In-memory
    def _run_in_memory_mode(self, files, workers, total_bytes, initial_bytes, progress_callback=None):
        
        with UniversalProgress(total=total_bytes, initial=initial_bytes, 
                               desc="Direct Ingestion", callback=progress_callback) as pbar:
            
            self._start_monitor(pbar)
            if workers < 2:
                self._run_serial(files, pbar)
            else:
                self._run_parallel(files, workers, pbar)
                
            self._stop_monitor()
        
        self._flush_buffer() # Write any remaining data

    # Strategy 2: Staged 
    def _run_staged_mode(self, files, workers, total_bytes, initial_bytes, progress_callback=None):
        # Parse -> Parquet Files -> Bulk Import
        with UniversalProgress(total=total_bytes, initial=initial_bytes, 
                               desc="Direct Ingestion", callback=progress_callback) as pbar:
            
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
                self.executor.shutdown(wait=True)
                self.executor = None
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
    def _run_direct_mode(self, files, total_bytes, initial_bytes, progress_callback=None):
        """
        Parses XML stream and injects directly into DuckDB via Arrow.
        Runs in Main Thread to utilize DuckDB's connection safely.
        """
        parser = TurboParser()
            
        with UniversalProgress(total=total_bytes, initial=initial_bytes, 
                               desc="Direct Ingestion", callback=progress_callback) as pbar:
            
            for file_path in files:
                try:
                    arrow_stream = parser.parse_to_arrow_stream(file_path, chunk_size=50000)
                    for arrow_batch in arrow_stream:
                        # 1. DuckDB'ye Hızlı Kayıt (Zero-Copy sayılır)
                        # 'arrow_batch' değişkeni SQL sorgusu içinde doğrudan kullanılır.
                        self.db.conn.execute("INSERT INTO roms SELECT * FROM arrow_batch")
                        
                        # 2. İstatistikleri Güncelle
                        rows_in_batch = arrow_batch.num_rows
                        self.total_roms += rows_in_batch
                        pbar.set_postfix({"ROMs": self.total_roms})

                    # 3. Progress Bar (Dosya boyutu kadar ilerlet)
                    try:
                        pbar.update(os.path.getsize(file_path))
                    except:
                        pbar.update(0)
                        
                except Exception as error:
                    self._handle_error(error, file_path)
            
    def _start_monitor(self, pbar):
        self.stop_monitor.clear()
        def monitor_progress():
            while not self.stop_monitor.is_set():
                time.sleep(1)
                if hasattr(pbar, 'console_bar') and pbar.console_bar:
                    pbar.console_bar.refresh()
        
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
        
        parser = InMemoryParser()
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

    def _prepare_temp_dir(self):
        """Cleans or creates the temporary staging directory for Parquet chunks."""
        p = Path(self.temp_dir)
        if p.exists():
            try:
                shutil.rmtree(p)
            except Exception as error:
                Console.warning(f"Warning: Could not clean temp dir {p}: {error}")
                
        p.mkdir(parents=True, exist_ok=True)

    def _handle_error(self, error, file_path):
        
        error_msg = str(error).lower()
        if "not enough space" in error_msg or "read-only file system" in error_msg:
             raise OSError("CRITICAL: Disk is full or not writable!") from error
             
        self.error_count += 1
        
        tqdm.write(f"{Console.SYM_FAIL} Failed: {os.path.basename(file_path)} (Check logs)")
        logging.error(f"Failed: {file_path} -> {error}")
    
class UniversalProgress:
    """
    A wrapper that abstracts progress reporting.
    If a 'callback' is provided (GUI mode), it invokes the callback.
    If no callback is provided (CLI mode), it uses 'tqdm' for console output.
    """
    def __init__(self, total: int, initial: int = 0, desc: str = "", unit: str = 'B', callback=None):
        
        self.callback = callback
        self.total = total
        self.current = initial
        self.console_bar = None
        
        if not self.callback:
            # CLI Mode: Initialize tqdm
            self.console_bar = tqdm(total=total, initial=initial, unit=unit, unit_scale=True, unit_divisor=1024, desc=desc)

    def update(self, n: int):
        
        self.current += n
        if self.console_bar:
            self.console_bar.update(n)
        elif self.callback:
            # GUI Mode: Send (current, total)
            # The GUI will take these values ​​and set the progress bar.
            self.callback(self.current, self.total)

    def set_postfix(self, stats: dict):
        
        if self.console_bar:
            self.console_bar.set_postfix(stats)
        elif self.callback:
            # Optional: This can be expanded if the GUI callback accepts a third parameter (stats). 
            # # For now, only percentages are sent to the GUI.
            pass

    def close(self):
        
        if self.console_bar:
            self.console_bar.close()

    def __enter__(self): 
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb): 
        
        self.close()