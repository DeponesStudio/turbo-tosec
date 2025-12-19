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

from turbo_tosec.database import DatabaseManager
from turbo_tosec.parser import DatFileParser

def worker_parse_task(file_path: str) -> List[Tuple]:
    """
    Standard Mode Worker: Parses XML and returns list of tuples (InMemory).
    """
    parser = DatFileParser()
    return parser.parse(file_path)

def worker_stream_task(file_path: str, temp_dir: str) -> dict:
    """
    Streaming Mode Worker: Parses XML and writes chunks to Parquet files (OnDisk).
    Returns stats dict instead of raw data to save RAM.
    """
    # Put it in parser.py
    from turbo_tosec.parser import parse_and_save_chunks
    
    try:
        result_stats = parse_and_save_chunks(file_path, temp_dir)
        return result_stats
    except Exception as e:
        # Throw it upper stack so process pool catches it.
        raise RuntimeError(f"Error in {os.path.basename(file_path)}: {e}")
    
def get_dat_files(root_dir: str) -> List[str]:
    """
    Finds all .dat files in the specified directory and its subdirectories.
    """
    dat_files = []
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.lower().endswith(".dat"):
                dat_files.append(os.path.join(root, file))
                
    return dat_files

class ImportSession:
    """
    Orchestrates the scanning, parsing, and database insertion workflow.
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

        # Streaming Settings
        self.streaming = getattr(args, 'streaming', False)
        self.temp_dir = getattr(args, 'temp_dir', 'temp_chunks')

        if self.streaming:
            self._prepare_temp_dir()
            
    def _prepare_temp_dir(self):
        """Cleans or creates the temporary directory for Parquet chunks."""
        p = Path(self.temp_dir)
        if p.exists():
            try:
                shutil.rmtree(p)
            except Exception as e:
                print(f"Warning: Could not clean temp dir {p}: {e}")
                
        p.mkdir(parents=True, exist_ok=True)

    def run(self, files_to_process: List[str]):
        """
        Executes the import process based on selected mode.
        """
        print("Calculating total size for progress bar...")
        total_bytes = sum(os.path.getsize(f) for f in self.all_files)
        remaining_bytes = sum(os.path.getsize(f) for f in files_to_process)
        initial_bytes = total_bytes - remaining_bytes
        
        # Check CPU core count to not exceed (ProcessPool consumes resources)
        max_cpu = multiprocessing.cpu_count()
        workers = min(self.args.workers, max_cpu) if self.args.workers > 0 else max_cpu

        try:
            # Karar Anı: Streaming mi Normal mi?
            if self.streaming:
                print(f"Mode: Streaming (Low RAM, High I/O)")
                print(f"   Storage: {self.temp_dir}/")
                print(f"   Workers: {workers}")
                self._run_streaming_mode(files_to_process, workers, total_bytes, initial_bytes)
            else:
                print(f"Mode: In-Memory (Standard)")
                print(f"   Workers: {workers}")
                self._run_standard_mode(files_to_process, workers, total_bytes, initial_bytes)

        except KeyboardInterrupt:
            print("\nInterrupted.")
        except Exception as error:
            print(f"\nCritical Error: {error}")
        finally:
            # comment in for debug
             if self.streaming and os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
             pass

        return self.total_roms, self.error_count

    def _run_standard_mode(self, files, workers, total_bytes, initial_bytes):
        """
        The original logic: Parse -> List -> DB
        """
        with tqdm(total=total_bytes, initial=initial_bytes, unit='B', unit_scale=True, unit_divisor=1024) as pbar:
            self._start_monitor(pbar)

            if workers < 2:
                self._run_serial(files, pbar)
            else:
                self._run_parallel(files, workers, pbar)
            
            self._stop_monitor()
        
        self._flush_buffer() # Write any remaining data

    def _run_streaming_mode(self, files, workers, total_bytes, initial_bytes):
        """
        The new logic: Parse -> Parquet Files -> Bulk Import
        """
        with tqdm(total=total_bytes, initial=initial_bytes, unit='B', unit_scale=True, 
                  unit_divisor=1024, desc="Generating Parquet") as pbar:
            
            self._start_monitor(pbar)

            # Generate Parquet Files
            
            executor = concurrent.futures.ProcessPoolExecutor(max_workers=workers)
            
            try:
                
                # Eğer durdurma/kill özelliği için self.executor lazımsa,
                # sadece işlem süresince tutup finally bloğunda None yapacağız.
                self.executor = executor
                
                # Call Streaming worker
                future_to_file = {executor.submit(worker_stream_task, f, self.temp_dir): f for f in files}
                
                for future in concurrent.futures.as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        stats = future.result() # Return as Dict: {'roms': 500, 'size': 1024}
                        
                        # Check Skipped Files (for Legacy CMP files)
                        if stats.get("skipped"):
                            tqdm.write(f"Skipped: {stats.get('file')} ({stats.get('reason')})")
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
            print(f"\nBulk Importing generated Parquet files from {self.temp_dir}...")
            # Bu fonksiyonu database.py'ye ekleyeceğiz veya manager üzerinden çağıracağız
            # DuckDB'nin harika özelliği: read_parquet('folder/*.parquet')
            try:
                self.db.import_from_parquet_folder(self.temp_dir)
                print("Bulk Import Complete.")
            except Exception as e:
                print(f"Bulk Import Failed: {e}")
        else:
            print("\nNo ROMs found to import.")
            
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

    def _process_result(self, data, file_path, pbar):
        """
        Common logic for handling a parsed file result.
        """
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

    def _run_serial(self, files, pbar):
        
        parser = DatFileParser()
        for file_path in files:
            try:
                data = parser.parse(file_path)
                self._process_result(data, file_path, pbar)
                
            except Exception as error:
                self._handle_error(error, file_path)

    def _run_parallel(self, files, workers, pbar):
        
        # chunk_size: How many files to assign to each worker at a time?
        # If too small, communication overhead increases; if too large, load balancing suffers.
        chunk_size = max(1, len(files) // (workers * 4))
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_file = {executor.submit(worker_parse_task, f): f for f in files}
            
            for future in concurrent.futures.as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    data = future.result()
                    self._process_result(data, file_path, pbar)
                    
                except Exception as error:
                    self._handle_error(error, file_path)
                    
    def _handle_error(self, error, file_path):
        
        error_msg = str(error).lower()
        if "not enough space" in error_msg or "read-only file system" in error_msg:
             raise OSError("CRITICAL: Disk is full or not writable!") from error
             
        self.error_count += 1
        logging.error(f"Failed: {file_path} -> {error}")
    