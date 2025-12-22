import os
import time
import platform
import ctypes
from typing import List, Tuple, Optional
import duckdb
from typing import NamedTuple

class DBConfig(NamedTuple):
    memory: str = "4GB"
    threads: int = 1
    turbo: bool = False
    
class DatabaseManager:
    """
    Manages DuckDB connection, schema creation, and data insertion.
    Encapsulates all SQL logic to keep the main flow clean.
    """
    def __init__(self, db_path: str, config: DBConfig = None):
        
        self.db_path = db_path
        # If config is None use default config
        self.config = config or DBConfig()
        self.conn = None
        
    def __enter__(self):
        
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        
        self.close()

    def connect(self):
        """Establishes connection and ensures schema exists."""
        self.conn = duckdb.connect(self.db_path)
        
        if self.config.turbo:
            print(f"DB: Turbo Mode engaged (Low safety, High speed) | Mem: {self.config.memory} | Threads: {self.config.threads}")
            
            # Memory Configuration
            final_mem = self.config.memory
            if "%" in final_mem or final_mem == "auto":
                final_mem = self._get_optimal_ram_limit(final_mem)
                
            self.conn.execute(f"PRAGMA memory_limit='{final_mem}'")
            self.conn.execute(f"PRAGMA threads={self.config.threads}")
            
        else:
            print("DB Config: Safe Mode engaged (Full integrity)")
            
        self._setup_schema()
    
    def close(self):
        """Closes the database connection safely."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def _setup_schema(self, target_conn=None):
        """Creates tables. Can work on the main connection or a provided temporary one."""
        connection = target_conn or self.conn
        if not connection:
            return

        # Main ROM table
        connection.execute("""
            CREATE TABLE IF NOT EXISTS roms (
                dat_filename VARCHAR,
                platform VARCHAR,
                game_name VARCHAR,
                description VARCHAR,
                rom_name VARCHAR,
                size BIGINT,
                crc VARCHAR,
                md5 VARCHAR,
                sha1 VARCHAR,
                status VARCHAR,
                system VARCHAR
            )
        """)
        # Processed files table
        connection.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                filename VARCHAR PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Metadata
        connection.execute("CREATE TABLE IF NOT EXISTS db_metadata (key VARCHAR PRIMARY KEY, value VARCHAR)")
    
    def get_metadata_value(self, key: str) -> Optional[str]:
        """Fetches a value from the metadata table safely."""
        try:
            result = self.conn.execute("SELECT value FROM db_metadata WHERE key=?", (key, )).fetchone()
            return result[0] if result else None
        except:
            return None

    def set_metadata_value(self, key: str, value: str):
        """Sets or updates a metadata key."""
        self.conn.execute("INSERT OR REPLACE INTO db_metadata VALUES (?, ?)", (key, value))

    def get_processed_files(self) -> set:
        """Returns a set of filenames that have already been imported."""
        try:
            res = self.conn.execute("SELECT filename FROM processed_files").fetchall()
            return {row[0] for row in res}
        except:
            return set()

    def wipe_database(self):
        """Clears all data but keeps the file structure."""
        try:
            self.conn.execute("DELETE FROM roms")
            self.conn.execute("DELETE FROM processed_files")
            self.conn.execute("DELETE FROM db_metadata")
            self.conn.execute("VACUUM")
        except Exception as error:
            print(f"Error wiping database: {error}")

    def insert_batch(self, buffer: List[Tuple]):
        """Inserts a batch of ROMs and marks their files as processed."""
        if not buffer:
            return
            
        # Insert ROM data
        self.conn.executemany("INSERT INTO roms VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", buffer)
        # Mark files as processed
        unique_files = {row[0] for row in buffer}
        for filename in unique_files:
            self.conn.execute("INSERT OR IGNORE INTO processed_files (filename) VALUES (?)", (filename, ))
            
    def export_to_parquet(self, db_path: str, parquet_path: str, threads: int = 1):
    
        if not os.path.exists(db_path):
            print(f"  Database not found: {db_path}")
            return

        print(f"  Exporting database to Parquet: {parquet_path} (Threads: {threads})...")
        start = time.time()
        
        try:
            conn = duckdb.connect(db_path)
            conn.execute(f"PRAGMA threads={threads}")
            conn.execute(f"COPY roms TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')")
            conn.close()
            print(f"  Export completed in {time.time() - start:.2f}s")
            
        except Exception as error:
            print(f"  Export failed: {error}")

    def import_from_parquet(self, db_path: str, parquet_path: str, threads: int = 1):
        
        if not os.path.exists(parquet_path):
            print(f"  Parquet file not found: {parquet_path}")
            return

        print(f"  Importing Parquet into database: {db_path} (Threads: {threads})...")
        start = time.time()
        
        try:
            conn = duckdb.connect(db_path) 
            self._setup_schema(target_conn=conn)
            conn.execute(f"PRAGMA threads={threads}")   
            # Read Parquet and insert into table
            conn.execute(f"INSERT INTO roms SELECT * FROM read_parquet('{parquet_path}')")
            
            # Statistics
            count = conn.execute("SELECT count(*) FROM roms").fetchone()[0]
            conn.close()
            
            print(f"  Import completed in {time.time() - start:.2f}s")
            print(f"  Total Rows in DB: {count:,}")
            
        except Exception as error:
            print(f"  Import failed: {error}")
    
    def import_from_parquet_folder(self, folder_path: str):
        """
        Bulk imports all .parquet files from a directory into the main table.
        Uses DuckDB's 'read_parquet' with wildcard support for maximum speed.
        """
        if not os.path.exists(folder_path):
             raise FileNotFoundError(f"Parquet folder not found: {folder_path}")

        # Check the folder for the existence of the part files.
        # DuckDB may throw an error or perform an empty operation if there is an empty folder
        if not any(f.endswith(".parquet") for f in os.listdir(folder_path)):
            print("No .parquet files found in temp folder to import.")
            return

        print(f"DuckDB: Bulk importing chunks from {folder_path}/*.parquet ...")
        
        # Windows fix: When sending paths within SQL, it's always safer to use a '/'.
        safe_path = folder_path.replace('\\', '/')
        
        try:
            # DuckDB's glob (*) capability ensures it to retrieve thousands of files 
            # with a single SQL command without looping.
            query = f"INSERT INTO roms SELECT * FROM read_parquet('{safe_path}/*.parquet', union_by_name=True);"
            self.conn.execute(query)
            
            print("Bulk Import Success.")
            
        except Exception as error:
            print(f"Bulk Import Error: {error}")
            raise error
    
    def configure_threads(self, thread_count: int):
        """Sets the PRAGMA threads for DuckDB."""
        if thread_count > 0:
            self.conn.execute(f"PRAGMA threads={thread_count}")

    def _get_optimal_ram_limit(self, limit_str):
        """Calculates 75% of total RAM in GB, working on both Windows and Linux."""
        if limit_str == "auto":
            limit_str = "75%"

        # Eğer zaten "16GB" gibi bir değer geldiyse doğrudan döndür
        if "%" not in limit_str:
            return limit_str

        try:
            percent = int(limit_str.replace("%", "")) / 100.0
            total_ram_bytes = 0
            
            # for Windows
            if platform.system() == "Windows":
                kernel32 = ctypes.windll.kernel32
                c_ulonglong = ctypes.c_ulonglong
                
                class MEMORYSTATUSEX(ctypes.Structure):
                    _fields_ = [
                        ('dwLength', ctypes.c_ulong),
                        ('dwMemoryLoad', ctypes.c_ulong),
                        ('ullTotalPhys', c_ulonglong),
                        ('ullAvailPhys', c_ulonglong),
                        ('ullTotalPageFile', c_ulonglong),
                        ('ullAvailPageFile', c_ulonglong),
                        ('ullTotalVirtual', c_ulonglong),
                        ('ullAvailVirtual', c_ulonglong),
                        ('ullAvailExtendedVirtual', c_ulonglong),
                    ]
                
                mem_status = MEMORYSTATUSEX()
                mem_status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
                kernel32.GlobalMemoryStatusEx(ctypes.byref(mem_status))
                total_ram_bytes = mem_status.ullTotalPhys

            # for Linux / Mac 
            else:
                if "SC_PAGE_SIZE" in os.sysconf_names and "SC_PHYS_PAGES" in os.sysconf_names:
                    total_ram_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
            
            if total_ram_bytes <= 0:
                return "4GB"

            limit_gb = int((total_ram_bytes * 0.75) / (1024**3))
            limit_gb = max(1, limit_gb)
            
            return f"{limit_gb}GB"

        except Exception as error:
            print(f"RAM detection failed ({error}), defaulting to 2GB.")
            return "2GB"
            
    def get_appender(self, table_name):
        return self.conn.cursor().appender(table_name)
