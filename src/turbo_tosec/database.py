import os
import time
from typing import List, Tuple, Optional
import duckdb

class DatabaseManager:
    """
    Manages DuckDB connection, schema creation, and data insertion.
    Encapsulates all SQL logic to keep the main flow clean.
    """
    def __init__(self, db_path: str, turbo_mode: bool = False):
        
        self.db_path = db_path
        self.turbo_mode = turbo_mode
        self.conn = None
        
    def __enter__(self):
        
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        
        self.close()

    def connect(self):
        """Establishes connection and ensures schema exists."""
        self.conn = duckdb.connect(self.db_path)
        
        if self.turbo_mode:
            print("DB: Turbo Mode engaged (Low safety, High speed)")
            # synchronous=OFF: Does not wait for disk to confirm "I wrote it" (Risky but very fast).
            self.conn.execute("PRAGMA synchronous=OFF")
            self.conn.execute("PRAGMA memory_limit='75%'")
        else:
            print("DB: Safe Mode engaged (Full integrity)")
            self.conn.execute("PRAGMA synchronous=FULL")
            
        self._setup_schema()
            
    def close(self):
        """Closes the database connection safely."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def _setup_schema(self):
        """Creates tables if they don't exist."""
        # Main ROM table
        self.conn.execute("""
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
        # processed files table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                filename VARCHAR PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Metadata (for vesion control, etc.)
        self.conn.execute("CREATE TABLE IF NOT EXISTS db_metadata (key VARCHAR PRIMARY KEY, value VARCHAR)")

    def get_metadata_value(self, key: str) -> Optional[str]:
        """Fetches a value from the metadata table safely."""
        try:
            result = self.conn.execute("SELECT value FROM db_metadata WHERE key=?", (key, )).fetchone()
            return result[0] if result else None
        except:
            return None

    def set_metadata_value(self, key: str, value: str):
        """Sets or updates a metadata key."""
        # Upsert logic (delete if exists, then insert)
        self.conn.execute("DELETE FROM db_metadata WHERE key=?", (key, ))
        self.conn.execute("INSERT INTO db_metadata VALUES (?, ?)", (key, value))

    def get_processed_files(self) -> set:
        """Returns a set of filenames that have already been imported."""
        try:
            res = self.conn.execute("SELECT filename FROM processed_files").fetchall()
            return {row[0] for row in res}
        except:
            return set()

    def wipe_database(self):
        """Clears all data but keeps the file."""
        self.conn.execute("DELETE FROM roms")
        self.conn.execute("DELETE FROM processed_files")
        self.conn.execute("DELETE FROM db_metadata")

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
            conn = self.create_database(db_path) # Şemayı oluştur
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
            
    def configure_threads(self, thread_count: int):
        """Sets the PRAGMA threads for DuckDB."""
        if thread_count > 0:
            self.conn.execute(f"PRAGMA threads={thread_count}")
