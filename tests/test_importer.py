import pytest
import os
from turbo_tosec.database import DatabaseManager
from turbo_tosec.parser import DatFileParser

# --- Mock Data Updated for v2.0 (14 Columns) ---
# Old: (filename, platform, game, desc, rom, size, crc, md5, sha1, status, system)
# New: (filename, platform, category, game, title, year, desc, rom, size, crc, md5, sha1, status, system)

MOCK_BUFFER = [
    (
        "Amiga.dat",        # dat_filename
        "Commodore Amiga",  # platform
        "Games",            # category (YENİ)
        "Game X (1990)",    # game_name
        "Game X",           # title (YENİ)
        1990,               # release_year (YENİ - INT)
        "Desc",             # description
        "rom.adf",          # rom_name
        500,                # size
        "crc", "md5", "sha1", # hashes
        "good",             # status
        "Amiga"             # system
    )
]

def test_database_integration(tmp_path):
    """Tests if insertion works with the new 14-column schema."""
    db_path = str(tmp_path / "test.duckdb")
    
    with DatabaseManager(db_path) as db:
        # Insert the updated mock buffer
        db.insert_batch(MOCK_BUFFER)
        
        # Verify count
        count = db.conn.execute("SELECT count(*) FROM roms").fetchone()[0]
        assert count == 1
        
        # Verify new columns
        row = db.conn.execute("SELECT title, release_year, category FROM roms").fetchone()
        assert row[0] == "Game X"
        assert row[1] == 1990
        assert row[2] == "Games"

def test_platform_parsing():
    """
    Tests the filename parsing logic.
    Mock filename: 'Commodore Amiga - Games - [ADF] (TOSEC).dat'
    Expected: Platform='Commodore Amiga', Category='Games - [ADF]'
    """
    parser = DatFileParser()
    
    # We provide a simulated file path
    # The parser attempts to extract system_name from the file path in the _get_common_info method
    fake_path = "/tmp/tosec/Commodore Amiga/Commodore Amiga - Games - [ADF] (TOSEC-v2020).dat"
    
    # Method return: (dat_filename, platform, category, system_name)
    _, platform, category, _ = parser._get_common_info(fake_path)
    
    assert platform == "Commodore Amiga"
    assert category == "Games - [ADF]"