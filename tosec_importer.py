import os
import time
import argparse
import xml.etree.ElementTree as ET
from typing import List, Tuple, Optional
import duckdb
from tqdm import tqdm 

def create_database(db_path: str):
    
    con = duckdb.connect(db_path)
    
    con.execute("DROP TABLE IF EXISTS roms")
    con.execute("""
        CREATE TABLE roms (
            dat_filename VARCHAR,
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
    return con

def get_dat_files(root_dir: str) -> List[str]:
    
    dat_files = []
    
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.lower().endswith(".dat"):
                dat_files.append(os.path.join(root, file))
                
    return dat_files

def parse_dat_file(file_path: str) -> List[Tuple]:
    
    rows = []
    dat_filename = os.path.basename(file_path)
    system_name = os.path.basename(os.path.dirname(file_path))

    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        for game in root.findall('game'):
            game_name = game.get('name')
            # Description may be missing sometimes, take it safe
            desc_node = game.find('description')
            description = desc_node.text if desc_node is not None else ""
            
            for rom in game.findall('rom'):
                rows.append((
                    dat_filename,
                    game_name,
                    description,
                    rom.get('name'),
                    rom.get('size'),
                    rom.get('crc'),
                    rom.get('md5'),
                    rom.get('sha1'),
                    rom.get('status', 'good'),
                    system_name
                ))
    except ET.ParseError:
        # Corrupted XML files can be logged here
        pass
    except Exception:
        pass
        
    return rows

def main():

    parser = argparse.ArgumentParser(description="Imports TOSEC DAT files into DuckDB database.")
    parser.add_argument("--input", "-i", required=True, help="The main directory path where the TOSEC DAT files are located.")
    parser.add_argument("--output", "-o", default="tosec.duckdb", help="Name/path of the DuckDB file to be created.")
    args = parser.parse_args()

    start_time = time.time()
    
    print(f"📂 Scanning directory: {args.input}...")
    all_dat_files = get_dat_files(args.input)
    print(f"📄 A total of {len(all_dat_files)} .dat files were found.")

    if not all_dat_files:
        print("❌ No .dat file found. Exiting.")
        return

    con = create_database(args.output)
    
    total_roms = 0
    print("🚀 İçe aktarma başlıyor...")
    
    with tqdm(total=len(all_dat_files), unit="file") as pbar:
        for file_path in all_dat_files:
            data = parse_dat_file(file_path)
            
            if data:
                con.executemany("""
                    INSERT INTO roms VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, data)
                total_roms += len(data)
            
            pbar.set_postfix({"Total ROMs": total_roms})
            pbar.update(1)

    con.close()
    
    end_time = time.time()
    duration = end_time - start_time
    
    print("\n✅ Transaction completed!")
    print(f"💾 Database: {args.output}")
    print(f"📊 Total ROM: {total_roms:,}") # Print with thousands separator
    print(f"⏱️ Elapsed Time: {duration:.2f} second ({duration/60:.1f} minute)")

if __name__ == "__main__":
    
    main()