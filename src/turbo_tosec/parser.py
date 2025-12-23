import os
from typing import Dict, List, Tuple, Optional
import re
import xml.etree.ElementTree as ET
import logging
import pyarrow as pa
import pyarrow.parquet as pq

def _detect_file_format(file_path: str) -> str:
    """
    It determines whether a file is XML or Legacy CMP by reading the file header.
    It doesn't read the entire file, only the first 1KB. It's fast.
    
    Returns: 'xml', 'cmp', or 'unknown'
    """
    try:
        # We're ignoring encoding errors because our goal is simply to read the header. 
        # Some older DAT files may contain strange characters.
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            head = f.read(1024).lower().strip()
            
            # 1. XML Control
            # Standart XML imzası veya TOSEC/MAME root tag'i var mı?
            if "<?xml" in head or "<datafile" in head or "<mame" in head:
                return 'xml'
            
            # 2. CMP (Legacy) Control
            # ClrMamePro signature or a structure in parentheses?
            if "clrmamepro" in head or "rom (" in head or "game (" in head:
                return 'cmp'
            
            return 'unknown'

    except Exception:
        # If the file is unreadable (e.g., if it's binary or if there's no authorization)
        return 'unknown'

def parse_game_info(game_name) -> Tuple[str, int]:
    """
    Extracts title and release year from the game name string
    Input: "Dragonstone (1994)(Core)(M3)(Disk 1 of 4)[cr RNX - TRD]"
    Output: ("Dragonstone", "1994")
    """
    # Safety Check: XML node might miss the 'name' attribute
    if not game_name:
        return "Unknown", None
    
    # 1. Title: Take everything up to the first '(' character)
    # If there are no parentheses, take the entire name.
    title_match = re.match(r'^(.*?)(\s*\(|$)', game_name)
    title = title_match.group(1).strip() if title_match else game_name.strip()
    
    # 2. Year: Capture the format (19xx) or (20xx)
    # Usually the first parenthesis, but look for 4 digits to be sure.
    year_match = re.search(r'\((\d{4})\)', game_name)
    release_year = int(year_match.group(1)) if year_match else None
    
    return title, release_year

def _try_parse_size(raw_value: str) -> int:
    """
    Parses a size string robustly, handling hex, units, and dirty formats.
    Returns 0 if absolutely no number can be extracted.
    
    Examples:
      "1024" -> 1024
      "1kb"  -> 1024
      "0x10" -> 16
      "10 mb" -> 10485760
      "None" -> 0
    """
    if not raw_value:
        return 0
        
    s = str(raw_value).strip().lower()
    
    # 1. Hexadecimal Check (0x...)
    if s.startswith("0x") or s.startswith("$"):
        try:
            # clear $ 
            clean_hex = s.replace("$", "")
            return int(clean_hex, 16)
        except ValueError:
            # Appears like hex but not. Ex. 0xZZZ
            raise ValueError(f"Invalid Hex format: '{raw_value}'")

    # 2. Unit Handling (KB, MB, GB)
    multiplier = 1
    if "kb" in s or "k " in s or s.endswith("k"):
        multiplier = 1024
    elif "mb" in s or "m " in s or s.endswith("m"):
        multiplier = 1024 ** 2
    elif "gb" in s or "g " in s or s.endswith("g"):
        multiplier = 1024 ** 3

    # 3. Extract digits
    match = re.search(r"(\d+)", s)
    if match:
        try:
            val = int(match.group(1))
            return val * multiplier
        except ValueError:
            raise ValueError(f"Integer conversion failed for: '{raw_value}'")
            
    # If no match.
    raise ValueError(f"Unknown/Unparsable size format: '{raw_value}'")

def _write_chunk_arrow(data: List[Dict], output_dir: str, original_filename: str, index: int, schema: pa.Schema):
    """Writes a list of dicts to Parquet using pure PyArrow."""
    if not data:
        return

    safe_name = "".join(x for x in original_filename if x.isalnum() or x in "._-")
    output_path = os.path.join(output_dir, f"{safe_name}_part_{index}.parquet")
    
    # 1. List of Dicts -> PyArrow Table
    table = pa.Table.from_pylist(data, schema=schema)
    # 2. Write to Disk
    pq.write_table(table, output_path, compression='snappy')

class DatFileParser:
    """
    Handles parsing of TOSEC DAT files in both XML and legacy CMP formats.
    """
    def __init__(self):
        
        # Compile header pattern to identify CMP files for once for performance
        self._cmp_header_pattern = re.compile(r'clrmamepro\s*\(', re.IGNORECASE)
        # Cmp parsing patterns
        self._game_pattern = re.compile(r'game\s*\(', re.IGNORECASE)
        self._name_pat = re.compile(r'name\s+"(.*?)"', re.IGNORECASE)
        self._desc_pat = re.compile(r'description\s+"(.*?)"', re.IGNORECASE)
        self._rom_pat = re.compile(r'rom\s*\(\s*(.*?)\s*\)', re.DOTALL | re.IGNORECASE)
        self._rom_name_pat = re.compile(r'name\s+"(.*?)"', re.IGNORECASE)
        self._size_pat = re.compile(r'size\s+(\d+)', re.IGNORECASE)
        self._crc_pat = re.compile(r'crc\s+([0-9a-fA-F]+)', re.IGNORECASE)
        self._md5_pat = re.compile(r'md5\s+([0-9a-fA-F]+)', re.IGNORECASE)
        self._sha1_pat = re.compile(r'sha1\s+([0-9a-fA-F]+)', re.IGNORECASE)

    def parse(self, file_path: str) -> List[Tuple]:
        """Auto-detects format and parses the file."""
        if self._is_cmp_file(file_path):
            return self._parse_cmp(file_path)
        return self._parse_xml(file_path)

    def _is_cmp_file(self, file_path: str) -> bool:
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                head = f.read(500)
                return "clrmamepro" in head.lower() or "rom (" in head.lower()
        except:
            return False

    def _get_common_info(self, file_path: str) -> Tuple[str, str, str]:
        
        dat_filename = os.path.basename(file_path)
        try:
            system_name = os.path.basename(os.path.dirname(file_path))
        except:
            system_name = "Unknown"
        
        # Category Parsing Logic
        # Format: "Commodore Amiga - Games - [ADF] (TOSEC...)"
        
        # 1. Clean extension and TOSEC tag
        clean_name = dat_filename.rsplit('.', 1)[0]
        if "(TOSEC" in clean_name:
            clean_name = clean_name.split("(TOSEC")[0].strip()
            
        parts = clean_name.split(' - ', 1)
        platform = parts[0].strip()
        category = parts[1].strip() if len(parts) > 1 else "Standard"
        
        return dat_filename, platform, category, system_name

    def _parse_xml(self, file_path: str) -> List[Tuple]:
        
        rows = []
        dat_filename, platform, category, system_name = self._get_common_info(file_path)
        
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            for game in root.findall('game'):
                game_name = game.get('name')
                title, release_year = parse_game_info(game_name)
                desc_node = game.find('description')
                description = desc_node.text if desc_node is not None else ""
                
                for rom in game.findall('rom'):
                    rows.append((
                        dat_filename, platform, category, game_name, title, release_year,
                        description, rom.get('name'), rom.get('size'), rom.get('crc'), 
                        rom.get('md5'), rom.get('sha1'), rom.get('status', 'good'), 
                        system_name
                    ))
                    
        except Exception as error:
            logging.error(f"FAILED (XML): {file_path} -> {error}")
            
        return rows

    def _parse_cmp(self, file_path: str) -> List[Tuple]:
        
        rows = []
        dat_filename, platform, category, system_name = self._get_common_info(file_path)

        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
                
        except Exception as error:
            logging.error(f"FAILED (Read CMP): {file_path} -> {error}")
            return []

        # --- CMP Parsing Logic (Bracket Counter) ---
        game_blocks = []
        iterator = self._game_pattern.finditer(content)
        
        for match in iterator:
            start_idx = match.end()
            current_idx = start_idx
            balance = 1
            
            while balance > 0 and current_idx < len(content):
                char = content[current_idx]
                if char == '(': balance += 1
                elif char == ')': balance -= 1
                current_idx += 1
                
            if balance == 0:
                game_blocks.append(content[start_idx : current_idx - 1])

        for block in game_blocks:
            g_name_match = self._name_pat.search(block)
            g_desc_match = self._desc_pat.search(block)
            
            game_name = g_name_match.group(1) if g_name_match else "Unknown"
            title, release_year = parse_game_info(game_name)
            description = g_desc_match.group(1) if g_desc_match else ""

            for rom_match in self._rom_pat.finditer(block):
                rom_data = rom_match.group(1)
                r_name = self._rom_name_pat.search(rom_data)
                
                if r_name:
                    r_size = self._size_pat.search(rom_data)
                    r_crc = self._crc_pat.search(rom_data)
                    r_md5 = self._md5_pat.search(rom_data)
                    r_sha1 = self._sha1_pat.search(rom_data)

                    rows.append((dat_filename, platform, category, game_name, 
                                 title, release_year, description,
                                r_name.group(1),
                                int(r_size.group(1)) if r_size else 0,
                                r_crc.group(1) if r_crc else "",
                                r_md5.group(1) if r_md5 else "",
                                r_sha1.group(1) if r_sha1 else "",
                                "good",
                                system_name
                    ))
        return rows

# Staging Engine => Xml -> Parquet using PyArraw
def parse_and_save_chunks(file_path: str, output_dir: str, chunk_size: int = 500000) -> Dict:
    """
    Reads XML in a staging fashion and writes directly to Parquet using PyArrow.
    No Pandas dependency = Smaller EXE size.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        
    fmt = _detect_file_format(file_path)
    if fmt != 'xml':
        raise ValueError(f"SKIPPED_LEGACY_FORMAT: Detected '{fmt}'. staging mode requires XML.")
    
    dat_filename = os.path.basename(file_path)
    try:
        system_name = os.path.basename(os.path.dirname(file_path))
    except:
        system_name = "Unknown"
    
    # Category Extraction
    clean_name = dat_filename.rsplit('.', 1)[0]
    if "(TOSEC" in clean_name:
        clean_name = clean_name.split("(TOSEC")[0].strip()
    parts = clean_name.split(' - ', 1)
    
    platform = parts[0].strip()
    category = parts[1].strip() if len(parts) > 1 else "Standard"

    buffer = []
    chunk_index = 0
    total_roms = 0
    
    # PyArrow Schema (Define manually for type-safety)
    # DuckDB recognizes types automatically.
    schema = pa.schema([('filename', pa.string()), 
                        ('platform', pa.string()),
                        ('category', pa.string()),
                        ('game_name', pa.string()),
                        ('title', pa.string()),
                        ('release_year', pa.int32()),
                        ('description', pa.string()), 
                        ('rom_name', pa.string()), 
                        ('size', pa.int64()),
                        ('crc', pa.string()), 
                        ('md5', pa.string()), 
                        ('sha1', pa.string()), 
                        ('status', pa.string()), 
                        ('system', pa.string())
    ])
    
    try:
        context = ET.iterparse(file_path, events=("end",))
        
        for event, elem in context:
            if elem.tag in ('game', 'machine'):
                game_name = elem.get('name')
                title, release_year = parse_game_info(game_name)
                desc_node = elem.find('description')
                description = desc_node.text if desc_node is not None else ""
                
                for rom in elem.findall('rom'):
                    final_size = _try_parse_size(rom.get('size'))
                    
                    row = {
                        'filename': dat_filename,
                        'platform': platform,
                        'category': category,
                        'game_name': game_name,
                        'title': title,
                        'release_year': release_year,
                        'description': description,
                        'rom_name': rom.get('name'),
                        'size': final_size,
                        'crc': rom.get('crc'),
                        'md5': rom.get('md5'),
                        'sha1': rom.get('sha1'),
                        'status': rom.get('status', 'good'),
                        'system': system_name
                    }
                    buffer.append(row)
                    total_roms += 1
                
                elem.clear()
                
                if len(buffer) >= chunk_size:
                    _write_chunk_arrow(buffer, output_dir, dat_filename, chunk_index, schema)
                    buffer = [] 
                    chunk_index += 1
        
        if buffer:
            _write_chunk_arrow(buffer, output_dir, dat_filename, chunk_index, schema)
            
        return {"roms": total_roms, "chunks": chunk_index + 1}

    except Exception as e:
        logging.error(f"Staging Error in {file_path}: {e}")
        raise e


