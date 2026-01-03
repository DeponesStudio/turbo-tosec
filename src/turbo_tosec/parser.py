import os
from typing import Dict, List, Tuple, Iterator, Optional
import re
import xml.etree.ElementTree as ET
import logging
import pyarrow as pa
import pyarrow.parquet as pq

# GLOBAL CONSTANTS (Module Level)
# Compile patterns ONCE at import time.
# Worker processes will inherit these without re-compiling.

# Compile header pattern to identify CMP files for once for performance
CMP_HEADER_PATTERN = re.compile(r'clrmamepro\s*\(', re.IGNORECASE)

# Cmp parsing patterns
GAME_PATTERN = re.compile(r'game\s*\(', re.IGNORECASE)
NAME_PAT = re.compile(r'name\s+"(.*?)"', re.IGNORECASE)
DESC_PAT = re.compile(r'description\s+"(.*?)"', re.IGNORECASE)
ROM_PAT = re.compile(r'rom\s*\(\s*(.*?)\s*\)', re.DOTALL | re.IGNORECASE)
ROM_NAME_PAT = re.compile(r'name\s+"(.*?)"', re.IGNORECASE)
SIZE_PAT = re.compile(r'size\s+(\d+)', re.IGNORECASE)
CRC_PAT = re.compile(r'crc\s+([0-9a-fA-F]+)', re.IGNORECASE)
MD5_PAT = re.compile(r'md5\s+([0-9a-fA-F]+)', re.IGNORECASE)
SHA1_PAT = re.compile(r'sha1\s+([0-9a-fA-F]+)', re.IGNORECASE)

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

def _get_common_info(file_path: str) -> Tuple[str, str, str]:
    
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

    except Exception as error:
        logging.error(f"Staging Error in {file_path}: {error}")
        raise error

class InMemoryParser:
    """
    Handles parsing of TOSEC DAT files in both XML and legacy CMP formats.
    """
    def __init__(self):
        pass

    def parse(self, file_path: str) -> List[Tuple]:
        """Auto-detects format and parses the file."""
        fmt = _detect_file_format(file_path)
        if fmt == 'cmp':
            return self._parse_cmp(file_path)
        elif fmt == 'xml':
            return self._parse_xml(file_path)

    def _parse_xml(self, file_path: str) -> List[Tuple]:
        
        rows = []
        dat_filename, platform, category, system_name = _get_common_info(file_path)
        
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
        dat_filename, platform, category, system_name = _get_common_info(file_path)

        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
                
        except Exception as error:
            logging.error(f"FAILED (Read CMP): {file_path} -> {error}")
            return []

        # --- CMP Parsing Logic (Bracket Counter) ---
        game_blocks = []
        iterator = GAME_PATTERN.finditer(content)
        
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
            g_name_match = NAME_PAT.search(block)
            g_desc_match = DESC_PAT.search(block)
            
            game_name = g_name_match.group(1) if g_name_match else "Unknown"
            title, release_year = parse_game_info(game_name)
            description = g_desc_match.group(1) if g_desc_match else ""

            for rom_match in ROM_PAT.finditer(block):
                rom_data = rom_match.group(1)
                r_name = ROM_NAME_PAT.search(rom_data)
                
                if r_name:
                    r_size = SIZE_PAT.search(rom_data)
                    r_crc = CRC_PAT.search(rom_data)
                    r_md5 = MD5_PAT.search(rom_data)
                    r_sha1 = SHA1_PAT.search(rom_data)

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

class TurboParser:
    """
    Handles parsing of TOSEC DAT files in both XML and legacy CMP formats.
    """
    ARROW_SCHEMA = pa.schema([
        ('filename', pa.string()), ('platform', pa.string()), ('category', pa.string()),
        ('game_name', pa.string()), ('title', pa.string()), ('release_year', pa.int32()),
        ('description', pa.string()), ('rom_name', pa.string()), ('size', pa.int64()),
        ('crc', pa.string()), ('md5', pa.string()), ('sha1', pa.string()), 
        ('status', pa.string()), ('system', pa.string())
    ])
    
    def __init__(self):
        pass
    
    def _parse_xml(self, file_path: str) -> Iterator[Tuple]:
            """
            It  performs XML parsing (extraction).
            It uses memory-safe stream processing (iterparse).
            """
            # Metadata Çıkarımı (Diğer yazılımcının mantığını koruduk)
            dat_filename = os.path.basename(file_path)
            try:
                system_name = os.path.basename(os.path.dirname(file_path))
            except:
                system_name = "Unknown"
            
            clean_name = dat_filename.rsplit('.', 1)[0]
            if "(TOSEC" in clean_name:
                clean_name = clean_name.split("(TOSEC")[0].strip()
            parts = clean_name.split(' - ', 1)
            
            platform = parts[0].strip()
            category = parts[1].strip() if len(parts) > 1 else "Standard"

            # XML Stream İşlemi
            try:
                context = ET.iterparse(file_path, events=("end",))
                
                for event, elem in context:
                    if elem.tag in ('game', 'machine'):
                        game_name = elem.get('name')
                        # Parse game info fonksiyonunun var olduğunu varsayıyoruz
                        title, release_year = parse_game_info(game_name) 
                        
                        desc_node = elem.find('description')
                        description = desc_node.text if desc_node is not None else ""
                        
                        for rom in elem.findall('rom'):
                            # Boyut parse etme güvenliği
                            final_size = _try_parse_size(rom.get('size'))
                            
                            yield (
                                dat_filename,
                                platform,
                                category,
                                game_name,
                                title,
                                release_year,
                                description,
                                rom.get('name'),
                                final_size,
                                rom.get('crc'),
                                rom.get('md5'),
                                rom.get('sha1'),
                                rom.get('status', 'good'),
                                system_name
                            )
                        
                        # Clear RAM
                        elem.clear()
                        
            except Exception as error:
                logging.error(f"Failed (XML Stream): {file_path} -> {error}")
                raise error

    def _parse_cmp(self, file_path: str) -> Iterator[Tuple]:
        """
        Parses Legacy ClrMamePro (CMP) format efficiently using a generator.
        Instead of loading the whole file, it processes line-by-line.
        """
        dat_filename, platform, category, system_name = _get_common_info(file_path)

        current_game_info = {}
        in_game_block = False

        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line = line.strip()
                    if not line: 
                        continue

                    game_blocks = []

                    # Start of block
                    if line.startswith("game (") or line.startswith("resource ("):
                        in_game_block = True
                        current_game_info = {'name': 'Unknown', 'description': ''}
                        continue
                    
                    # End of block
                    if line == ")":
                        in_game_block = False
                        continue

                    if in_game_block:
                        # 1. Parse game data
                        if line.startswith('name "'):
                            current_game_info['name'] = line.split('"')[1]
                        elif line.startswith('description "'):
                            current_game_info['description'] = line.split('"')[1]

                        # 2. Parse ROM line and yield
                        if line.startswith("rom ("):
                            r_name_match = ROM_NAME_PAT.search(line)
                            if not r_name_match: 
                                continue

                            r_name = r_name_match.group(1)
                            
                            # This line is good for regex.
                            r_size = SIZE_PAT.search(line)
                            r_crc = CRC_PAT.search(line)
                            r_md5 = MD5_PAT.search(line)
                            r_sha1 = SHA1_PAT.search(line)
                            
                            # Parse Game Details
                            title, release_year = parse_game_info(current_game_info['name'])

                            yield (
                                dat_filename, 
                                platform, 
                                category, 
                                current_game_info['name'],
                                title, 
                                release_year, 
                                current_game_info['description'],
                                r_name,
                                int(r_size.group(1)) if r_size else 0,
                                r_crc.group(1) if r_crc else "",
                                r_md5.group(1) if r_md5 else "",
                                r_sha1.group(1) if r_sha1 else "",
                                "good",
                                system_name
                            )

        except Exception as error:
            logging.error(f"Failed (Read CMP): {file_path} -> {error}")
            raise error

    def parse(self, file_path: str) -> Iterator[Tuple]:
        """
        Polyglot Parser: Detects the dat format and streams the data from the correct parser.
        """
        fmt = _detect_file_format(file_path)
        
        if fmt == 'xml':
            yield from self._parse_xml(file_path)
        elif fmt == 'cmp':
            yield from self._parse_cmp(file_path)
        else:
            logging.warning(f"Skipped (Unknown Format): {file_path}")
            # Unknown format; it doesn't throw an error.
            return
    
    def parse_to_arrow_stream(self, file_path: str, chunk_size: int = 50000) -> Iterator[pa.Table]:
        """
        Consumes the self.parse() generator, buffers the tuples, and yields 
        ready-to-insert PyArrow Tables. 
        
        This is used by Session (Direct Mode) and GUI (Preview).
        """
        buffer = []
        
        try:
            record_iterator = self.parse(file_path)
            
            for record in record_iterator:
                # Convert Tuple -> Dict (for PyArrow Table)
                row = {
                    'filename': record[0], 'platform': record[1], 'category': record[2],
                    'game_name': record[3], 'title': record[4], 'release_year': record[5],
                    'description': record[6], 'rom_name': record[7], 'size': record[8],
                    'crc': record[9], 'md5': record[10], 'sha1': record[11],
                    'status': record[12], 'system': record[13]
                }
                buffer.append(row)
                
                # If buffer is full, create an Arrow Table and yield it
                if len(buffer) >= chunk_size:
                    yield pa.Table.from_pylist(buffer, schema=self.ARROW_SCHEMA)
                    buffer = []
            
            # Yield the remainings in buffer
            if buffer:
                yield pa.Table.from_pylist(buffer, schema=self.ARROW_SCHEMA)
                
        except Exception as error:
            logging.error(f"Arrow Stream Error in {file_path}: {error}")
            raise error
        
    def parse_and_save_chunks(self, file_path: str, output_dir: str, chunk_size: int = 500000) -> Dict:
        """
        It retrieves data from the data source (generator), buffers it, and writes it as Parquet. 
        It doesn't know whether the data is XML or CMP.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            
        dat_filename = os.path.basename(file_path)
        buffer = []
        chunk_index = 0
        total_roms = 0
        
        try:
            # 1. Request Data from Source (Pull Model)
            # self.parse will select and run the correct parser.
            iterator = self.parse(file_path)
            
            for record in iterator:
                # Convert the incoming Tuple to Dictionary (for PyArrow)
                row = {
                    'filename': record[0], 'platform': record[1], 'category': record[2],
                    'game_name': record[3], 'title': record[4], 'release_year': record[5],
                    'description': record[6], 'rom_name': record[7], 'size': record[8],
                    'crc': record[9], 'md5': record[10], 'sha1': record[11],
                    'status': record[12], 'system': record[13]
                }
                
                buffer.append(row)
                total_roms += 1
                
                # 2. Write and empty the buffer if full.
                if len(buffer) >= chunk_size:
                    _write_chunk_arrow(buffer, output_dir, dat_filename, chunk_index, self.ARROW_SCHEMA)
                    buffer = [] 
                    chunk_index += 1
            
            # 3. Write the last chunk
            if buffer:
                _write_chunk_arrow(buffer, output_dir, dat_filename, chunk_index, self.ARROW_SCHEMA)
                
            return {"roms": total_roms, "chunks": chunk_index + 1}

        except Exception as e:
            logging.error(f"Staging Error in {file_path}: {e}")
            raise e
