import os
from typing import List, Tuple, Optional
import re
import xml.etree.ElementTree as ET
import logging

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
        
        # Extract platform name from the filename
        platform = dat_filename.split(' - ')[0]
        return dat_filename, platform, system_name

    def _parse_xml(self, file_path: str) -> List[Tuple]:
        
        rows = []
        dat_filename, platform, system_name = self._get_common_info(file_path)
        
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            for game in root.findall('game'):
                game_name = game.get('name')
                desc_node = game.find('description')
                description = desc_node.text if desc_node is not None else ""
                
                for rom in game.findall('rom'):
                    rows.append((
                        dat_filename, platform, game_name, description,
                        rom.get('name'), rom.get('size'), rom.get('crc'), 
                        rom.get('md5'), rom.get('sha1'), rom.get('status', 'good'), 
                        system_name
                    ))
                    
        except Exception as error:
            logging.error(f"FAILED (XML): {file_path} -> {error}")
            
        return rows

    def _parse_cmp(self, file_path: str) -> List[Tuple]:
        
        rows = []
        dat_filename, platform, system_name = self._get_common_info(file_path)

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
            description = g_desc_match.group(1) if g_desc_match else ""

            for rom_match in self._rom_pat.finditer(block):
                rom_data = rom_match.group(1)
                r_name = self._rom_name_pat.search(rom_data)
                
                if r_name:
                    r_size = self._size_pat.search(rom_data)
                    r_crc = self._crc_pat.search(rom_data)
                    r_md5 = self._md5_pat.search(rom_data)
                    r_sha1 = self._sha1_pat.search(rom_data)

                    rows.append((dat_filename, platform, game_name, description,
                                r_name.group(1),
                                int(r_size.group(1)) if r_size else 0,
                                r_crc.group(1) if r_crc else "",
                                r_md5.group(1) if r_md5 else "",
                                r_sha1.group(1) if r_sha1 else "",
                                "good",
                                system_name
                    ))
        return rows
