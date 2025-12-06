import os
from turbo_tosec.tosec_importer import _is_cmp_file, parse_cmp_dat_file

SAMPLE_CMP_CONTENT = """clrmamepro (
    name "Commodore 64 - Games"
    description "Commodore 64 - Games (TOSEC-v2012)"
    version 2012
)

game (
    name "Test Game (1986)"
    description "Test Game Description"
    rom ( name "test.zip" size 100 crc 12345678 md5 abcdef123456 sha1 1234567890abcdef )
)

game (
    name "Another Game"
    description "Just another game"
    rom ( name "game2.rom" size 200 crc AABBCCDD md5 11223344 sha1 55667788 )
)
"""

def test_is_cmp_file_detection(tmp_path):
    """
    tests if _is_cmp_file correctly identifies a valid CMP file.
    """
    # 1. Create a valid CMP file
    cmp_file = tmp_path / "test.dat"
    cmp_file.write_text(SAMPLE_CMP_CONTENT, encoding="utf-8")
    
    # 2. Create an invalid (XML) file
    xml_file = tmp_path / "test.xml"
    xml_file.write_text("<?xml version='1.0'?><datafile>...</datafile>", encoding="utf-8")

    # 3. Test
    assert _is_cmp_file(str(cmp_file)) is True
    assert _is_cmp_file(str(xml_file)) is False

def test_cmp_parsing_logic(tmp_path):
    """
    parse_cmp_dat_file fonksiyonunun veriyi doğru çekip çekmediğini test eder.
    """
    # 1. Prepare the file
    dat_file = tmp_path / "games.dat"
    dat_file.write_text(SAMPLE_CMP_CONTENT, encoding="utf-8")
    
    # 2. Call the function
    results = parse_cmp_dat_file(str(dat_file))
    
    # 3 .Verify the results
    # Expected: 2 games (rows) should be returned
    assert len(results) == 2
    
    # Check the first game's data
    # Tuple elements: (filename, platform, game, desc, rom, size, crc, md5, sha1, status, system)
    game1 = results[0]
    assert game1[2] == "Test Game (1986)"      # Game Name
    assert game1[3] == "Test Game Description" # Description
    assert game1[4] == "test.zip"              # ROM Name
    assert game1[5] == 100                     # Size (Integer olmalı)
    assert game1[6] == "12345678"              # CRC

    # Check the second game's data
    game2 = results[1]
    assert game2[2] == "Another Game"
    assert game2[4] == "game2.rom"
    assert game2[5] == 200
