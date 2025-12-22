#!/usr/bin/env python3
"""
Turbo-TOSEC Entry Point
Wrapper script that launches the CLI module.
"""
import sys
import os

# src klasörünü path'e ekle ki modülleri bulabilsin
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from multiprocessing import freeze_support
from turbo_tosec.cli import main

if __name__ == "__main__":
    freeze_support()
    main()
