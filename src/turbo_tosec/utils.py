import os
from typing import List

import os
import sys
import shutil

class Console:
    """
    Old-school ASCII styling for CLI output.
    Design Philosophy: "No Emojis. Just Data."
    """
    # ANSI Colors
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    
    # Symbols
    SYM_INFO = "[*]"
    SYM_PLUS = "[+]"
    SYM_WARN = "[!]"
    SYM_FAIL = "[X]"
    SYM_TIME = "[T]"

    @staticmethod
    def banner():
        """Prints the system banner."""
        # Width calculation for centering
        cols, _ = shutil.get_terminal_size((80, 20))
        
        logo = f"""{Console.OKBLUE}{Console.BOLD}
______            __               ______
  /_  __/_  ______  / /_  ____       /_  __/___  ________  _____
   / / / / / / __ \/ __ \/ __ \_______/ / / __ \/ ___/ _ \/ ___/
  / / / /_/ / / / / /_/ / /_/ /_____// / / /_/ (__  )  __/ /__
 /_/  \__,_/_/ /_/_.___/\____/      /_/  \____/____/\___/\___/
        {Console.ENDC}"""
        print(logo)
        print(f"{Console.OKCYAN}{':: HIGH-PERFORMANCE DATA INGESTION ENGINE ::'.center(cols)}{Console.ENDC}")
        print(f"{Console.OKCYAN}{':: DEPONES LABS ::'.center(cols)}{Console.ENDC}\n")

    @staticmethod
    def section(title: str):
        """Draws a section header line."""
        cols, _ = shutil.get_terminal_size((80, 20))
        print(f"\n{Console.BOLD}{Console.HEADER}{title.upper()}{Console.ENDC}")
        print(f"{Console.HEADER}{'=' * cols}{Console.ENDC}")

    @staticmethod
    def info(msg: str, indent: int = 0):
        """Standard info log."""
        prefix = " " * indent + Console.SYM_INFO
        print(f"{Console.OKBLUE}{prefix} {msg}{Console.ENDC}")

    @staticmethod
    def success(msg: str, indent: int = 0):
        """Success operation log."""
        prefix = " " * indent + Console.SYM_PLUS
        print(f"{Console.OKGREEN}{prefix} {msg}{Console.ENDC}")

    @staticmethod
    def warning(msg: str):
        """Warning log."""
        print(f"{Console.WARNING}{Console.SYM_WARN} {msg}{Console.ENDC}")

    @staticmethod
    def error(msg: str):
        """Critical error log."""
        print(f"{Console.FAIL}{Console.SYM_FAIL} {msg}{Console.ENDC}")
        
    @staticmethod
    def perf(msg: str):
        """Performance metrics log."""
        print(f"{Console.OKCYAN}{Console.SYM_TIME} {msg}{Console.ENDC}")

def get_dat_files(root_dir: str) -> List[str]:
    
    dat_files = []
    
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.lower().endswith(".dat"):
                dat_files.append(os.path.join(root, file))
    return dat_files

def clean_path(path: str) -> str:
    """Normalizes paths for display (removes distinct drive letters if needed)."""
    return os.path.normpath(path)