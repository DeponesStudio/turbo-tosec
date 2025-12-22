# 🚀 turbo-tosec v2.0

> **High-Performance TOSEC Ingestion Engine powered by DuckDB & Apache Arrow.**

**turbo-tosec** is a next-generation data engineering tool designed to scan, parse, and convert massive **TOSEC (The Old School Emulation Center)** DAT collections into a single, instantly queryable **DuckDB** database file.

Unlike traditional XML parsers, **turbo-tosec v2.0** utilizes modern **Zero-Copy Ingestion** and **ETL Staging** techniques to process gigabytes of metadata in seconds, transforming scattered XML files into a structured SQL warehouse.

---

### 📥 Download Now (No Python Required)

If you don't want to install Python, simply download the standalone executable for your OS:

* **Windows:** [Download `turbo-tosec_v2.0.0_Windows.exe](https://www.google.com/search?q=%5Bhttps://github.com/berkacunas/turbo-tosec/releases/latest%5D(https://github.com/berkacunas/turbo-tosec/releases/latest))`
* **Linux:** [Download `turbo-tosec_v2.0.0_Linux.tar.gz](https://www.google.com/search?q=%5Bhttps://github.com/berkacunas/turbo-tosec/releases/latest%5D(https://github.com/berkacunas/turbo-tosec/releases/latest))`

---

## ⚡ Why turbo-tosec v2.0?

* **Three Ingestion Strategies:** Choose between **Direct Mode** (Speed), **Staged Mode** (Safety/Resume), or **Legacy Mode** based on your hardware.
* **Crash-Safe & Resumable:** Power outage? No problem. **Staged Mode** saves progress to disk and resumes exactly where it left off.
* **Zero Dependencies:** No need for MySQL or Postgres servers. The output is a single, portable `.duckdb` file.
* **Apache Arrow Integration:** Uses columnar memory formats for lightning-fast data transfer between Python and DuckDB.
* **Smart Recursive Scanning:** Automatically hunts down thousands of `.dat` files in nested subdirectories.

## 📦 Installation

This project requires Python 3.9+.

```bash
git clone https://github.com/berkacunas/turbo-tosec.git
cd turbo-tosec
pip install .

```

## 🛠️ Usage & Strategies

**turbo-tosec** offers different strategies to handle data ingestion. Choose the one that fits your needs:

### 1. Direct Mode (Streaming) 🏎️

**Best for:** High Speed, Good RAM, Fast SSDs.

Uses **Apache Arrow** to stream XML data directly into DuckDB without intermediate disk I/O. This is the fastest method (Zero-Copy).

```bash
turbo-tosec --input "C:\TOSEC\DATs" --direct

```

### 2. Staged Mode (Batch / ETL) 🛡️

**Best for:** Huge Datasets, Low RAM, Reliability.

Follows the **ETL (Extract, Transform, Load)** pattern. Parses XMLs into compressed temporary Parquet files (Staging) before bulk loading.

* **Resumable:** If the process is interrupted, re-running the command will skip already processed files.
* **Parallel:** Uses multiple worker processes.

```bash
# Process with 4 CPU cores
turbo-tosec --input "C:\TOSEC\DATs" --staged --workers 4

```

### 3. In-Memory Mode (Legacy) 💾

**Best for:** Very small files or debugging.

Loads the entire XML DOM into RAM. *Not recommended for large DAT files.* This mode is active by default if no flag is provided.

```bash
turbo-tosec --input "C:\TOSEC\DATs"

```

## ⚙️ CLI Arguments

| Flag | Description | Default |
| --- | --- | --- |
| `-i, --input` | Path to the root directory containing DAT files. | **Required** |
| `-o, --output` | Path for the output DuckDB database. | `tosec.duckdb` |
| `--direct` | **[New]** Enable Zero-Copy Streaming Mode (Fastest). | `False` |
| `--staged` | **[New]** Enable ETL Batch Mode (Resumable/Safe). | `False` |
| `-w, --workers` | Number of parallel processes (Staged Mode only). | `CPU Count` |
| `--temp-dir` | Directory for staging Parquet chunks (Staged Mode). | `temp_chunks` |
| `-b, --batch-size` | Batch size for insertion transactions. | `1000` |

## ⚡ Performance Benchmarks

*Tests performed on a dataset of ~3,000 DAT files (1M+ ROM entries).*

| Strategy | Speed | RAM Usage | Disk I/O |
| --- | --- | --- | --- |
| **In-Memory** | 🐢 Slow | 🔴 High | Low |
| **Staged** | 🐇 Fast | 🟢 Low | High (Temp files) |
| **Direct** | 🐆 **Fastest** | 🟢 Low | **Minimal** |

## 🔍 Example Queries (SQL)

You can open the generated `.duckdb` file using **DBeaver** or **VSCode SQLTools**.

**Find Verified [!] Commodore 64 Games:**

```sql
SELECT game_name, rom_name 
FROM roms 
WHERE platform LIKE '%Commodore 64%' 
  AND rom_name LIKE '%[!]%';

```

**Find Duplicates (Clone Checking):**

```sql
SELECT crc, COUNT(*) as count 
FROM roms 
GROUP BY crc 
HAVING count > 1 
ORDER BY count DESC;

```

## 📚 Documentation

For detailed architecture explanations and advanced usage, please refer to the **[Project Wiki](https://github.com/berkacunas/turbo-tosec/wiki)**.

## 📄 License

This project is licensed under the **GNU General Public License v3.0 (GPL-3.0)**.

---

## ❤️ Support the Project

**turbo-tosec** is developed and maintained by **Depones Labs**. If you find this tool useful, please consider making a donation to support open-source development.

<a href="[https://github.com/sponsors/berkacunas](https://github.com/sponsors/berkacunas)">
<img src="[https://img.shields.io/badge/Sponsor-GitHub-pink?style=for-the-badge&logo=github-sponsors](https://img.shields.io/badge/Sponsor-GitHub-pink?style=for-the-badge&logo=github-sponsors)" height="50" alt="Sponsor on GitHub">
</a>

<a href="[https://www.buymeacoffee.com/depones](https://www.buymeacoffee.com/depones)" target="_blank"><img src="[https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png](https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png)" alt="Buy Me A Coffee" style="height: 60px !important;width: 217px !important;" ></a>

* **Star this repo!** ⭐ It helps visibility.

---

*Disclaimer: This project does not contain TOSEC database files or ROMs. It strictly provides a tool to process the metadata files provided by the TOSEC project.*

**Copyright © 2025 berkacunas & Depones Labs.**