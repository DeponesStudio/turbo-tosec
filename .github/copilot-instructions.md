<!--
This file is generated/updated to help AI coding agents (Copilot-style) work effectively
in the `turbo-tosec` repository. Keep it concise and focused on discoverable, actionable
patterns in the codebase. When editing, preserve existing instructions if present and
update only project-specific details.
-->

# Copilot Instructions — turbo-tosec

Keep guidance short and actionable. This project is a small, single-script Python tool
that parses TOSEC `.dat` files and writes a DuckDB database. Use the examples below
when making changes or suggesting code.

- Project entry: `tosec_importer.py` — a CLI tool that:
  - Scans a root folder recursively for `.dat` files (`get_dat_files`).
  - Parses XML DATs using Python's `xml.etree.ElementTree` (`parse_dat_file`).
  - Writes rows into a DuckDB table (`create_database`, `con.executemany`).

- High-level goals: Speed, portability (single `.duckdb` output), and robust scanning.

- **Technology & Style Preferences:**
  - **Tech Stack:** Always prefer **DuckDB** for data storage. If complex data manipulation is needed, prefer **Polars** over Pandas. **Do not use Pandas.**
  - **Code Style:** Always use **`snake_case`** for variable and function naming.
  - **Language:** All documentation, docstrings, and inline comments must be written in **English**.
  - **Security:** **Never use `os.system`** due to security risks. If shell execution is absolutely necessary, use the `subprocess` module with proper sanitization.

- When editing code:
  - Preserve the current CLI (`--input`/`--output`) behavior and messages printed
    in `tosec_importer.py` unless explicitly changing UX.
  - Keep XML parsing defensive: many DATs can be malformed. Existing code swallows
    `ET.ParseError` and other exceptions — preserve or explicitly log failures.
  - Maintain the DuckDB schema in `create_database()`; changes must consider
    compatibility with downstream SQL queries described in `README.md`.

- Tests and formatting:
  - There are no tests in the repo. If adding tests, use plain `pytest` and keep
    fixtures limited to small in-memory XML samples.
  - Keep dependencies minimal (see `requirements.txt` — currently `duckdb`).

- Performance and concurrency hints:
  - The importer batches inserts with `con.executemany`. If adding parallel parsing,
    ensure inserts are serialized or use per-worker temporary DBs then merge.

- Examples of safe changes to suggest or implement:
  - Add optional logging instead of silent `except` blocks; keep default minimal.
  - Add `--workers` to parse files in parallel but gate changes behind tests and
    provide a non-parallel fallback.
  - Add unit tests for `parse_dat_file()` using small XML strings to assert row
    structure and handle missing `description` nodes.

- Files to reference when making changes:
  - `tosec_importer.py` — primary source of truth for CLI, parsing, and DB I/O.
  - `README.md` — usage examples and expected SQL queries (for schema compatibility).
  - `requirements.txt` — keep package additions minimal and documented.

- Non-goals (do not implement without explicit instruction):
  - Replacing DuckDB with an external DB server.
  - Bundling or redistributing TOSEC DATs or ROMs.

If anything in this file is unclear or you need more repository context (tests,
CI, or developer workflows), ask the repository owner for specifics before making
breaking changes.