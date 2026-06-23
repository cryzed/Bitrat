# Bitrat

![Logo](logo.png)

_(Logo by [thatfuckingbird](https://github.com/thatfuckingbird))_

A tool for detecting **bitrot** (silent data corruption) in files. It works by recording cryptographic hashes of every file in a directory tree, then re-checking them on subsequent runs to detect when a file's contents have changed without the modification time changing.

Bitrat is an alternative to [Bitrot](https://github.com/ambv/bitrot), built with parallel hashing and a self-contained portable database. Originally made for a [friend](https://github.com/thatfuckingbird).

## Quick Start

```bash
# Install
uv sync

# Run against the current directory
uv run bitrat

# Run against a specific directory
uv run bitrat -p /path/to/data
```

The first run hashes every file and creates the database. Subsequent runs re-hash files to detect corruption.

## How It Works

Bitrat operates in two phases:

1. **Check phase**: Re-hashes every file tracked in the database. If the hash differs but the modification time is the same, the file has suffered bitrot. If the modification time has changed, the record is updated with the new hash. Files that no longer exist have their records removed.
2. **Update phase**: Scans the target directory for new files not yet tracked and adds them to the database.

Hashing is performed in parallel using a process pool, bounded by `--workers` (default: `min(cpu_count, 2)`).

Note that parallelism has limited benefit when all files reside on the same disk.
The bottleneck is typically disk I/O, not hashing speed, so extra workers contend for the same storage bandwidth.


## Database Design

The database is a SQLite file stored at `<target_path>/.bitrot.db`. It contains a single table:

| Column     | Type | Description                              |
|------------|------|------------------------------------------|
| `path`     | TEXT | **Relative** path from the target root   |
| `hash`     | BLOB | Raw binary hash digest                   |
| `modified` | REAL | File modification time (mtime)           |

### Path Portability

Paths are stored as **relative paths** from the target directory. This means the `.bitrot.db` file is fully portable: you can move or copy the entire directory tree (including the database) to a new location, and all records remain valid. The database does not encode any absolute paths.

For example, if you run `bitrat -p /data/photos`, a file at `/data/photos/2024/vacation/beach.jpg` is stored in the database as `2024/vacation/beach.jpg`. Moving the entire `/data/photos` tree elsewhere preserves all lookups.

The database file itself (`.bitrot.db`) is excluded from scanning, so it never hashes itself.

## CLI Reference

```
uv run bitrat [OPTIONS]
```

| Flag              | Short  | Default                  | Description                                       |
|-------------------|--------|--------------------------|---------------------------------------------------|
| `--path`          | `-p`   | `.` (current directory)  | Target directory to scan                          |
| `--hash-algorithm`| `-H`   | `sha1`                   | Hash algorithm (any from `hashlib.algorithms_available`) |
| `--workers`       | `-w`   | `min(cpu_count, 2)`      | Number of parallel hashing workers                |
| `--chunk-size`    | `-c`   | `1000000` (1 MB)         | Bytes read per chunk when hashing                 |
| `--save-every`    | `-s`   | `1000`                   | Batch size for database commits and in-flight concurrency cap        |
| `--no-check`      | `-n`   | off                      | Skip the check phase; only add/update new files   |

### Exit Codes

- `0`: All files match their recorded hashes.
- `1`: Bitrot detected in one or more files.

## Typical Usage

```bash
# First run: scan and record all files
uv run bitrat -p /mnt/backups

# Daily check (e.g., via cron)
uv run bitrat -p /mnt/backups

# Quick scan: only add new files, skip re-checking existing ones
uv run bitrat -n -p /mnt/backups

# Use SHA-256 instead of SHA-1
uv run bitrat -H sha256 -p /mnt/backups

# Tune chunk size for large files
uv run bitrat -c 5000000 -p /mnt/backups
```

## Architecture

```
bitrat/
  __init__.py     # Package version
  __main__.py     # Entry point for `python -m bitrat`
  core.py         # Main logic: check_files(), update_files(), run(), main()
  database.py     # SQLite schema, queries, Record dataclass
  console.py      # Argument parser, ExitCode enum
  types.py        # Shared type aliases (PathType)
  utils.py        # Path normalization, hexlify, timezone helpers
```

### Core Flow (`core.py`)

- `get_hash()`: Reads a file in chunks, computes the hash, and captures `mtime` immediately after to minimize timestamp/hash drift.
- `check_files()`: Iterates database records, submits hashes to a process pool, compares results, and reports mismatches.
- `update_files()`: Walks the target directory, skips files already tracked, and adds new ones.
- `run()`: Opens the database, runs both phases, vacuums the database, and closes resources.

### Database Layer (`database.py`)

- `get_database_path()`: Resolves `<target_path>/.bitrot.db`.
- `get_database()`: Opens or creates the SQLite database with the `records` table.
- `update_record()`: `INSERT OR REPLACE` for upsert semantics.
- `delete_record()` / `record_exists()`: Standard lookups.
- `yield_records()` / `count_records()`: Batched iteration over records.
- `vacuum_database()`: Compacts the database after deletions.
