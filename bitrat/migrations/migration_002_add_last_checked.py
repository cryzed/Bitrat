"""Add last_checked column to the records table for resume support."""

import sqlite3

MIGRATION_ID = "002_add_last_checked"


def apply(cursor: sqlite3.Cursor) -> None:
    """Add the last_checked column to the records table.

    Stores a Unix timestamp of when a file was last integrity-checked.
    Allows runs to skip files checked within a configurable window.
    """
    # Check if the column already exists (idempotent, as SQLite cannot rollback DDL).
    cursor.execute("PRAGMA table_info(records)")
    columns: set[str] = {row[1] for row in cursor.fetchall()}
    if "last_checked" not in columns:
        cursor.execute("ALTER TABLE records ADD COLUMN last_checked REAL")
