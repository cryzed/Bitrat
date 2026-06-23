"""Create the migrations tracking table."""

import sqlite3

MIGRATION_ID = "001_create_migrations"


def apply(cursor: sqlite3.Cursor) -> None:
    """Create the migrations table and register this migration."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS migrations (
            id TEXT PRIMARY KEY,
            applied_at REAL NOT NULL
        )
        """
    )
