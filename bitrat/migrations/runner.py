"""Migration runner that discovers and applies pending migrations."""

import importlib
import logging
import sqlite3
import time
import typing as T

_logger = logging.getLogger(__name__)


# Explicit migration registry. Add new migrations here in order.
_MIGRATION_MODULES = ("migration_001_create_migrations", "migration_002_add_last_checked")


@T.runtime_checkable
class _MigrationModule(T.Protocol):
    """Protocol that every migration module must implement."""

    MIGRATION_ID: str

    def apply(self, cursor: sqlite3.Cursor) -> None: ...


def _table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
    """Check whether a table exists in the database."""
    cursor.execute("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)", (table_name,))
    return bool(cursor.fetchone()[0])


def _is_applied(cursor: sqlite3.Cursor, migration_id: str) -> bool:
    """Check whether a migration has already been applied."""
    cursor.execute("SELECT EXISTS(SELECT 1 FROM migrations WHERE id=? LIMIT 1)", (migration_id,))
    return bool(cursor.fetchone()[0])


def _mark_applied(cursor: sqlite3.Cursor, migration_id: str) -> None:
    """Record that a migration has been applied."""
    cursor.execute("INSERT INTO migrations(id, applied_at) VALUES (?, ?)", (migration_id, time.time()))


def run_migrations(cursor: sqlite3.Cursor) -> None:
    """Run all pending migrations against the database.

    Args:
        cursor: A cursor connected to the target database.

    Raises:
        RuntimeError: If a migration fails to apply.
    """
    for module_name in _MIGRATION_MODULES:
        # Import lazily to avoid circular imports with __init__.py.
        module = importlib.import_module(f".{module_name}", package="bitrat.migrations")

        if not isinstance(module, _MigrationModule):
            raise TypeError(f"{module_name!r} does not implement migration protocol")

        migration_id = module.MIGRATION_ID
        # The migrations table may not exist yet (the first migration creates it).
        if _table_exists(cursor, "migrations") and _is_applied(cursor, migration_id):
            continue

        _logger.info("Applying migration %s", migration_id)
        try:
            module.apply(cursor)
            _mark_applied(cursor, migration_id)
            cursor.connection.commit()
        except Exception as error:
            _logger.error("Migration %s failed: %s", migration_id, error)
            cursor.connection.rollback()
            raise RuntimeError(f"Migration {migration_id!r} failed: {error}") from error
