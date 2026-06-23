import dataclasses
import datetime
import pathlib
import sqlite3
from collections import abc

from .migrations import run_migrations
from .utils import get_system_timezone, hexlify


@dataclasses.dataclass(frozen=True, slots=True)
class Record:
    path: str
    hash: bytes
    modified: float
    last_checked: float | None

    @property
    def hash_hexdigest(self) -> str:
        return hexlify(self.hash)

    @property
    def modified_date(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.modified, tz=get_system_timezone())

    @property
    def last_checked_date(self) -> datetime.datetime | None:
        if self.last_checked is None:
            return None

        return datetime.datetime.fromtimestamp(self.last_checked, tz=get_system_timezone())


def get_database_path(target_path: pathlib.Path) -> pathlib.Path:
    return target_path / ".bitrot.db"


def get_database(path: pathlib.Path) -> sqlite3.Connection:
    is_new = not path.is_file()
    database = sqlite3.connect(path)
    cursor = database.cursor()

    if is_new:
        # Initialize database schema for a fresh database.
        cursor.execute("CREATE TABLE records (path TEXT PRIMARY KEY, hash BLOB, modified REAL)")
        database.commit()

    # Run any pending migrations.
    run_migrations(cursor)
    cursor.close()
    return database


def update_record(cursor: sqlite3.Cursor, path: str, hash_: bytes, modified: float) -> None:
    cursor.execute("INSERT OR REPLACE INTO records(path, hash, modified) VALUES (?, ?, ?)", (path, hash_, modified))


def delete_record(cursor: sqlite3.Cursor, path: str) -> None:
    cursor.execute("DELETE FROM records WHERE path=?", (path,))


def record_exists(cursor: sqlite3.Cursor, path: str) -> bool:
    cursor.execute("SELECT EXISTS(SELECT 1 FROM records WHERE path=? LIMIT 1)", (path,))
    return bool(cursor.fetchone()[0])


def yield_records(cursor: sqlite3.Cursor, last_check_before: float | None = None) -> abc.Generator[Record]:
    """Yield records that were last checked before the given timestamp, or never checked.

    If last_check_before is None, yields all records without filtering.
    """
    if last_check_before is None:
        cursor.execute("SELECT path, hash, modified, last_checked FROM records")
    else:
        cursor.execute(
            "SELECT path, hash, modified, last_checked FROM records WHERE last_checked IS NULL OR last_checked < ?",
            (last_check_before,),
        )

    while chunk := cursor.fetchmany():
        for record in chunk:
            yield Record(*record)


def update_last_checked(cursor: sqlite3.Cursor, path: str, timestamp: float) -> None:
    """Update the last_checked timestamp for a record."""
    cursor.execute("UPDATE records SET last_checked=? WHERE path=?", (timestamp, path))


def vacuum_database(cursor: sqlite3.Cursor) -> None:
    cursor.execute("VACUUM")
