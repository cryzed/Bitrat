import dataclasses
import os
import pathlib
import sqlite3
import typing as T

from bitrat.types import PathType


@dataclasses.dataclass
class Record:
    path: str
    digest: bytes
    mtime: float


def get_database(path: PathType) -> sqlite3.Connection:
    path = pathlib.Path(os.fspath(path))
    if path.is_file():
        return sqlite3.connect(path)

    # Initialize database
    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE records (path TEXT PRIMARY KEY, hash BLOB, mtime REAL)")
    connection.commit()
    return connection


def update_record(cursor: sqlite3.Cursor, path: str, digest: bytes, mtime: float) -> None:
    cursor.execute("INSERT OR REPLACE INTO records(path, hash, mtime) VALUES (?, ?, ?)", (path, digest, mtime))


def delete_record(cursor: sqlite3.Cursor, path: str) -> None:
    cursor.execute("DELETE FROM records WHERE path=?", (path,))


def record_exists(cursor: sqlite3.Cursor, path: str) -> bool:
    cursor.execute("SELECT EXISTS(SELECT 1 FROM records WHERE path=? LIMIT 1)", (path,))
    return cursor.fetchone()[0] == 1


def yield_records(cursor: sqlite3.Cursor) -> T.Generator[Record, None, None]:
    cursor.execute("SELECT path, hash, mtime FROM records")
    while chunk := cursor.fetchmany():
        for record in chunk:
            yield Record(*record)


def get_total_records(cursor: sqlite3.Cursor) -> int:
    cursor.execute("SELECT COUNT() FROM records")
    return cursor.fetchone()[0]
