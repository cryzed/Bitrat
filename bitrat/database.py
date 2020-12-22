import dataclasses
import pathlib
import sqlite3
import typing as T
from datetime import datetime

from bitrat.types import PathType
from bitrat.utils import ensure_pathlib_path, hexlify


@dataclasses.dataclass
class Record:
    path: str
    hash: bytes
    modified: float

    @property
    def hash_hexdigest(self) -> str:
        return hexlify(self.hash)

    @property
    def modified_date(self) -> datetime:
        return datetime.fromtimestamp(self.modified)


def get_database_path(target_path: PathType) -> pathlib.Path:
    return ensure_pathlib_path(target_path) / ".bitrot.db"


def get_database(path: PathType) -> sqlite3.Connection:
    path = ensure_pathlib_path(path)
    if path.is_file():
        return sqlite3.connect(path)

    # Initialize database
    database = sqlite3.connect(path)
    cursor = database.cursor()
    cursor.execute("CREATE TABLE records (path TEXT PRIMARY KEY, hash BLOB, modified REAL)")
    database.commit()
    return database


def update_record(cursor: sqlite3.Cursor, path: str, hash_: bytes, modified: float) -> None:
    cursor.execute("INSERT OR REPLACE INTO records(path, hash, modified) VALUES (?, ?, ?)", (path, hash_, modified))


def delete_record(cursor: sqlite3.Cursor, path: str) -> None:
    cursor.execute("DELETE FROM records WHERE path=?", (path,))


def record_exists(cursor: sqlite3.Cursor, path: str) -> bool:
    cursor.execute("SELECT EXISTS(SELECT 1 FROM records WHERE path=? LIMIT 1)", (path,))
    return bool(cursor.fetchone()[0])


def yield_records(cursor: sqlite3.Cursor) -> T.Generator[Record, None, None]:
    cursor.execute("SELECT path, hash, modified FROM records")
    while chunk := cursor.fetchmany():
        for record in chunk:
            yield Record(*record)


def count_records(cursor: sqlite3.Cursor) -> int:
    cursor.execute("SELECT COUNT() FROM records")
    return cursor.fetchone()[0]
