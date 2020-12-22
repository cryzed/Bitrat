import dataclasses
import sqlite3
import typing as T
from datetime import datetime

from bitrat.types import PathType
from bitrat.utils import ensure_pathlib_path, hexlify


@dataclasses.dataclass
class Record:
    path: str
    digest: bytes
    modified: float

    @property
    def hexdigest(self) -> str:
        return hexlify(self.digest)

    @property
    def modified_date(self) -> datetime:
        return datetime.fromtimestamp(self.modified)


def get_database(path: PathType) -> sqlite3.Connection:
    path = ensure_pathlib_path(path)
    if path.is_file():
        return sqlite3.connect(path)

    # Initialize database
    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE records (path TEXT PRIMARY KEY, hash BLOB, modified REAL)")
    connection.commit()
    return connection


def update_record(cursor: sqlite3.Cursor, path: str, digest: bytes, modified: float) -> None:
    cursor.execute("INSERT OR REPLACE INTO records(path, hash, modified) VALUES (?, ?, ?)", (path, digest, modified))


def delete_record(cursor: sqlite3.Cursor, path: str) -> None:
    cursor.execute("DELETE FROM records WHERE path=?", (path,))


def has_record(cursor: sqlite3.Cursor, path: str) -> bool:
    cursor.execute("SELECT EXISTS(SELECT 1 FROM records WHERE path=? LIMIT 1)", (path,))
    return bool(cursor.fetchone()[0])


def yield_records(cursor: sqlite3.Cursor) -> T.Generator[Record, None, None]:
    cursor.execute("SELECT path, hash, modified FROM records")
    while chunk := cursor.fetchmany():
        for record in chunk:
            yield Record(*record)


def get_total_records(cursor: sqlite3.Cursor) -> int:
    cursor.execute("SELECT COUNT() FROM records")
    return cursor.fetchone()[0]
