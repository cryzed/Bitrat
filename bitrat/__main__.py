import argparse
import binascii
import concurrent.futures
import enum
import functools
import hashlib
import os
import pathlib
import sqlite3
import typing as T
from datetime import datetime

from bitrat.console import ExitCode, get_argument_parser
from bitrat.database import (
    Record,
    delete_record,
    get_database,
    get_total_records,
    record_exists,
    update_record,
    yield_records,
)
from bitrat.types import PathType


def calculate_hash(path: PathType, hash_algorithm: str, chunk_size: int) -> bytes:
    path = pathlib.Path(os.fspath(path))
    hash_ = hashlib.new(hash_algorithm)

    with path.open("rb") as file:
        while chunk := file.read(chunk_size):
            hash_.update(chunk)

    return hash_.digest()


def run(arguments: argparse.Namespace) -> ExitCode:
    root_path = pathlib.Path(arguments.path)
    database_path = root_path / ".bitrot.db"
    database = get_database(database_path)
    database_cursor = database.cursor()
    pool = concurrent.futures.ProcessPoolExecutor(max_workers=arguments.workers)
    exit_code = ExitCode.Success

    # Check against files in database
    check_futures: T.Dict[concurrent.futures.Future, Record] = {}
    total_records = get_total_records(database_cursor)
    print(f"- Checking against {total_records} records from {str(database_path)!r}...")

    for record in yield_records(database_cursor):
        record_path = root_path / record.path
        if not record_path.is_file():
            print(f"\t- Removing record for {record.path!r}, no such file")
            delete_record(database_cursor, record.path)
            continue

        future = pool.submit(calculate_hash, record_path, arguments.hash_algorithm, arguments.chunk_size)
        check_futures[future] = record

    future_count = len(check_futures)
    for index, future in enumerate(concurrent.futures.as_completed(check_futures), start=1):
        record = check_futures[future]
        record_path = root_path / record.path
        hash_ = future.result()
        mtime = record_path.stat().st_mtime
        hexdigest = binascii.hexlify(hash_).decode("ASCII")

        if record.mtime != mtime:
            print(f"\t- ({index}/{future_count}) Updating record for {record.path!r}: {hexdigest}")
            update_record(database_cursor, record.path, hash_, mtime)
        elif record.hash_ != hash_:
            record_hexdigest = binascii.hexlify(record.hash_).decode("ASCII")
            record_date = datetime.fromtimestamp(record.mtime)
            date = datetime.fromtimestamp(mtime)
            print(f"\t- ({index}/{future_count}) Bitrot detected in {record.path!r}!")
            print(f"\t\tRecorded: {record_hexdigest!r} at {record_date}")
            print(f"\t\tCurrent:  {hexdigest!r} at {date}")
            exit_code = ExitCode.Failure

        del check_futures[future]

    # Check for new files
    update_futures: T.Dict[concurrent.futures.Future, pathlib.Path] = {}
    print(f"- Checking for new files in {str(root_path)!r}...")
    for path in root_path.rglob("*"):
        if path == database_path:
            continue

        if not path.is_file():
            continue

        relative_path = path.relative_to(root_path)
        if record_exists(database_cursor, str(relative_path)):
            continue

        future = pool.submit(calculate_hash, path, arguments.hash_algorithm, arguments.chunk_size)
        update_futures[future] = path

    future_count = len(update_futures)
    for index, future in enumerate(concurrent.futures.as_completed(update_futures), start=1):
        path = update_futures[future]
        relative_path = path.relative_to(root_path)
        hash_ = future.result()
        hexdigest = binascii.hexlify(hash_).decode("ASCII")
        print(f"\t- ({index}/{future_count}) Adding record for {str(relative_path)!r}: {hexdigest}")
        update_record(database_cursor, str(relative_path), hash_, path.stat().st_mtime)
        del update_futures[future]

    database.commit()
    database.close()
    return exit_code


def main() -> None:
    parser = get_argument_parser()
    arguments = parser.parse_args()
    parser.exit(run(arguments))


if __name__ == "__main__":
    main()
