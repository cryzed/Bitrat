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
    has_record,
    update_record,
    yield_records,
)
from bitrat.types import PathType


def calculate_hash(path: PathType, hash_algorithm: str, chunk_size: int) -> bytes:
    path = pathlib.Path(os.fspath(path))
    hash_object = hashlib.new(hash_algorithm)

    with path.open("rb") as file:
        while chunk := file.read(chunk_size):
            hash_object.update(chunk)

    return hash_object.digest()


def run(arguments: argparse.Namespace) -> ExitCode:
    root_path = pathlib.Path(arguments.path)
    database_path = root_path / ".bitrot.db"
    database = get_database(database_path)
    database_cursor = database.cursor()
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=arguments.workers)
    database_changes = 0
    exit_code = ExitCode.Success

    if arguments.check:
        # Check against files in database
        check_futures: T.Dict[concurrent.futures.Future, Record] = {}
        total_records = get_total_records(database_cursor)
        print(f"Checking against {total_records} records from {str(database_path)!r}...")
        for record in yield_records(database_cursor):
            record_path = root_path / record.path
            if not record_path.is_file():
                print(f"\t- Removing record for {record.path!r}, no such file")
                delete_record(database_cursor, record.path)
                database_changes += 1
                continue

            future = executor.submit(calculate_hash, record_path, arguments.hash_algorithm, arguments.chunk_size)
            check_futures[future] = record

            if database_changes % arguments.save_every == 0:
                database.commit()

        future_count = len(check_futures)
        for index, future in enumerate(concurrent.futures.as_completed(check_futures), start=1):
            record = check_futures[future]
            record_path = root_path / record.path
            digest = future.result()
            modified = record_path.stat().st_mtime
            hexdigest = binascii.hexlify(digest).decode("ASCII")

            if record.modified != modified:
                print(f"\t- ({index}/{future_count}) Updating record for {record.path!r}: {hexdigest}")
                update_record(database_cursor, record.path, digest, modified)
                database_changes += 1
            elif record.digest != digest:
                record_hexdigest = binascii.hexlify(record.digest).decode("ASCII")
                record_date = datetime.fromtimestamp(record.modified)
                date = datetime.fromtimestamp(modified)
                print(f"\t- ({index}/{future_count}) Bitrot detected in {record.path!r}!")
                print(f"\t\tRecorded: {record_hexdigest!r} at {record_date}")
                print(f"\t\tCurrent:  {hexdigest!r} at {date}")
                exit_code = ExitCode.Failure

            if database_changes % arguments.save_every == 0:
                database.commit()
                database_changes += 1

            del check_futures[future]

    # Check for new files
    update_futures: T.Dict[concurrent.futures.Future, pathlib.Path] = {}
    print(f"Checking for new files in {str(root_path)!r}...")
    for path in root_path.rglob("*"):
        if path == database_path:
            continue

        if not path.is_file():
            continue

        relative_path = path.relative_to(root_path)
        if has_record(database_cursor, str(relative_path)):
            continue

        future = executor.submit(calculate_hash, path, arguments.hash_algorithm, arguments.chunk_size)
        update_futures[future] = path

    future_count = len(update_futures)
    for index, future in enumerate(concurrent.futures.as_completed(update_futures), start=1):
        path = update_futures[future]
        digest = future.result()
        relative_path = path.relative_to(root_path)
        hexdigest = binascii.hexlify(digest).decode("ASCII")
        print(f"\t- ({index}/{future_count}) Adding record for {str(relative_path)!r}: {hexdigest}")
        update_record(database_cursor, str(relative_path), digest, path.stat().st_mtime)
        database_changes += 1

        if database_changes % arguments.save_every == 0:
            database.commit()
            database_changes += 1

        del update_futures[future]

    database.commit()
    database.close()
    return exit_code


def main() -> None:
    parser = get_argument_parser()
    arguments = parser.parse_args()
    parser.exit(run(arguments))
