import argparse
import concurrent.futures
import functools
import hashlib
import pathlib
import sqlite3
import sys
import typing as T
from concurrent.futures.process import ProcessPoolExecutor
from datetime import datetime

from bitrat.console import ExitCode, get_argument_parser
from bitrat.database import (
    Record,
    count_records,
    delete_record,
    get_database,
    get_database_path,
    record_exists,
    update_record,
    yield_records,
)
from bitrat.types import PathType
from bitrat.utils import ensure_pathlib_path, hexlify


def get_hash(path: PathType, hash_algorithm: str, chunk_size: int) -> bytes:
    path = ensure_pathlib_path(path)
    hash_ = hashlib.new(hash_algorithm)

    with path.open("rb") as file:
        while chunk := file.read(chunk_size):
            hash_.update(chunk)

    return hash_.digest()


stderr = functools.partial(print, file=sys.stderr)


def check_files(database: sqlite3.Connection, executor: ProcessPoolExecutor, arguments: argparse.Namespace) -> ExitCode:
    exit_code = ExitCode.Success
    database_changes = 0

    def maybe_commit() -> None:
        nonlocal database_changes
        if database_changes % arguments.save_every == 0:
            database.commit()
            database_changes = 0

    cursor = database.cursor()
    cursor.arraysize = arguments.save_every
    target_path = ensure_pathlib_path(arguments.path)
    database_path = get_database_path(target_path)
    check_futures: T.Dict[concurrent.futures.Future, Record] = {}

    print(f"Checking against {count_records(cursor)} records from {str(database_path)!r}...")
    for record in yield_records(cursor):
        full_record_path = target_path / record.path
        if not full_record_path.is_file():
            print(f"\t- Deleting record for {record.path!r}: no such file")
            delete_record(cursor, record.path)
            database_changes += 1
            continue

        future = executor.submit(get_hash, full_record_path, arguments.hash_algorithm, arguments.chunk_size)
        check_futures[future] = record
        maybe_commit()

    future_count = len(check_futures)
    for index, future in enumerate(concurrent.futures.as_completed(check_futures), start=1):
        record = check_futures.pop(future)

        # pylint: disable=broad-except
        try:
            hash_ = future.result()
        except Exception as error:
            print(f"\t- ({index}/{future_count}) Error while hashing {record.path!r}: {error}")
            continue

        hexdigest = hexlify(hash_)
        full_record_path = target_path / record.path
        modified = full_record_path.stat().st_mtime
        if record.modified != modified:
            print(f"\t- ({index}/{future_count}) Updating record for {record.path!r}: {hexdigest!r}")
            update_record(cursor, record.path, hash_, modified)
            database_changes += 1
        elif record.hash != hash_:
            modified_date = datetime.fromtimestamp(modified)
            stderr(f"\t- ({index}/{future_count}) Bitrot detected in {record.path!r}!")
            stderr(f"\t\tRecorded hash: {record.hash_hexdigest!r} at {record.modified_date}")
            stderr(f"\t\tCurrent hash:  {hexdigest!r} at {modified_date}")
            exit_code = ExitCode.Failure

        maybe_commit()

    cursor.close()
    return exit_code


def update_files(
    database: sqlite3.Connection, executor: ProcessPoolExecutor, arguments: argparse.Namespace
) -> ExitCode:
    exit_code = ExitCode.Success
    database_changes = 0

    def maybe_commit() -> None:
        nonlocal database_changes
        if database_changes % arguments.save_every == 0:
            database.commit()
            database_changes = 0

    target_path = ensure_pathlib_path(arguments.path)
    cursor = database.cursor()
    database_path = get_database_path(target_path)
    update_futures: T.Dict[concurrent.futures.Future, pathlib.Path] = {}

    print(f"Checking for new files in {str(target_path)!r}...")
    for path in target_path.rglob("*"):
        if path == database_path or not path.is_file():
            continue

        relative_path = path.relative_to(target_path)
        if record_exists(cursor, str(relative_path)):
            continue

        future = executor.submit(get_hash, path, arguments.hash_algorithm, arguments.chunk_size)
        update_futures[future] = path

    future_count = len(update_futures)
    for index, future in enumerate(concurrent.futures.as_completed(update_futures), start=1):
        path = update_futures.pop(future)
        relative_path = path.relative_to(target_path)

        # pylint: disable=broad-except
        try:
            hash_ = future.result()
        except Exception as error:
            print(f"\t- ({index}/{future_count}) Error while hashing {str(relative_path)!r}: {error}")
            continue

        print(f"\t- ({index}/{future_count}) Adding record for {str(relative_path)!r}")
        update_record(cursor, str(relative_path), hash_, path.stat().st_mtime)
        database_changes += 1

        maybe_commit()

    cursor.close()
    return exit_code


def run(arguments: argparse.Namespace) -> ExitCode:
    target_path = ensure_pathlib_path(arguments.path)
    database = get_database(get_database_path(target_path))
    executor = ProcessPoolExecutor(max_workers=arguments.workers)

    if arguments.check:
        exit_code = check_files(database, executor, arguments)
        if exit_code is not ExitCode.Success:
            return exit_code

    exit_code = update_files(database, executor, arguments)
    database.commit()
    database.close()
    executor.shutdown()
    return exit_code


def main() -> None:
    parser = get_argument_parser()
    arguments = parser.parse_args()

    try:
        parser.exit(run(arguments))
    except KeyboardInterrupt:
        print("Aborted.", file=sys.stderr)
