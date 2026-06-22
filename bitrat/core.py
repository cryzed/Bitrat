import argparse
import concurrent.futures
import dataclasses
import datetime
import functools
import hashlib
import pathlib
import sqlite3
import sys

from .console import ExitCode, get_argument_parser
from .database import (
    Record,
    count_records,
    delete_record,
    get_database,
    get_database_path,
    record_exists,
    update_record,
    vacuum_database,
    yield_records,
)
from .types import PathType
from .utils import get_path, get_system_timezone, hexlify


@dataclasses.dataclass(frozen=True, slots=True)
class HashResult:
    hash: bytes
    modified: float


def get_hash(path: PathType, hash_algorithm: str, chunk_size: int) -> HashResult:
    path = get_path(path)
    hash_ = hashlib.new(hash_algorithm)

    # Get the modified time immediately before hashing, so there's a reduced possibility of the timestamp and hash
    # getting out-of-sync. A problematic scenario: the hash for a file was calculated, but the file's contents have
    # changed again in the period between the hash being calculated and the main thread getting the last modified time.
    # During the next check, the hash for the file's new content won't match, but have the same timestamp, leading to a
    # false positive bitrot detection.
    modified = path.stat().st_mtime
    with path.open("rb") as file:
        while chunk := file.read(chunk_size):
            hash_.update(chunk)

    return HashResult(hash_.digest(), modified)


_stderr = functools.partial(print, file=sys.stderr)


def check_files(
    database: sqlite3.Connection, executor: concurrent.futures.ProcessPoolExecutor, arguments: argparse.Namespace
) -> ExitCode:
    exit_code = ExitCode.SUCCESS
    database_changes = 0

    def maybe_commit() -> None:
        nonlocal database_changes
        if database_changes % arguments.save_every == 0:
            database.commit()
            database_changes = 0

    read_cursor = database.cursor()
    read_cursor.arraysize = arguments.save_every
    update_cursor = database.cursor()
    target_path = get_path(arguments.path)
    database_path = get_database_path(target_path)
    futures: dict[concurrent.futures.Future[HashResult], Record] = {}

    print(f"Checking against {count_records(read_cursor)} records from {str(database_path)!r}...")
    for record in yield_records(read_cursor):
        full_record_path = target_path / record.path
        if not full_record_path.is_file():
            print(f"\t- Deleting record for {record.path!r}: no such file")
            delete_record(update_cursor, record.path)
            database_changes += 1
            maybe_commit()
            continue

        future = executor.submit(get_hash, full_record_path, arguments.hash_algorithm, arguments.chunk_size)
        futures[future] = record
        maybe_commit()

    future_count = len(futures)
    for index, future in enumerate(concurrent.futures.as_completed(futures), start=1):
        record = futures.pop(future)

        try:
            result = future.result()
        except FileNotFoundError:
            # File disappeared before the hash was calculated, the record will be removed during the next run.
            continue
        except Exception as error:  # noqa: BLE001
            _stderr(f"\t- ({index}/{future_count}) Error while hashing {record.path!r}: {error}")
            continue

        hexdigest = hexlify(result.hash)
        if record.modified != result.modified:
            print(f"\t- ({index}/{future_count}) Updating record for {record.path!r}: {hexdigest!r}")
            update_record(update_cursor, record.path, result.hash, result.modified)
            database_changes += 1
        elif record.hash != result.hash:
            modified_date = datetime.datetime.fromtimestamp(result.modified, tz=get_system_timezone())
            _stderr(f"\t- ({index}/{future_count}) Bitrot detected in {record.path!r}!")
            _stderr(f"\t\tRecorded hash: {record.hash_hexdigest!r} at {record.modified_date}")
            _stderr(f"\t\tCurrent hash:  {hexdigest!r} at {modified_date}")
            exit_code = ExitCode.FAILURE

        maybe_commit()

    database.commit()
    read_cursor.close()
    update_cursor.close()
    return exit_code


def update_files(
    database: sqlite3.Connection, executor: concurrent.futures.ProcessPoolExecutor, arguments: argparse.Namespace
) -> ExitCode:
    exit_code = ExitCode.SUCCESS
    database_changes = 0

    def maybe_commit() -> None:
        nonlocal database_changes
        if database_changes % arguments.save_every == 0:
            database.commit()
            database_changes = 0

    target_path = get_path(arguments.path)
    cursor = database.cursor()
    database_path = get_database_path(target_path)
    futures: dict[concurrent.futures.Future[HashResult], pathlib.Path] = {}

    print(f"Checking for new files in {str(target_path)!r}...")
    for path in target_path.rglob("*"):
        if path == database_path or not path.is_file():
            continue

        relative_path = str(path.relative_to(target_path))

        try:
            if record_exists(cursor, relative_path):
                continue
        except UnicodeEncodeError as error:
            _stderr(f"\t- Problematic filename {relative_path!r}: {error}, skipping")
            continue

        future = executor.submit(get_hash, path, arguments.hash_algorithm, arguments.chunk_size)
        futures[future] = path

    future_count = len(futures)
    for index, future in enumerate(concurrent.futures.as_completed(futures), start=1):
        path = futures.pop(future)
        relative_path = str(path.relative_to(target_path))

        # Blind except: future.result() can raise any exception from the worker process.
        try:
            result = future.result()
        except FileNotFoundError:
            # File disappeared before the hash was calculated.
            continue
        except Exception as error:  # noqa: BLE001
            _stderr(f"\t- ({index}/{future_count}) Error while hashing {relative_path!r}: {error}")
            continue

        print(f"\t- ({index}/{future_count}) Adding record for {relative_path!r}")
        update_record(cursor, relative_path, result.hash, result.modified)
        database_changes += 1

        maybe_commit()

    database.commit()
    cursor.close()
    return exit_code


def run(arguments: argparse.Namespace) -> ExitCode:
    target_path = get_path(arguments.path)
    database = get_database(get_database_path(target_path))
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=arguments.workers)

    if arguments.check:
        exit_code = check_files(database, executor, arguments)
        if exit_code is not ExitCode.SUCCESS:
            return exit_code

    exit_code = update_files(database, executor, arguments)

    print("Vacuuming database...")
    vacuum_database(database.cursor())
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
