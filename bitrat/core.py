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
        if database_changes >= arguments.save_every:
            database.commit()
            database_changes = 0

    update_cursor = database.cursor()

    def process_completed_future(future: concurrent.futures.Future[HashResult], record: Record, index: int) -> None:
        nonlocal exit_code
        nonlocal database_changes

        try:
            result = future.result()
        except FileNotFoundError:
            # File disappeared before the hash was calculated, the record will be removed during the next run.
            return
        except Exception as error:  # noqa: BLE001
            _stderr(f"\t- ({index}) Error while hashing {record.path!r}: {error}")
            return

        hexdigest = hexlify(result.hash)
        if record.modified != result.modified:
            print(f"\t- ({index}) Updating record for {record.path!r}: {hexdigest!r}")
            update_record(update_cursor, record.path, result.hash, result.modified)
            database_changes += 1
        elif record.hash != result.hash:
            modified_date = datetime.datetime.fromtimestamp(result.modified, tz=get_system_timezone())
            _stderr(f"\t- ({index}) Bitrot detected in {record.path!r}!")
            _stderr(f"\t\tRecorded hash: {record.hash_hexdigest!r} at {record.modified_date}")
            _stderr(f"\t\tCurrent hash:  {hexdigest!r} at {modified_date}")
            exit_code = ExitCode.FAILURE

    read_cursor = database.cursor()
    read_cursor.arraysize = arguments.save_every
    target_path = get_path(arguments.path)
    database_path = get_database_path(target_path)
    pending_futures: dict[concurrent.futures.Future[HashResult], Record] = {}

    print(f"Checking against {count_records(read_cursor)} records from {str(database_path)!r}...")
    last_index = 0
    for index, record in enumerate(yield_records(read_cursor)):
        last_index = index
        full_record_path = target_path / record.path
        if not full_record_path.is_file():
            print(f"\t- ({index}) Deleting record for {record.path!r}: no such file")
            delete_record(update_cursor, record.path)
            database_changes += 1
            maybe_commit()
            continue

        future = executor.submit(get_hash, full_record_path, arguments.hash_algorithm, arguments.chunk_size)
        pending_futures[future] = record

        # Keep the count of pending futures bounded to avoid blowing up memory; Use a llop and FIRST_COMPLETED so we
        # don't wait for a long-running future the whole time
        while len(pending_futures) >= arguments.save_every:
            done_futures, _ = concurrent.futures.wait(
                tuple(pending_futures), return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done_futures:
                record = pending_futures.pop(future)
                process_completed_future(future, record, index)
                maybe_commit()

    # Exhaust rest of futures
    for index, future in enumerate(concurrent.futures.as_completed(tuple(pending_futures)), start=last_index + 1):
        record = pending_futures.pop(future)
        process_completed_future(future, record, index)
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

        if database_changes > arguments.save_every == 0:
            database.commit()
            database_changes = 0

    target_path = get_path(arguments.path)

    # Don't need separate cursos for this: both queries are single-record actions
    cursor = database.cursor()

    def process_completed_future(future: concurrent.futures.Future[HashResult], path: pathlib.Path, index: int) -> None:
        nonlocal database_changes
        relative_path = str(path.relative_to(target_path))

        try:
            result = future.result()
        except FileNotFoundError:
            # File disappeared before the hash was calculated.
            return
        except Exception as error:  # noqa: BLE001
            _stderr(f"\t- ({index}) Error while hashing {relative_path!r}: {error}")
            return

        print(f"\t- ({index}) Adding record for {relative_path!r}")
        update_record(cursor, relative_path, result.hash, result.modified)
        database_changes += 1

    database_path = get_database_path(target_path)
    pending_futures: dict[concurrent.futures.Future[HashResult], pathlib.Path] = {}
    print(f"Checking for new files in {str(target_path)!r}...")

    last_index = 0
    for index, path in enumerate(target_path.rglob("*")):
        last_index = index
        if path == database_path or not path.is_file():
            continue

        relative_path = str(path.relative_to(target_path))
        try:
            if record_exists(cursor, relative_path):
                continue
        except UnicodeEncodeError as error:
            _stderr(f"\t- ({index}) Problematic filename {relative_path!r}: {error}, skipping")
            continue

        future = executor.submit(get_hash, path, arguments.hash_algorithm, arguments.chunk_size)
        pending_futures[future] = path

        # Keep the count of pending futures bounded to avoid blowing up memory; Use a llop and FIRST_COMPLETED so we
        # don't wait for a long-running future the whole time
        while len(pending_futures) >= arguments.save_every:
            done_futures, _ = concurrent.futures.wait(
                tuple(pending_futures), return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done_futures:
                path = pending_futures.pop(future)
                process_completed_future(future, path, index)
                maybe_commit()

    # Exhaust rest of futures
    for index, future in enumerate(concurrent.futures.as_completed(tuple(pending_futures)), start=last_index + 1):
        path = pending_futures.pop(future)
        process_completed_future(future, path, index)
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
