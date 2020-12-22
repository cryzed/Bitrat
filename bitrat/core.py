import argparse
import concurrent.futures
import hashlib
import pathlib
import sys
import typing as T
from datetime import datetime

from bitrat.console import ExitCode, get_argument_parser
from bitrat.database import (
    Record,
    delete_record,
    get_database,
    count_records,
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


def run(arguments: argparse.Namespace) -> ExitCode:
    root_path = pathlib.Path(arguments.path)
    database_path = root_path / ".bitrot.db"
    database = get_database(database_path)
    database_cursor = database.cursor()
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=arguments.workers)
    database_changes = 0
    exit_code = ExitCode.Success

    def maybe_commit() -> None:
        nonlocal database_changes
        if database_changes % arguments.save_every == 0:
            database.commit()
            database_changes = 0

    if arguments.check:
        # Check against files in database
        check_futures: T.Dict[concurrent.futures.Future, Record] = {}
        record_count = count_records(database_cursor)
        print(f"Checking against {record_count} records from {str(database_path)!r}...")
        for record in yield_records(database_cursor):
            full_record_path = root_path / record.path
            if not full_record_path.is_file():
                print(f"\t- Removing record for {record.path!r}, no such file")
                delete_record(database_cursor, record.path)
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
                digest = future.result()
            except Exception as error:
                print(f"\t- ({index}/{future_count}) Error while calculating hash for {record.path!r}: {error}")
                continue

            hexdigest = hexlify(digest)
            full_record_path = root_path / record.path
            modified = full_record_path.stat().st_mtime
            if record.modified != modified:
                print(f"\t- ({index}/{future_count}) Updating record for {record.path!r}: {hexdigest!r}")
                update_record(database_cursor, record.path, digest, modified)
                database_changes += 1
            elif record.digest != digest:
                modified_date = datetime.fromtimestamp(modified)
                print(f"\t- ({index}/{future_count}) Bitrot detected in {record.path!r}!")
                print(f"\t\tRecorded: {record.hexdigest!r} at {record.modified_date}")
                print(f"\t\tCurrent:  {hexdigest!r} at {modified_date}")
                exit_code = ExitCode.Failure

            maybe_commit()

    # Check for new files
    update_futures: T.Dict[concurrent.futures.Future, pathlib.Path] = {}
    print(f"Checking for new files in {str(root_path)!r}...")
    for path in root_path.rglob("*"):
        if path == database_path or not path.is_file():
            continue

        relative_path = path.relative_to(root_path)
        if record_exists(database_cursor, str(relative_path)):
            continue

        future = executor.submit(get_hash, path, arguments.hash_algorithm, arguments.chunk_size)
        update_futures[future] = path

    future_count = len(update_futures)
    for index, future in enumerate(concurrent.futures.as_completed(update_futures), start=1):
        path = update_futures.pop(future)

        # pylint: disable=broad-except
        try:
            digest = future.result()
        except Exception as error:
            print(f"\t- ({index}/{future_count}) Error while calculating hash for {record.path!r}: {error}")
            continue

        relative_path = path.relative_to(root_path)
        print(f"\t- ({index}/{future_count}) Adding record for {str(relative_path)!r}: {hexlify(digest)!r}")
        update_record(database_cursor, str(relative_path), digest, path.stat().st_mtime)
        database_changes += 1

        maybe_commit()

    database.commit()
    database.close()
    return exit_code


def main() -> None:
    parser = get_argument_parser()
    arguments = parser.parse_args()

    # TODO: Shutdown thread pool
    try:
        parser.exit(run(arguments))
    except KeyboardInterrupt:
        print("Aborted.", file=sys.stderr)
