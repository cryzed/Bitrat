import argparse
import enum
import hashlib
import os
import pathlib


@enum.unique
class ExitCode(enum.IntEnum):
    SUCCESS = 0
    FAILURE = 1


def get_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--path", "-p", type=pathlib.Path, default=pathlib.Path.cwd(), help="Target directory to scan")
    parser.add_argument(
        "--hash-algorithm", "-H", default="sha1", choices=hashlib.algorithms_available, help="Hash algorithm to use"
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=min(os.cpu_count() or 1, 2),
        help="Number of parallel hashing workers (disk I/O-bound on a single disk)",
    )
    parser.add_argument(
        "--chunk-size", "-c", type=int, default=1000**2, help="Bytes read per chunk when hashing a file"
    )
    parser.add_argument(
        "--save-every",
        "-s",
        type=int,
        default=1000,
        help="Batch size for database commits and in-flight concurrency cap",
    )
    parser.add_argument(
        "--no-check",
        "-n",
        dest="check",
        action="store_false",
        help="Skip the integrity check phase; only add or update files",
    )
    parser.add_argument(
        "--since-last-check",
        type=int,
        default=86400,
        help="Only check files not verified in the last N seconds (default: 86400 = 24 hours)",
    )
    return parser
