import argparse
import enum
import hashlib
import os


@enum.unique
class ExitCode(enum.IntEnum):
    Success = 0
    Failure = 1


def get_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--path", "-p", default=os.getcwd())
    parser.add_argument("--hash-algorithm", "-H", default="sha1", choices=hashlib.algorithms_available)
    parser.add_argument("--workers", "-w", type=int, default=os.cpu_count())
    parser.add_argument("--chunk-size", "-c", type=int, default=1000 ** 2)
    return parser
