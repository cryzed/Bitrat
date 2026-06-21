import binascii
import datetime
import functools
import os
import pathlib

from .types import PathType


def get_path(path: PathType) -> pathlib.Path:
    return path if isinstance(path, pathlib.Path) else pathlib.Path(os.fspath(path))


def hexlify(data: bytes) -> str:
    return binascii.hexlify(data).decode("ASCII")


@functools.cache
def get_system_timezone() -> datetime.tzinfo | None:
    return datetime.datetime.now().astimezone().tzinfo
