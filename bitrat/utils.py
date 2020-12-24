import binascii
import os
import pathlib

from bitrat.types import PathType


def get_path(path: PathType) -> pathlib.Path:
    return path if isinstance(path, pathlib.Path) else pathlib.Path(os.fspath(path))


def hexlify(data: bytes) -> str:
    return binascii.hexlify(data).decode("ASCII")
