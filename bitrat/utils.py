import binascii
import pathlib
import os

from bitrat.types import PathType


def ensure_path(path: PathType) -> pathlib.Path:
    if isinstance(path, pathlib.Path):
        return path

    return pathlib.Path(os.fspath(path))


def hexlify(data: bytes) -> str:
    return binascii.hexlify(data).decode("ASCII")
