import binascii
import datetime
import functools


def hexlify(data: bytes) -> str:
    return binascii.hexlify(data).decode("ASCII")


@functools.cache
def get_system_timezone() -> datetime.tzinfo | None:
    return datetime.datetime.now().astimezone().tzinfo
