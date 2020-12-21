import binascii


def hexlify(data: bytes) -> str:
    return binascii.hexlify(data).decode("ASCII")
