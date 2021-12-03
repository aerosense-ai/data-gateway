import random
from os import urandom

from data_gateway.dummy_serial.constants import ALPHABET, NUMBERS


def random_bytes(length=8, as_bytearray=False):
    """Generate a random bytes object of a given length.

    :param length: Length of bytes object to generate (default 8)
    :type length: int
    :param as_bytearray: If true, return a (mutable) bytearray type, rather than an (immutable) bytes type.
    :type as_bytearray: bool
    :return: Bytes or bytearray object of given length, initialised to random values in [0, 255]
    :rtype: Union[bytes, bytearray]
    """
    if as_bytearray:
        return bytearray(urandom(length))
    else:
        return bytes(urandom(length))


def random_string(length=8, alphabet=None):
    """Generate a random string for test cases.

    :param length: Length of string to generate.
    :type length: int
    :param alphabet: Alphabet to use to create string (default A-Z,0-9)
    :type alphabet: str
    """
    alphabet = alphabet or ALPHABET + NUMBERS
    return "".join(random.choice(alphabet) for _ in range(length))
