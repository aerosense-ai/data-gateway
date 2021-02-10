class GatewayException(Exception):
    """All exceptions raised by the library must inherit from this exception"""


class UnknownPacketTypeException(GatewayException, ValueError):
    """Raised if attempting to parse a packet of unknown type"""

    pass
