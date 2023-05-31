class GatewayError(Exception):
    """All exceptions raised by the library must inherit from this exception"""


class UnknownPacketTypeError(GatewayError, ValueError):
    """Raise if attempting to parse a packet of unknown type"""


class UnknownSensorNameError(GatewayError, ValueError):
    """Raise if an unknown sensor name is used."""


class DataMustBeSavedError(GatewayError, ValueError):
    """Raise if options are given to the packet reader that mean no data will be saved locally or uploaded to the cloud."""
