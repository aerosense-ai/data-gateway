class GatewayError(Exception):
    """All exceptions raised by the library must inherit from this exception"""


class UnknownPacketTypeError(GatewayError, ValueError):
    """Raise if attempting to parse a packet of unknown type"""


class UnknownSensorNameError(GatewayError, ValueError):
    """Raise if an unknown sensor name is used."""


class WrongNumberOfSensorCoordinatesError(GatewayError, ValueError):
    """Raise if the number of sensor coordinates given for a sensor does not equal the number of sensors specified in
    the `number_of_sensors` configuration field.
    """
