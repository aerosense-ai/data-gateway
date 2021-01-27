import logging.handlers
import time
from serial.serialutil import PortNotOpenError, SerialException

from . import constants, exceptions


logger = logging.getLogger(__name__)


class DummySerial(object):
    """ Dummy (mock) serial port for testing purposes.

    Mimics the behavior of a serial port as defined by the `pyserial <http://pyserial.sourceforge.net/>`_ module.

    As the portname argument not is used properly, only one port on :mod:`dummyserial` can be used simultaneously.

    :param port: Serial port
    :type port: str
    :param timeout: Timeout in seconds (Default 2 seconds)
    :type timeout: int
    :param responses: Dictionary of response strings or method for generating a response
    :type responses: Union[dict, function]
    :param baudrate: Baudrate (Default is 9600)
    :type baudrate: int
    """

    def __init__(self, *args, **kwargs):
        """ Class constructor for DummySerial
        """
        logger.debug("args=%s", args)
        logger.debug("kwargs=%s", kwargs)

        self._isOpen = True  # pylint: disable=C0103
        self._waiting_data = constants.NO_DATA_PRESENT

        self.port = kwargs["port"]  # Serial port name.
        self.initial_port_name = self.port  # Initial name given to the port

        self.responses = kwargs.get("responses", {})
        self.timeout = kwargs.get("timeout", constants.DEFAULT_TIMEOUT)
        self.baudrate = kwargs.get("baudrate", constants.DEFAULT_BAUDRATE)

    def __repr__(self):
        """ String representation of the DummySerial instance
        """
        return "{0}.{1}<id=0x{2:x}, open={3}>(port={4!r}, timeout={5!r}, " "waiting_data={6!r})".format(
            self.__module__,
            self.__class__.__name__,
            id(self),
            self._isOpen,
            self.port,
            self.timeout,
            self._waiting_data,
        )

    def open(self):
        """ Open the dummy serial port
        """
        logger.debug("Opening port")

        if self._isOpen:
            raise SerialException("Port is already open.")

        self._isOpen = True
        self.port = self.initial_port_name

    def close(self):
        """ Close the dummy serial port
        """
        logger.debug("Closing port")
        if self._isOpen:
            self._isOpen = False
        self.port = None

    def write(self, data):
        """ Write to the dummy serial port

        This will affect the response for subsequent read operations.

        :param data: data to write to the port
        :type data: Union[bytes, bytearray]
        """

        if not self._isOpen:
            raise PortNotOpenError

        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("expected bytes or bytearray, got {}".format(type(data)))

        logger.debug("type(data): %s", type(data))

        if isinstance(data, bytearray):
            data = bytes(data)

        logger.debug("Writing bytes(%s): %s", len(data), self._ashex(data))

        self._waiting_data = self._check_response(data)

    def read(self, size=1):
        """ Read size bytes from the Dummy Serial Responses.
        The response is dependent on what was written last to the port on
        dummyserial, and what is defined in the :data:`RESPONSES` dictionary.

        If the response is shorter than size, it will sleep for timeout.
        If the response is longer than size, it will return only size bytes.

        :param size: For compatibility with the real function.
        :type size: int
        :return:
        :rtype: bytes

        """
        logger.debug("Reading %s bytes.", size)

        if not self._isOpen:
            raise PortNotOpenError

        if size < 0:
            raise exceptions.DSIOError("The size to read must not be negative. Given: {!r}".format(size))

        elif size < len(self._waiting_data):
            # Partially flush the buffer to response
            logger.debug(
                "The size (%s) to read is smaller than the available data. Some bytes will be kept for later. Available (%s): '%s'",
                size,
                len(self._waiting_data),
                self._waiting_data,
            )
            data_out = self._waiting_data[:size]
            self._waiting_data = self._waiting_data[size:]

        elif size == len(self._waiting_data):
            # Flush the buffer fully to the response
            data_out = self._waiting_data
            self._waiting_data = constants.NO_DATA_PRESENT

        else:
            # Wait for timeout - we asked for more data than available!
            logger.debug(
                "The size (%s) to read is larger than the available data. Will sleep until timeout. Available (%s): '%s'",
                size,
                len(self._waiting_data),
                self._waiting_data,
            )
            time.sleep(self.timeout)
            data_out = self._waiting_data
            self._waiting_data = constants.NO_DATA_PRESENT

        logger.debug('Read (%s): "%s"', len(data_out), data_out)

        return data_out

    @property
    def in_waiting(self):
        """ Length of waiting output data
        """
        return len(self._waiting_data)

    def _check_response(self, data_in):

        data_out = constants.NO_DATA_PRESENT
        if data_in in self.responses:
            data_out = self.responses[data_in]

        return data_out

    def _ashex(self, msg):
        return " ".join(["{:02X}".format(x) for x in msg])
