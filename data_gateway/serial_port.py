import logging
import os

import serial

from data_gateway.dummy_serial import DummySerial


logger = logging.getLogger(__name__)


def get_serial_port(serial_port, configuration, use_dummy_serial_port=False):
    """Get the serial port or a dummy serial port if specified. If a `serial.Serial` instance is provided, return that
    as the serial port to use.

    :param str|serial.Serial serial_port: the name of a serial port or a `serial.Serial` instance
    :param data_gateway.configuration.Configuration configuration: the packet reader configuration
    :param bool use_dummy_serial_port: if `True`, use a dummy serial port instead
    :return serial.Serial|data_gateway.dummy_serial.DummySerial:
    """
    if isinstance(serial_port, str):
        serial_port_name = serial_port

        if use_dummy_serial_port:
            serial_port = DummySerial(port=serial_port_name, baudrate=configuration.baudrate)
        else:
            serial_port = serial.Serial(port=serial_port_name, baudrate=configuration.baudrate)

        logger.info("Serial port %r found.", serial_port_name)

        # The buffer size can only be set on Windows.
        if os.name == "nt":
            serial_port.set_buffer_size(
                rx_size=configuration.serial_buffer_rx_size,
                tx_size=configuration.serial_buffer_tx_size,
            )
        else:
            logger.debug("Serial port buffer size can only be set on Windows.")

    return serial_port
