import serial.threaded


class SerialPortReadingProtocol(serial.threaded.Protocol):
    """A threaded protocol for reading data from a serial port and assembling it into packets for the packet reader to
    parse. Serial port reading is carried out in a separate thread to packet handling, reducing the likelihood of data
    loss due to a serial port buffer overflow. Packets are assumed to take on the following format with any amount of
    non-packet noise in between:

    ```
    <Packet key><Packet type><Packet length><First byte of packet data>...<nth byte of packet data>
    ```

    :return None:
    """

    initiator = b"\0"

    def __init__(self):
        self.transport = None
        self.buffer = bytearray()

        self.current_packet = bytearray()
        self.current_packet_type = None
        self.current_packet_expected_length = None
        self._currently_receiving_packet = False

        self.previous_timestamp = {}
        self.collected_data = {}

    def connection_made(self, transport):
        """Set the transport attribute to the reader thread when it is started, update the previous timestamp and
        collected data parameters, and set the packet initiator to the packet key from the packet reader configuration.

        :param CustomReaderThread transport:
        :return None:
        """
        self.transport = transport

        for sensor_name in transport.packet_reader.config.sensor_names:
            self.previous_timestamp[sensor_name] = -1
            self.collected_data[sensor_name] = [
                ([0] * transport.packet_reader.config.samples_per_packet[sensor_name])
                for _ in range(transport.packet_reader.config.number_of_sensors[sensor_name])
            ]

        self.initiator = transport.packet_reader.config.packet_key.to_bytes(1, transport.packet_reader.config.endian)

    def connection_lost(self, exception):
        """Forget the reader thread.

        :param Exception exception:
        :return None:
        """
        self.transport = None
        super().connection_lost(exception)

    def data_received(self, data):
        """Pull all waiting data from the serial port into an in-memory buffer, assemble it into packets, and handle
        the packets.

        :param bytes data:
        :return None:
        """
        if len(data) == 0:
            return

        self.buffer.extend(data)

        while len(self.buffer) > 0:

            byte = bytearray([self.buffer.pop(0)])

            # Start collecting a packet if the initiator byte is received.
            if byte == self.initiator:
                self._currently_receiving_packet = True
                continue

            # Ignore byte if not currently receiving a packet.
            if not self._currently_receiving_packet:
                continue

            # Receive further bytes for the current packet.
            if self.current_packet_type is None:
                self.current_packet_type = str(int.from_bytes(byte, self.transport.packet_reader.config.endian))
                continue

            # Get the expected length of the packet.
            if self.current_packet_expected_length is None:
                self.current_packet_expected_length = int.from_bytes(byte, self.transport.packet_reader.config.endian)
                continue

            # Collect more data until the expected packet length is reached.
            if len(self.current_packet) < self.current_packet_expected_length:
                self.current_packet.extend(byte)

            # When the expected packet length is reached, handle the packet and get ready for the next one.
            if len(self.current_packet) == self.current_packet_expected_length:
                self.handle_packet()
                self._prepare_for_new_packet()

            if self.transport.stop_when_no_more_data and self.transport.serial.in_waiting == 0:
                self.transport.serial.close()
                self.transport.packet_reader.stop = True

    def handle_packet(self):
        """Handle a packet by either updating the handles or parsing it as a payload for the packet reader.

        :return None:
        """
        if self.current_packet_type == str(self.transport.packet_reader.config.type_handle_def):
            self.transport.packet_reader.update_handles(self.current_packet)
            return

        self.transport.packet_reader.parse_payload(
            packet_type=self.current_packet_type,
            payload=self.current_packet,
            data=self.collected_data,
            previous_timestamp=self.previous_timestamp,
        )

    def _prepare_for_new_packet(self):
        """Reset the current packet information.

        :return None:
        """
        self.current_packet = bytearray()
        self.current_packet_type = None
        self.current_packet_expected_length = None
        self._currently_receiving_packet = False


class CustomReaderThread(serial.threaded.ReaderThread):
    """A custom thread-based serial port reader for the data gateway.

    :param serial.Serial serial_instance: the serial port to read from
    :param type protocol_factory: this should be the `SerialPortReadingProtocol` class (not an instance of it)
    :param data_gateway.packet_reader.PacketReader packet_reader: an instance of the data gateway packet reader
    :param bool stop_when_no_more_data: stop reading when no more data is received from the port (for testing)
    :return None:
    """

    def __init__(self, serial_instance, protocol_factory, packet_reader, stop_when_no_more_data=False):
        self.packet_reader = packet_reader
        self.stop_when_no_more_data = stop_when_no_more_data
        super().__init__(serial_instance, protocol_factory)
