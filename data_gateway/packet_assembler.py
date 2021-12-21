class PacketAssembler:
    """A protocol for receiving and buffering bytes before assembling them into packets for the packet reader to parse.
    Packets are assumed to take on the following format with any amount of non-packet noise in between:

    ```
    <Packet initiator><Packet type><Packet length (n)><First byte of packet data>...<nth byte of packet data>
    ```

    :param data_gateway.packet_reader.PacketReader packet_reader:
    :return None:
    """

    def __init__(self, packet_reader):
        self.packet_reader = packet_reader
        self.packet_initiator = packet_reader.config.packet_key.to_bytes(1, packet_reader.config.endian)

        # Parameters for assembling packets.
        self.buffer = bytearray()
        self._current_packet = bytearray()
        self._current_packet_type = None
        self._current_packet_expected_length = None
        self._currently_receiving_packet = False

        # Parameters for parsing packets by the packet reader.
        self._previous_timestamp = {}
        self._collected_data = {}

        for sensor_name in packet_reader.config.sensor_names:
            self._previous_timestamp[sensor_name] = -1
            self._collected_data[sensor_name] = [
                ([0] * packet_reader.config.samples_per_packet[sensor_name])
                for _ in range(packet_reader.config.number_of_sensors[sensor_name])
            ]

    def run(self, data):
        """Put the given data from a bytearray into an in-memory buffer, assemble it into packets, and handle them.

        :param bytearray data: any amount of data
        :return None:
        """
        if len(data) == 0:
            return

        self.buffer.extend(data)

        while len(self.buffer) > 0:
            byte = bytearray([self.buffer.pop(0)])
            self._handle_byte(byte)

            # When the expected packet length is reached, handle the packet and get ready for the next one.
            if len(self._current_packet) == self._current_packet_expected_length:
                self._handle_packet()
                self._prepare_for_new_packet()

    def _handle_byte(self, byte):
        """Handle a byte by:
        - Starting a new packet
        - Ignoring it if it arrives outside a defined packet
        - Getting the current packet's type or length from it
        - Adding it to the current packet

        :param bytearray byte:
        :return None:
        """
        # Start collecting a packet if the initiator byte is received.
        if byte == self.packet_initiator:
            self._currently_receiving_packet = True
            return

        # Ignore byte if not currently receiving a packet.
        elif not self._currently_receiving_packet:
            return

        # Receive further bytes for the current packet.
        if self._current_packet_type is None:
            self._current_packet_type = str(int.from_bytes(byte, self.packet_reader.config.endian))

        # Get the expected length of the packet.
        elif self._current_packet_expected_length is None:
            self._current_packet_expected_length = int.from_bytes(byte, self.packet_reader.config.endian)

        # Collect more data until the expected packet length is reached.
        elif len(self._current_packet) < self._current_packet_expected_length:
            self._current_packet.extend(byte)

    def _handle_packet(self):
        """Handle a packet by either updating the handles or parsing it as a payload for the packet reader.

        :return None:
        """
        if self._current_packet_type == str(self.packet_reader.config.type_handle_def):
            self.packet_reader.update_handles(self._current_packet)
            return

        self.packet_reader._parse_payload(
            packet_type=self._current_packet_type,
            payload=self._current_packet,
            data=self._collected_data,
            previous_timestamp=self._previous_timestamp,
        )

    def _prepare_for_new_packet(self):
        """Reset the current packet information.

        :return None:
        """
        self._current_packet = bytearray()
        self._current_packet_type = None
        self._current_packet_expected_length = None
        self._currently_receiving_packet = False
