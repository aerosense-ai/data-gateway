import serial.threaded


class Protocol(serial.threaded.Protocol):
    configuration = None
    packet_reader = None
    initiator = b"\0"

    def __init__(self):
        self.transport = None
        self.current_packet = bytearray()
        self.current_packet_type = None
        self.current_packet_expected_length = None
        self._currently_receiving_packet = False

        self.previous_timestamp = {}
        self.collected_data = {}

        for sensor_name in self.configuration.sensor_names:
            self.previous_timestamp[sensor_name] = -1
            self.collected_data[sensor_name] = [
                ([0] * self.configuration.samples_per_packet[sensor_name])
                for _ in range(self.configuration.number_of_sensors[sensor_name])
            ]

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.transport = None
        super().connection_lost(exc)

    def data_received(self, data):
        """Assemble data into packets and handle them.

        :param bytes data:
        :return None:
        """
        # Ignore empty data.
        if len(data) == 0:
            return

        # Start collecting a packet if the initiator byte is received.
        if data == self.initiator:
            self._currently_receiving_packet = True
            return

        # Ignore data if not currently receiving a packet.
        if not self._currently_receiving_packet:
            return

        # Receive further bytes for the current packet.
        if self.current_packet_type is None:
            self.current_packet_type = str(int.from_bytes(data, self.configuration.endian))

        elif self.current_packet_expected_length is None:
            self.current_packet_expected_length = int.from_bytes(data, self.configuration.endian)

        # Collect more data until the expected packet length is reached.
        elif len(self.current_packet) < self.current_packet_expected_length:
            self.current_packet.extend(data)

        # When the expected packet length is reached, handle the packet and get ready for the next one.
        else:
            self.handle_packet(self.current_packet)
            self._get_ready_for_new_packet()

    def handle_packet(self, packet):
        """Handle a packet by either updating the handles or parsing it as a payload for the packet reader.

        :param bytes packet:
        :return None:
        """
        if self.current_packet_type == str(self.configuration.type_handle_def):
            self.packet_reader.update_handles(packet)
            return

        self.packet_reader._parse_payload(
            packet_type=self.current_packet_type,
            payload=packet,
            data=self.collected_data,
            previous_timestamp=self.previous_timestamp,
        )

    def _get_ready_for_new_packet(self):
        """Reset the current packet information.

        :return None:
        """
        self.current_packet = bytearray()
        self.current_packet_type = None
        self.current_packet_expected_length = None
        self._currently_receiving_packet = False


class CustomReaderThread(serial.threaded.ReaderThread):
    def __init__(self, serial_instance, protocol_factory, stop_when_no_more_data=False):
        self.stop_when_no_more_data = stop_when_no_more_data
        super().__init__(serial_instance, protocol_factory)

    def run(self):
        """Reader loop"""
        if not hasattr(self.serial, "cancel_read"):
            self.serial.timeout = 1

        self.protocol = self.protocol_factory()

        try:
            self.protocol.connection_made(self)

        except Exception as e:
            self.alive = False
            self.protocol.connection_lost(e)
            self._connection_made.set()
            return

        error = None
        self._connection_made.set()

        while self.alive and self.serial.is_open:
            try:
                # read all that is there or wait for one byte (blocking)
                data = self.serial.read(1)
            except serial.SerialException as e:
                # probably some I/O problem such as disconnected USB serial
                # adapters -> exit
                error = e
                break
            else:
                if data:
                    # make a separated try-except for called user code
                    try:
                        self.protocol.data_received(data)
                    except Exception as e:
                        error = e
                        break

                else:
                    if self.stop_when_no_more_data:
                        self.serial.is_open = False
                        self.protocol.packet_reader.stop = True

        self.alive = False
        self.protocol.connection_lost(error)
        self.protocol = None
