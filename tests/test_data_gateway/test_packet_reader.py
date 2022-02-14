import multiprocessing
import tempfile
from unittest.mock import patch

from data_gateway.packet_reader import PacketReader
from tests import LENGTH, PACKET_KEY, RANDOM_BYTES
from tests.base import BaseTestCase


class TestPacketReader(BaseTestCase):
    def test_error_is_logged_if_unknown_sensor_type_packet_is_received(self):
        """Test that an error is logged if an unknown sensor type packet is received."""
        queue = multiprocessing.Queue()
        queue.put({"packet_type": bytes([0]), "packet": b"".join((PACKET_KEY, bytes([0]), LENGTH, RANDOM_BYTES[0]))})

        packet_reader = PacketReader(
            save_locally=False,
            upload_to_cloud=False,
            output_directory=tempfile.TemporaryDirectory().name,
        )

        with patch("data_gateway.packet_reader.logger") as mock_logger:
            packet_reader.parse_packets(
                packet_queue=queue,
                stop_signal=multiprocessing.Value("i", 0),
                stop_when_no_more_data_after=0.1,
            )

        self.assertIn("Received packet with unknown type: ", mock_logger.method_calls[2].args[0])

    def test_update_handles_fails_if_start_and_end_handles_are_incorrect(self):
        """Test that an error is raised if the start and end handles are incorrect when trying to update handles."""
        packet = bytearray(RANDOM_BYTES[0])
        packet[0:1] = int(0).to_bytes(1, "little")
        packet[2:3] = int(255).to_bytes(1, "little")

        packet_reader = PacketReader(
            save_locally=False,
            upload_to_cloud=False,
            output_directory=tempfile.TemporaryDirectory().name,
        )

        with patch("data_gateway.packet_reader.logger") as mock_logger:
            packet_reader.update_handles(packet)

        self.assertIn("Handle error", mock_logger.method_calls[0].args[0])

    def test_update_handles(self):
        """Test that the handles can be updated."""
        packet = bytearray(RANDOM_BYTES[0])
        packet[0:1] = int(0).to_bytes(1, "little")
        packet[2:3] = int(26).to_bytes(1, "little")
        packet_reader = PacketReader(
            save_locally=False,
            upload_to_cloud=False,
            output_directory=tempfile.TemporaryDirectory().name,
        )

        with patch("data_gateway.packet_reader.logger") as mock_logger:
            packet_reader.update_handles(packet)

        self.assertIn("Successfully updated handles", mock_logger.method_calls[0].args[0])

    def test_packet_reader_with_info_packets(self):
        """Test that the packet reader works with info packets."""
        packet_types = [bytes([40]), bytes([54]), bytes([56]), bytes([58])]

        packets = [
            [bytes([1]), bytes([2]), bytes([3])],
            [bytes([0]), bytes([1]), bytes([2]), bytes([3])],
            [bytes([0]), bytes([1])],
            [bytes([0])],
        ]

        queue = multiprocessing.Queue()

        for index, packet_type in enumerate(packet_types):
            for packet in packets[index]:
                queue.put({"packet_type": str(int.from_bytes(packet_type, "little")), "packet": packet})

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=False,
                output_directory=temporary_directory,
            )

            with patch("data_gateway.packet_reader.logger") as mock_logger:
                packet_reader.parse_packets(
                    packet_queue=queue,
                    stop_signal=multiprocessing.Value("i", 0),
                    stop_when_no_more_data_after=0.1,
                )

                log_messages = [call_arg.args for call_arg in mock_logger.info.call_args_list]

                for message in [
                    ("Microphone data reading done",),
                    ("Microphone data erasing done",),
                    ("Microphones started ",),
                    ("Command declined, %s", "Bad block detection ongoing"),
                    ("Command declined, %s", "Task already registered, cannot register again"),
                    ("Command declined, %s", "Task is not registered, cannot de-register"),
                    ("Command declined, %s", "Connection Parameter update unfinished"),
                    ("\n%s\n", "Exiting sleep"),
                    ("\n%s\n", "Entering sleep"),
                    ("Battery info",),
                    ("Voltage : %fV\n Cycle count: %f\nState of charge: %f%%", 0.0, 0.0, 0.0),
                ]:
                    self.assertIn(message, log_messages)