import multiprocessing
from unittest.mock import patch

from data_gateway.packet_reader import PacketReader
from tests import LENGTH, PACKET_KEY, RANDOM_BYTES
from tests.base import BaseTestCase


class TestPacketReader(BaseTestCase):
    def test_error_is_logged_if_unknown_sensor_type_packet_is_received(self):
        """Test that an error is logged if an unknown sensor type packet is received."""
        queue = multiprocessing.Queue()
        queue.put({"packet_type": bytes([0]), "packet": b"".join((PACKET_KEY, bytes([0]), LENGTH, RANDOM_BYTES[0]))})

        packet_reader = PacketReader(save_locally=False, upload_to_cloud=False)

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

        packet_reader = PacketReader(save_locally=False, upload_to_cloud=False)

        with patch("data_gateway.packet_reader.logger") as mock_logger:
            packet_reader.update_handles(packet)

        self.assertIn("Handle error", mock_logger.method_calls[0].args[0])

    def test_update_handles(self):
        """Test that the handles can be updated."""
        packet = bytearray(RANDOM_BYTES[0])
        packet[0:1] = int(0).to_bytes(1, "little")
        packet[2:3] = int(26).to_bytes(1, "little")
        packet_reader = PacketReader(save_locally=False, upload_to_cloud=False)

        with patch("data_gateway.packet_reader.logger") as mock_logger:
            packet_reader.update_handles(packet)

        self.assertIn("Successfully updated handles", mock_logger.method_calls[0].args[0])
