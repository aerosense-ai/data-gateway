import os
import tempfile
import unittest
from gcloud_storage_emulator.server import create_server
from google.cloud import storage
from octue.utils.cloud.credentials import GCPCredentialsManager

from dummy_serial.dummy_serial import DummySerial
from dummy_serial.utils import random_bytes
from gateway.readers.constants import PACKET_KEY
from gateway.readers.packet_reader import read_packets


class TestPacketReader(unittest.TestCase):
    """Testing operation of the PacketReader class"""

    TEST_PROJECT_NAME = os.environ["TEST_PROJECT_NAME"]
    TEST_BUCKET_NAME = os.environ["TEST_BUCKET_NAME"]
    storage_emulator = create_server("localhost", 9090, in_memory=True)

    @classmethod
    def setUpClass(cls):
        cls.storage_emulator.start()
        storage.Client(
            project=cls.TEST_PROJECT_NAME, credentials=GCPCredentialsManager().get_credentials()
        ).create_bucket(bucket_or_name=cls.TEST_BUCKET_NAME)

    @classmethod
    def tearDownClass(cls):
        cls.storage_emulator.stop()

    def _generate_filenames(self, directory_path):
        return {
            "Mics": os.path.join(directory_path, "mics.csv"),
            "Baros": os.path.join(directory_path, "baros.csv"),
            "Acc": os.path.join(directory_path, "acc.csv"),
            "Gyro": os.path.join(directory_path, "gyro.csv"),
            "Mag": os.path.join(directory_path, "mag.csv"),
            "Analog": os.path.join(directory_path, "analog.csv"),
        }

    def test_packet_reader_with_baro_sensor(self):
        serial_port = DummySerial(port="test")
        packet_key = PACKET_KEY.to_bytes(1, "little")
        sensor_type = bytes([34])
        length = bytes([244])

        for _ in range(2):
            serial_port.write(data=packet_key + sensor_type + length + random_bytes(256))

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_filenames(temporary_directory)
            read_packets(serial_port, filenames, stop_when_no_more_data=True)

            with open(os.path.join(temporary_directory, "baros.csv")) as f:
                outputs = f.read().split("\n")
                self.assertTrue(len(outputs) > 1)
                self.assertTrue(len(outputs[0].split(",")) > 1)

    def test_packet_reader_with_mic_sensor(self):
        serial_port = DummySerial(port="test")
        packet_key = PACKET_KEY.to_bytes(1, "little")
        sensor_type = bytes([54])
        length = bytes([244])

        for _ in range(2):
            serial_port.write(data=packet_key + sensor_type + length + random_bytes(256))

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_filenames(temporary_directory)
            read_packets(serial_port, filenames, stop_when_no_more_data=True)

            with open(os.path.join(temporary_directory, "mics.csv")) as f:
                outputs = f.read().split("\n")
                self.assertTrue(len(outputs) > 1)
                self.assertTrue(len(outputs[0].split(",")) > 1)

    def test_packet_reader_with_acc_sensor(self):
        serial_port = DummySerial(port="test")
        packet_key = PACKET_KEY.to_bytes(1, "little")
        sensor_type = bytes([74])
        length = bytes([244])

        for _ in range(2):
            serial_port.write(data=packet_key + sensor_type + length + random_bytes(256))

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_filenames(temporary_directory)
            read_packets(serial_port, filenames, stop_when_no_more_data=True)

            with open(os.path.join(temporary_directory, "acc.csv")) as f:
                outputs = f.read().split("\n")
                self.assertTrue(len(outputs) > 1)
                self.assertTrue(len(outputs[0].split(",")) > 1)

    def test_packet_reader_with_gyro_sensor(self):
        serial_port = DummySerial(port="test")
        packet_key = PACKET_KEY.to_bytes(1, "little")
        sensor_type = bytes([76])
        length = bytes([244])

        for _ in range(2):
            serial_port.write(data=packet_key + sensor_type + length + random_bytes(256))

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_filenames(temporary_directory)
            read_packets(serial_port, filenames, stop_when_no_more_data=True)

            with open(os.path.join(temporary_directory, "gyro.csv")) as f:
                outputs = f.read().split("\n")
                self.assertTrue(len(outputs) > 1)
                self.assertTrue(len(outputs[0].split(",")) > 1)

    def test_packet_reader_with_mag_sensor(self):
        serial_port = DummySerial(port="test")
        packet_key = PACKET_KEY.to_bytes(1, "little")
        sensor_type = bytes([78])
        length = bytes([244])

        for _ in range(2):
            serial_port.write(data=packet_key + sensor_type + length + random_bytes(256))

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_filenames(temporary_directory)
            read_packets(serial_port, filenames, stop_when_no_more_data=True)

            with open(os.path.join(temporary_directory, "mag.csv")) as f:
                outputs = f.read().split("\n")
                self.assertTrue(len(outputs) > 1)
                self.assertTrue(len(outputs[0].split(",")) > 1)

    def test_packet_reader_with_analog_sensor(self):
        serial_port = DummySerial(port="test")
        packet_key = PACKET_KEY.to_bytes(1, "little")
        sensor_type = bytes([80])
        length = bytes([244])

        for _ in range(2):
            serial_port.write(data=packet_key + sensor_type + length + random_bytes(256))

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_filenames(temporary_directory)
            read_packets(serial_port, filenames, stop_when_no_more_data=True)

            with open(os.path.join(temporary_directory, "analog.csv")) as f:
                outputs = f.read().split("\n")
                self.assertTrue(len(outputs) > 1)
                self.assertTrue(len(outputs[0].split(",")) > 1)


if __name__ == "__main__":
    unittest.main()
