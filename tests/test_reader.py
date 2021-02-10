import os
import tempfile
import unittest
from gcloud_storage_emulator.server import create_server
from google.cloud import storage
from octue.utils.cloud.credentials import GCPCredentialsManager
from octue.utils.cloud.persistence import GoogleCloudStorageClient

from dummy_serial.dummy_serial import DummySerial
from dummy_serial.utils import random_bytes
from gateway.readers.constants import PACKET_KEY
from gateway.readers.packet_reader import PacketReader
from gateway.uploaders import CLOUD_DIRECTORY_NAME


class TestPacketReader(unittest.TestCase):
    """Testing operation of the PacketReader class"""

    TEST_PROJECT_NAME = os.environ["TEST_PROJECT_NAME"]
    TEST_BUCKET_NAME = os.environ["TEST_BUCKET_NAME"]
    PACKET_KEY = PACKET_KEY.to_bytes(1, "little")
    LENGTH = bytes([244])
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

    def _check_batches_are_uploaded_to_cloud(self, packet_reader, sensor_name):
        number_of_batches = packet_reader.uploader.streams[sensor_name]["batch_number"]
        self.assertTrue(number_of_batches > 0)

        with tempfile.TemporaryDirectory() as temporary_directory:
            for i in range(number_of_batches):
                batch_download_path = os.path.join(temporary_directory, f"batch-{i}.csv")

                GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME).download_file(
                    bucket_name=self.TEST_BUCKET_NAME,
                    path_in_bucket=f"{CLOUD_DIRECTORY_NAME}/{sensor_name}/batch-{i}.csv",
                    local_path=batch_download_path,
                )

                with open(batch_download_path) as f:
                    outputs = f.read().split("\n")
                    self.assertTrue(len(outputs) > 1)
                    self.assertTrue(len(outputs[0].split(",")) > 1)

    def test_packet_reader_with_baro_sensor(self):
        serial_port = DummySerial(port="test")
        sensor_type = bytes([34])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        packet_reader = PacketReader()

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_filenames(temporary_directory)
            packet_reader.read_packets(serial_port, filenames, stop_when_no_more_data=True)

            with open(os.path.join(temporary_directory, "baros.csv")) as f:
                outputs = f.read().split("\n")
                self.assertTrue(len(outputs) > 1)
                self.assertTrue(len(outputs[0].split(",")) > 1)

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_name="Baros")

    def test_packet_reader_with_mic_sensor(self):
        serial_port = DummySerial(port="test")
        sensor_type = bytes([54])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        packet_reader = PacketReader()

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_filenames(temporary_directory)
            packet_reader.read_packets(serial_port, filenames, stop_when_no_more_data=True)

            with open(os.path.join(temporary_directory, "mics.csv")) as f:
                outputs = f.read().split("\n")
                self.assertTrue(len(outputs) > 1)
                self.assertTrue(len(outputs[0].split(",")) > 1)

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_name="Mics")

    def test_packet_reader_with_acc_sensor(self):
        serial_port = DummySerial(port="test")
        sensor_type = bytes([74])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        packet_reader = PacketReader()

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_filenames(temporary_directory)
            packet_reader.read_packets(serial_port, filenames, stop_when_no_more_data=True)

            with open(os.path.join(temporary_directory, "acc.csv")) as f:
                outputs = f.read().split("\n")
                self.assertTrue(len(outputs) > 1)
                self.assertTrue(len(outputs[0].split(",")) > 1)

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_name="Acc")

    def test_packet_reader_with_gyro_sensor(self):
        serial_port = DummySerial(port="test")
        sensor_type = bytes([76])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        packet_reader = PacketReader()

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_filenames(temporary_directory)
            packet_reader.read_packets(serial_port, filenames, stop_when_no_more_data=True)

            with open(os.path.join(temporary_directory, "gyro.csv")) as f:
                outputs = f.read().split("\n")
                self.assertTrue(len(outputs) > 1)
                self.assertTrue(len(outputs[0].split(",")) > 1)

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_name="Gyro")

    def test_packet_reader_with_mag_sensor(self):
        serial_port = DummySerial(port="test")
        sensor_type = bytes([78])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        packet_reader = PacketReader()

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_filenames(temporary_directory)
            packet_reader.read_packets(serial_port, filenames, stop_when_no_more_data=True)

            with open(os.path.join(temporary_directory, "mag.csv")) as f:
                outputs = f.read().split("\n")
                self.assertTrue(len(outputs) > 1)
                self.assertTrue(len(outputs[0].split(",")) > 1)

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_name="Mag")

    def test_packet_reader_with_analog_sensor(self):
        serial_port = DummySerial(port="test")
        sensor_type = bytes([80])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        packet_reader = PacketReader()

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_filenames(temporary_directory)
            packet_reader.read_packets(serial_port, filenames, stop_when_no_more_data=True)

            with open(os.path.join(temporary_directory, "analog.csv")) as f:
                outputs = f.read().split("\n")
                self.assertTrue(len(outputs) > 1)
                self.assertTrue(len(outputs[0].split(",")) > 1)

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_name="Analog")


if __name__ == "__main__":
    unittest.main()
