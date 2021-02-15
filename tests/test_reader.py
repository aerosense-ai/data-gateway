import os
import tempfile
import unittest
from gcloud_storage_emulator.server import create_server
from google.cloud import storage
from octue.utils.cloud.credentials import GCPCredentialsManager
from octue.utils.cloud.persistence import GoogleCloudStorageClient

from data_gateway.readers.constants import PACKET_KEY
from data_gateway.readers.packet_reader import PacketReader
from data_gateway.uploaders import CLOUD_DIRECTORY_NAME
from dummy_serial.dummy_serial import DummySerial
from dummy_serial.utils import random_bytes


class TestPacketReader(unittest.TestCase):
    """Testing operation of the PacketReader class"""

    TEST_PROJECT_NAME = os.environ["TEST_PROJECT_NAME"]
    TEST_BUCKET_NAME = os.environ["TEST_BUCKET_NAME"]
    PACKET_KEY = PACKET_KEY.to_bytes(1, "little")
    LENGTH = bytes([244])
    UPLOAD_INTERVAL = 10
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

    @staticmethod
    def _generate_file_paths(directory_path):
        """Generate paths for the expected output files."""
        return {
            "Mics": os.path.join(directory_path, "mics.csv"),
            "Baros": os.path.join(directory_path, "baros.csv"),
            "Acc": os.path.join(directory_path, "acc.csv"),
            "Gyro": os.path.join(directory_path, "gyro.csv"),
            "Mag": os.path.join(directory_path, "mag.csv"),
            "Analog": os.path.join(directory_path, "analog.csv"),
        }

    def _check_batches_are_uploaded_to_cloud(self, packet_reader, sensor_name, number_of_batches_to_check=5):
        """Check that non-trivial batches from a packet reader for a particular sensor are uploaded to cloud storage."""
        number_of_batches = packet_reader.uploader.batcher.current_batches[sensor_name]["batch_number"]
        self.assertTrue(number_of_batches > 0)

        client = GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME)

        for i in range(number_of_batches_to_check):
            data = client.download_as_string(
                bucket_name=self.TEST_BUCKET_NAME,
                path_in_bucket=f"{CLOUD_DIRECTORY_NAME}/{sensor_name}/batch-{i}.csv",
            )

            lines = data.split("\n")
            self.assertTrue(len(lines) > 1)
            self.assertTrue(len(lines[0].split(",")) > 1)

    def _check_data_is_written_to_files(self, temporary_directory, filename):
        """Check that non-trivial data is written to the given file."""
        with open(os.path.join(temporary_directory, filename)) as f:
            outputs = f.read().split("\n")
            self.assertTrue(len(outputs) > 1)
            self.assertTrue(len(outputs[0].split(",")) > 1)

    def test_packet_reader_with_baro_sensor(self):
        """Test that the packet reader works with the baro sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([34])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        packet_reader = PacketReader(save_locally=True, upload_to_cloud=True, upload_interval=self.UPLOAD_INTERVAL)

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_file_paths(temporary_directory)
            packet_reader.read_packets(serial_port, filenames, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(temporary_directory, "baros.csv")

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_name="Baros", number_of_batches_to_check=1)

    def test_packet_reader_with_mic_sensor(self):
        """Test that the packet reader works with the mic sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([54])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        packet_reader = PacketReader(save_locally=True, upload_to_cloud=True, upload_interval=self.UPLOAD_INTERVAL)

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_file_paths(temporary_directory)
            packet_reader.read_packets(serial_port, filenames, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(temporary_directory, "mics.csv")

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_name="Mics", number_of_batches_to_check=1)

    def test_packet_reader_with_acc_sensor(self):
        """Test that the packet reader works with the acc sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([74])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        packet_reader = PacketReader(save_locally=True, upload_to_cloud=True, upload_interval=self.UPLOAD_INTERVAL)

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_file_paths(temporary_directory)
            packet_reader.read_packets(serial_port, filenames, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(temporary_directory, "acc.csv")

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_name="Acc", number_of_batches_to_check=1)

    def test_packet_reader_with_gyro_sensor(self):
        """Test that the packet reader works with the gyro sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([76])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        packet_reader = PacketReader(save_locally=True, upload_to_cloud=True, upload_interval=self.UPLOAD_INTERVAL)

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_file_paths(temporary_directory)
            packet_reader.read_packets(serial_port, filenames, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(temporary_directory, "gyro.csv")

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_name="Gyro", number_of_batches_to_check=1)

    def test_packet_reader_with_mag_sensor(self):
        """Test that the packet reader works with the mag sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([78])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        packet_reader = PacketReader(save_locally=True, upload_to_cloud=True, upload_interval=self.UPLOAD_INTERVAL)

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_file_paths(temporary_directory)
            packet_reader.read_packets(serial_port, filenames, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(temporary_directory, "mag.csv")

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_name="Mag", number_of_batches_to_check=1)

    def test_packet_reader_with_analog_sensor(self):
        """Test that the packet reader works with the analog sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([80])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        packet_reader = PacketReader(save_locally=True, upload_to_cloud=True, upload_interval=self.UPLOAD_INTERVAL)

        with tempfile.TemporaryDirectory() as temporary_directory:
            filenames = self._generate_file_paths(temporary_directory)
            packet_reader.read_packets(serial_port, filenames, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(temporary_directory, "analog.csv")

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_name="Analog", number_of_batches_to_check=1)


if __name__ == "__main__":
    unittest.main()
