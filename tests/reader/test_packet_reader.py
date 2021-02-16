import json
import os
import tempfile
import unittest
from gcloud_storage_emulator.server import create_server
from google.cloud import storage
from octue.utils.cloud.credentials import GCPCredentialsManager
from octue.utils.cloud.persistence import GoogleCloudStorageClient

from data_gateway.reader.configuration import Configuration
from data_gateway.reader.packet_reader import PacketReader
from dummy_serial.dummy_serial import DummySerial
from dummy_serial.utils import random_bytes


class TestPacketReader(unittest.TestCase):
    """Testing operation of the PacketReader class"""

    TEST_PROJECT_NAME = os.environ["TEST_PROJECT_NAME"]
    TEST_BUCKET_NAME = os.environ["TEST_BUCKET_NAME"]
    PACKET_KEY = Configuration().packet_key.to_bytes(1, "little")
    LENGTH = bytes([244])
    BATCH_INTERVAL = 10
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

    def _check_batches_are_uploaded_to_cloud(self, packet_reader, sensor_names, number_of_batches_to_check=5):
        """Check that non-trivial batches from a packet reader for a particular sensor are uploaded to cloud storage."""
        number_of_batches = packet_reader.uploader._batch_number
        self.assertTrue(number_of_batches > 0)

        client = GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME)

        for i in range(number_of_batches_to_check):
            data = json.loads(
                client.download_as_string(
                    bucket_name=self.TEST_BUCKET_NAME,
                    path_in_bucket=f"{packet_reader.uploader.output_directory}/batch-{i}.json",
                )
            )

            for name in sensor_names:
                lines = data[name].split("\n")
                self.assertTrue(len(lines) > 1)
                self.assertTrue(len(lines[0].split(",")) > 1)

    def _check_data_is_written_to_files(self, temporary_directory, sensor_names):
        """Check that non-trivial data is written to the given file."""
        batches = [file for file in os.listdir(temporary_directory) if file.startswith("batch")]
        self.assertTrue(len(batches) > 0)

        for batch in batches:
            with open(os.path.join(temporary_directory, batch)) as f:
                data = json.load(f)

                for name in sensor_names:
                    lines = data[name].split("\n")
                    self.assertTrue(len(lines) > 1)
                    self.assertTrue(len(lines[0].split(",")) > 1)

    def test_configuration_file_is_persisted(self):
        """Test that the configuration file is persisted"""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([34])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=self.TEST_PROJECT_NAME,
                bucket_name=self.TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)

            configuration_path = os.path.join(temporary_directory, "configuration.json")

            # Check configuration file is present and valid locally.
            with open(configuration_path) as f:
                Configuration.from_dict(json.load(f))

        # Check configuration file is present and valid on the cloud.
        configuration = GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME).download_as_string(
            bucket_name=self.TEST_BUCKET_NAME,
            path_in_bucket=f"{packet_reader.uploader.output_directory}/configuration.json",
        )

        Configuration.from_dict(json.loads(configuration))

    def test_packet_reader_with_baro_sensor(self):
        """Test that the packet reader works with the baro sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([34])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=self.TEST_PROJECT_NAME,
                bucket_name=self.TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(temporary_directory, sensor_names=["Baros"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Baros"], number_of_batches_to_check=1)

    def test_packet_reader_with_mic_sensor(self):
        """Test that the packet reader works with the mic sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([54])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=self.TEST_PROJECT_NAME,
                bucket_name=self.TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(temporary_directory, sensor_names=["Mics"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Mics"], number_of_batches_to_check=1)

    def test_packet_reader_with_acc_sensor(self):
        """Test that the packet reader works with the acc sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([74])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=self.TEST_PROJECT_NAME,
                bucket_name=self.TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(temporary_directory, sensor_names=["Acc"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Acc"], number_of_batches_to_check=1)

    def test_packet_reader_with_gyro_sensor(self):
        """Test that the packet reader works with the gyro sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([76])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=self.TEST_PROJECT_NAME,
                bucket_name=self.TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(temporary_directory, sensor_names=["Gyro"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Gyro"], number_of_batches_to_check=1)

    def test_packet_reader_with_mag_sensor(self):
        """Test that the packet reader works with the mag sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([78])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=self.TEST_PROJECT_NAME,
                bucket_name=self.TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(temporary_directory, sensor_names=["Mag"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Mag"], number_of_batches_to_check=1)

    def test_packet_reader_with_analog_sensor(self):
        """Test that the packet reader works with the analog sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([80])

        for _ in range(2):
            serial_port.write(data=self.PACKET_KEY + sensor_type + self.LENGTH + random_bytes(256))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=self.TEST_PROJECT_NAME,
                bucket_name=self.TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(temporary_directory, sensor_names=["Analog"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Analog"], number_of_batches_to_check=1)


if __name__ == "__main__":
    unittest.main()
