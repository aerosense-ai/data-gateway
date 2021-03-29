import json
import os
import tempfile
import unittest
from octue.utils.cloud import storage
from octue.utils.cloud.storage.client import GoogleCloudStorageClient

from data_gateway import exceptions
from data_gateway.reader.configuration import Configuration
from data_gateway.reader.packet_reader import PacketReader
from dummy_serial.dummy_serial import DummySerial
from tests import LENGTH, PACKET_KEY, RANDOM_BYTES, TEST_BUCKET_NAME, TEST_PROJECT_NAME


class TestPacketReader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.BATCH_INTERVAL = 10
        cls.storage_client = GoogleCloudStorageClient(project_name=TEST_PROJECT_NAME)

    def _check_batches_are_uploaded_to_cloud(self, packet_reader, sensor_names, number_of_batches_to_check=5):
        """Check that non-trivial batches from a packet reader for a particular sensor are uploaded to cloud storage."""
        number_of_batches = packet_reader.uploader._batch_number
        self.assertTrue(number_of_batches > 0)

        for i in range(number_of_batches_to_check):
            data = json.loads(
                self.storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(
                        packet_reader.uploader.output_directory,
                        packet_reader.uploader._session_subdirectory,
                        f"window-{i}.json",
                    ),
                )
            )

            for name in sensor_names:
                lines = data[name].split("\n")
                self.assertTrue(len(lines) > 1)
                self.assertTrue(len(lines[0].split(",")) > 1)

    def _check_data_is_written_to_files(self, packet_reader, temporary_directory, sensor_names):
        """Check that non-trivial data is written to the given file."""
        batch_directory = os.path.join(temporary_directory, packet_reader.writer._session_subdirectory)
        batches = [file for file in os.listdir(batch_directory) if file.startswith(packet_reader.writer._batch_prefix)]
        self.assertTrue(len(batches) > 0)

        for batch in batches:
            with open(os.path.join(batch_directory, batch)) as f:
                data = json.load(f)

                for name in sensor_names:
                    lines = data[name].split("\n")
                    self.assertTrue(len(lines) > 1)
                    self.assertTrue(len(lines[0].split(",")) > 1)

    def test_error_is_raised_if_unknown_sensor_type_packet_is_received(self):
        """Test that an `UnknownPacketTypeException` is raised if an unknown sensor type packet is received."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([0])

        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            with self.assertRaises(exceptions.UnknownPacketTypeException):
                packet_reader.read_packets(serial_port, stop_when_no_more_data=True)

    def test_configuration_file_is_persisted(self):
        """Test that the configuration file is persisted."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([34])

        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)

            configuration_path = os.path.join(temporary_directory, "configuration.json")

            # Check configuration file is present and valid locally.
            with open(configuration_path) as f:
                Configuration.from_dict(json.load(f))

        # Check configuration file is present and valid on the cloud.
        configuration = self.storage_client.download_as_string(
            bucket_name=TEST_BUCKET_NAME,
            path_in_bucket=storage.path.join(packet_reader.uploader.output_directory, "configuration.json"),
        )

        # Test configuration is valid.
        Configuration.from_dict(json.loads(configuration))

    def test_packet_reader_with_baros_p_sensor(self):
        """Test that the packet reader works with the Baro_P sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([34])

        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(packet_reader, temporary_directory, sensor_names=["Baros_P"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Baros_P"], number_of_batches_to_check=1)

    def test_packet_reader_with_baros_t_sensor(self):
        """Test that the packet reader works with the Baro_T sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([34])

        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(packet_reader, temporary_directory, sensor_names=["Baros_T"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Baros_T"], number_of_batches_to_check=1)

    def test_packet_reader_with_mic_sensor(self):
        """Test that the packet reader works with the mic sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([54])

        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(packet_reader, temporary_directory, sensor_names=["Mics"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Mics"], number_of_batches_to_check=1)

    def test_packet_reader_with_acc_sensor(self):
        """Test that the packet reader works with the acc sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([74])

        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(packet_reader, temporary_directory, sensor_names=["Acc"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Acc"], number_of_batches_to_check=1)

    def test_packet_reader_with_gyro_sensor(self):
        """Test that the packet reader works with the gyro sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([76])

        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(packet_reader, temporary_directory, sensor_names=["Gyro"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Gyro"], number_of_batches_to_check=1)

    def test_packet_reader_with_mag_sensor(self):
        """Test that the packet reader works with the mag sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([78])

        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(packet_reader, temporary_directory, sensor_names=["Mag"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Mag"], number_of_batches_to_check=1)

    def test_packet_reader_with_analog_sensor(self):
        """Test that the packet reader works with the analog sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([80])

        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
            self._check_data_is_written_to_files(packet_reader, temporary_directory, sensor_names=["Analog"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Analog"], number_of_batches_to_check=1)

    def test_all_sensors_together(self):
        """Test that the packet reader works with all sensors together."""
        serial_port = DummySerial(port="test")
        sensor_types = bytes([34]), bytes([54]), bytes([74]), bytes([76]), bytes([78]), bytes([80])
        sensor_names = "Baros_P", "Baros_T", "Mics", "Acc", "Gyro", "Mag", "Analog"

        for sensor_type in sensor_types:
            serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[0])))
            serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=True,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            packet_reader.read_packets(serial_port, stop_when_no_more_data=True)

            self._check_data_is_written_to_files(packet_reader, temporary_directory, sensor_names=sensor_names)

        self._check_batches_are_uploaded_to_cloud(
            packet_reader, sensor_names=sensor_names, number_of_batches_to_check=1
        )
