import json
import os
import tempfile
from unittest.mock import patch
from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient

from data_gateway import exceptions
from data_gateway.reader.configuration import Configuration
from data_gateway.reader.packet_reader import PacketReader
from dummy_serial.dummy_serial import DummySerial
from tests import LENGTH, PACKET_KEY, RANDOM_BYTES, TEST_BUCKET_NAME, TEST_PROJECT_NAME
from tests.base import BaseTestCase


class TestPacketReader(BaseTestCase):
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
                lines = data["sensor_data"][name]
                self.assertTrue(len(lines[0]) > 1)

    def _check_data_is_written_to_files(self, packet_reader, temporary_directory, sensor_names):
        """Check that non-trivial data is written to the given file."""
        batch_directory = os.path.join(temporary_directory, packet_reader.writer._session_subdirectory)
        batches = [file for file in os.listdir(batch_directory) if file.startswith(packet_reader.writer._batch_prefix)]
        self.assertTrue(len(batches) > 0)

        for batch in batches:
            with open(os.path.join(batch_directory, batch)) as f:
                data = json.load(f)

                for name in sensor_names:
                    lines = data["sensor_data"][name]
                    self.assertTrue(len(lines[0]) > 1)

    def test_error_is_raised_if_unknown_sensor_type_packet_is_received(self):
        """Test that an `UnknownPacketTypeException` is raised if an unknown sensor type packet is received."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([0])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

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
        packet_type = bytes([34])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

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

    def test_update_handles_fails_if_start_and_end_handles_are_incorrect(self):
        """Test that an error is raised if the start and end handles are incorrect when trying to update handles."""
        serial_port = DummySerial(port="test")

        # Set packet type to handles update packet.
        packet_type = bytes([255])

        # Set first two bytes of payload to incorrect range for updating handles.
        payload = bytearray(RANDOM_BYTES[0])
        payload[0:1] = int(0).to_bytes(1, "little")
        payload[2:3] = int(255).to_bytes(1, "little")
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, payload)))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=False,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )

            with patch("data_gateway.reader.packet_reader.logger") as mock_logger:
                packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
                self.assertIn("Handle error", mock_logger.method_calls[0].args[0])

    def test_update_handles(self):
        """Test that the handles can be updated."""
        serial_port = DummySerial(port="test")

        # Set packet type to handles update packet.
        packet_type = bytes([255])

        # Set first two bytes of payload to correct range for updating handles.
        payload = bytearray(RANDOM_BYTES[0])
        payload[0:1] = int(0).to_bytes(1, "little")
        payload[2:3] = int(20).to_bytes(1, "little")
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, payload)))

        with tempfile.TemporaryDirectory() as temporary_directory:
            packet_reader = PacketReader(
                save_locally=True,
                upload_to_cloud=False,
                output_directory=temporary_directory,
                batch_interval=self.BATCH_INTERVAL,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )

            with patch("data_gateway.reader.packet_reader.logger") as mock_logger:
                packet_reader.read_packets(serial_port, stop_when_no_more_data=True)
                self.assertIn("Successfully updated handles", mock_logger.method_calls[0].args[0])

    def test_packet_reader_with_baros_p_sensor(self):
        """Test that the packet reader works with the Baro_P sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([34])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

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
        packet_type = bytes([34])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

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
        packet_type = bytes([38])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

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
        packet_type = bytes([42])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

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
        packet_type = bytes([44])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

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
        packet_type = bytes([46])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

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
        packet_type = bytes([48])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

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
            self._check_data_is_written_to_files(packet_reader, temporary_directory, sensor_names=["Analog Vbat"])

        self._check_batches_are_uploaded_to_cloud(
            packet_reader, sensor_names=["Analog Vbat"], number_of_batches_to_check=1
        )

    def test_packet_reader_with_connections_statistics(self):
        """Test that the packet reader works with the connection statistics "sensor"."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([52])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

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

            self._check_data_is_written_to_files(packet_reader, temporary_directory, sensor_names=["Constat"])

        self._check_batches_are_uploaded_to_cloud(packet_reader, sensor_names=["Constat"], number_of_batches_to_check=1)

    def test_all_sensors_together(self):
        """Test that the packet reader works with all sensors together."""
        serial_port = DummySerial(port="test")
        packet_types = (bytes([34]), bytes([38]), bytes([42]), bytes([44]), bytes([46]), bytes([48]), bytes([52]))
        sensor_names = ("Baros_P", "Baros_T", "Mics", "Acc", "Gyro", "Mag", "Analog Vbat", "Constat")

        for packet_type in packet_types:
            serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
            serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

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
