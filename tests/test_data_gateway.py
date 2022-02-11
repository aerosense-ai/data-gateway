import json
import os
import tempfile
from unittest.mock import patch

from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient

from data_gateway.configuration import Configuration
from data_gateway.data_gateway import DataGateway
from data_gateway.dummy_serial import DummySerial
from data_gateway.persistence import TimeBatcher
from tests import LENGTH, PACKET_KEY, RANDOM_BYTES, TEST_BUCKET_NAME, TEST_PROJECT_NAME
from tests.base import BaseTestCase


class TestDataGateway(BaseTestCase):
    """Test `DataGateway` with different sensors. NOTE: The payloads are generated randomly. Consequently, two
    consecutive packets are extremely unlikely to have consecutive timestamps. This will trigger lost packet warning
    during tests.
    """

    @classmethod
    def setUpClass(cls):
        """Set up the class with a window size and a Google Cloud Storage client.

        :return None:
        """
        cls.WINDOW_SIZE = 10
        cls.storage_client = GoogleCloudStorageClient(project_name=TEST_PROJECT_NAME)

    def test_error_is_logged_if_unknown_sensor_type_packet_is_received(self):
        """Test that an error is logged if an unknown sensor type packet is received."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([0])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port=serial_port,
                save_locally=True,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            with self.assertLogs() as logging_context:
                data_gateway.start(stop_when_no_more_data=True)

        self.assertIn("Received packet with unknown type: 0", logging_context.output[1])

    def test_configuration_file_is_persisted(self):
        """Test that the configuration file is persisted."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([34])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port=serial_port,
                save_locally=True,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )

            data_gateway.start(stop_when_no_more_data=True)

            configuration_path = os.path.join(
                temporary_directory, data_gateway.packet_reader.session_subdirectory, "configuration.json"
            )

            # Check configuration file is present and valid locally.
            with open(configuration_path) as f:
                Configuration.from_dict(json.load(f))

        # Check configuration file is present and valid on the cloud.
        configuration = self.storage_client.download_as_string(
            bucket_name=TEST_BUCKET_NAME,
            path_in_bucket=storage.path.join(
                data_gateway.packet_reader.uploader.output_directory,
                data_gateway.packet_reader.session_subdirectory,
                "configuration.json",
            ),
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
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )

            with self.assertLogs() as logging_context:
                data_gateway.start(stop_when_no_more_data=True)
                self.assertIn("Handle error", logging_context.output[1])

    def test_update_handles(self):
        """Test that the handles can be updated."""
        serial_port = DummySerial(port="test")

        # Set packet type to handles update packet.
        packet_type = bytes([255])

        # Set first two bytes of payload to correct range for updating handles.
        payload = bytearray(RANDOM_BYTES[0])
        payload[0:1] = int(0).to_bytes(1, "little")
        payload[2:3] = int(26).to_bytes(1, "little")
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, payload)))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                upload_to_cloud=False,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )

            with self.assertLogs() as logging_context:
                data_gateway.start(stop_when_no_more_data=True)
                self.assertIn("Successfully updated handles", logging_context.output[1])

    def test_data_gateway_with_baros_p_sensor(self):
        """Test that the packet reader works with the Baro_P sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([34])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )

            data_gateway.start(stop_when_no_more_data=False)
            self._check_data_is_written_to_files(temporary_directory, sensor_names=["Baros_P"])

        self._check_windows_are_uploaded_to_cloud(
            temporary_directory, sensor_names=["Baros_P"], number_of_windows_to_check=1
        )

    def test_data_gateway_with_baros_t_sensor(self):
        """Test that the packet reader works with the Baro_T sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([34])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            data_gateway.start(stop_when_no_more_data=True)
            self._check_data_is_written_to_files(
                data_gateway.packet_reader, temporary_directory, sensor_names=["Baros_T"]
            )

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader, sensor_names=["Baros_T"], number_of_windows_to_check=1
        )

    def test_data_gateway_with_diff_baros_sensor(self):
        """Test that the packet reader works with the Diff_Baros sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([36])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            data_gateway.start(stop_when_no_more_data=True)
            self._check_data_is_written_to_files(
                data_gateway.packet_reader, temporary_directory, sensor_names=["Diff_Baros"]
            )

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader,
            sensor_names=["Diff_Baros"],
            number_of_windows_to_check=1,
        )

    def test_data_gateway_with_mic_sensor(self):
        """Test that the packet reader works with the mic sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([38])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            data_gateway.start(stop_when_no_more_data=True)
            self._check_data_is_written_to_files(data_gateway.packet_reader, temporary_directory, sensor_names=["Mics"])

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader, sensor_names=["Mics"], number_of_windows_to_check=1
        )

    def test_data_gateway_with_acc_sensor(self):
        """Test that the packet reader works with the acc sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([42])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            data_gateway.start(stop_when_no_more_data=True)
            self._check_data_is_written_to_files(data_gateway.packet_reader, temporary_directory, sensor_names=["Acc"])

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader, sensor_names=["Acc"], number_of_windows_to_check=1
        )

    def test_data_gateway_with_gyro_sensor(self):
        """Test that the packet reader works with the gyro sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([44])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            data_gateway.start(stop_when_no_more_data=True)
            self._check_data_is_written_to_files(data_gateway.packet_reader, temporary_directory, sensor_names=["Gyro"])

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader, sensor_names=["Gyro"], number_of_windows_to_check=1
        )

    def test_data_gateway_with_mag_sensor(self):
        """Test that the packet reader works with the mag sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([46])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            data_gateway.start(stop_when_no_more_data=True)
            self._check_data_is_written_to_files(data_gateway.packet_reader, temporary_directory, sensor_names=["Mag"])

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader, sensor_names=["Mag"], number_of_windows_to_check=1
        )

    def test_data_gateway_with_connections_statistics(self):
        """Test that the packet reader works with the connection statistics "sensor"."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([52])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            data_gateway.start(stop_when_no_more_data=True)

            self._check_data_is_written_to_files(
                data_gateway.packet_reader, temporary_directory, sensor_names=["Constat"]
            )

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader, sensor_names=["Constat"], number_of_windows_to_check=1
        )

    def test_data_gateway_with_connections_statistics_in_sleep_mode(self):
        """Test that the packet reader works with the connection statistics "sensor" in sleep state. Normally,
        randomly generated payloads would trigger packet loss warning in logger. Check that this warning is suppressed
        in sleep mode.
        """
        serial_port = DummySerial(port="test")
        # Enter sleep state
        serial_port.write(data=b"".join((PACKET_KEY, bytes([56]), bytes([1]), bytes([1]))))

        packet_type = bytes([52])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                upload_to_cloud=False,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )

            with patch("data_gateway.packet_reader.logger") as mock_logger:
                data_gateway.start(stop_when_no_more_data=True)

            self._check_data_is_written_to_files(
                data_gateway.packet_reader, temporary_directory, sensor_names=["Constat"]
            )
            self.assertEqual(0, mock_logger.warning.call_count)

    def test_all_sensors_together(self):
        """Test that the packet reader works with all sensors together."""
        serial_port = DummySerial(port="test")
        packet_types = (bytes([34]), bytes([36]), bytes([38]), bytes([42]), bytes([44]), bytes([46]))
        sensor_names = ("Baros_P", "Baros_T", "Diff_Baros", "Mics", "Acc", "Gyro", "Mag")

        for packet_type in packet_types:
            serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
            serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )
            data_gateway.start(stop_when_no_more_data=True)

            self._check_data_is_written_to_files(
                data_gateway.packet_reader, temporary_directory, sensor_names=sensor_names
            )

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader,
            sensor_names=sensor_names,
            number_of_windows_to_check=1,
        )

    def test_data_gateway_with_info_packets(self):
        """Test that the packet reader works with info packets."""
        serial_port = DummySerial(port="test")

        packet_types = [bytes([40]), bytes([54]), bytes([56]), bytes([58])]

        payloads = [
            [bytes([1]), bytes([2]), bytes([3])],
            [bytes([0]), bytes([1]), bytes([2]), bytes([3])],
            [bytes([0]), bytes([1])],
            [bytes([0])],
        ]

        for index, packet_type in enumerate(packet_types):
            for payload in payloads[index]:
                serial_port.write(data=b"".join((PACKET_KEY, packet_type, bytes([1]), payload)))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                upload_to_cloud=False,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                project_name=TEST_PROJECT_NAME,
                bucket_name=TEST_BUCKET_NAME,
            )

            with self.assertLogs() as logging_context:
                data_gateway.start(stop_when_no_more_data=True)

                log_messages_combined = "\n".join(logging_context.output)

                for message in [
                    "Microphone data reading done",
                    "Microphone data erasing done",
                    "Microphones started ",
                    "Command declined, Bad block detection ongoing",
                    "Command declined, Task already registered, cannot register again",
                    "Command declined, Task is not registered, cannot de-register",
                    "Command declined, Connection Parameter update unfinished",
                    "\nExiting sleep\n",
                    "\nEntering sleep\n",
                    "Battery info",
                    "Voltage : 0.000000V\n Cycle count: 0.000000\nState of charge: 0.000000%",
                ]:
                    self.assertIn(message, log_messages_combined)

    def _check_windows_are_uploaded_to_cloud(self, output_directory, sensor_names, number_of_windows_to_check=5):
        """Check that non-trivial windows from a packet reader for a particular sensor are uploaded to cloud storage."""
        window_paths = [
            blob.name
            for blob in self.storage_client.scandir(
                cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, *output_directory.split(os.path.pathsep))
            )
        ]

        self.assertTrue(len(window_paths) >= number_of_windows_to_check)

        for path in window_paths:
            data = json.loads(self.storage_client.download_as_string(bucket_name=TEST_BUCKET_NAME, path_in_bucket=path))

            for name in sensor_names:
                lines = data["sensor_data"][name]
                self.assertTrue(len(lines[0]) > 1)

    def _check_data_is_written_to_files(self, output_directory, sensor_names):
        """Check that non-trivial data is written to the given file."""
        session_subdirectory = os.listdir(output_directory)[0]
        window_directory = os.path.join(output_directory, session_subdirectory)
        windows = [file for file in os.listdir(window_directory) if file.startswith(TimeBatcher._file_prefix)]
        self.assertTrue(len(windows) > 0)

        for window in windows:
            with open(os.path.join(window_directory, window)) as f:
                data = json.load(f)

                for name in sensor_names:
                    lines = data["sensor_data"][name]
                    self.assertTrue(len(lines[0]) > 1)
