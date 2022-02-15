import json
import os
import shutil
from unittest.mock import patch

import coolname
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

    def setUp(self):
        """Create a uniquely-named output directory."""
        self.output_directory = coolname.generate_slug(2)

    def tearDown(self):
        """Delete the output directory created in `setUp`."""
        try:
            shutil.rmtree(self.output_directory)
        except FileNotFoundError:
            pass

    def test_configuration_file_is_persisted(self):
        """Test that the configuration file is persisted."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([34])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        data_gateway = DataGateway(
            serial_port=serial_port,
            save_locally=True,
            output_directory=self.output_directory,
            window_size=self.WINDOW_SIZE,
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
        )

        data_gateway.start(stop_when_no_more_data_after=0.1)

        # Check configuration file is present and valid locally.
        with open(os.path.join(data_gateway.packet_reader.local_output_directory, "configuration.json")) as f:
            Configuration.from_dict(json.load(f))

        # Check configuration file is present and valid on the cloud.
        configuration = self.storage_client.download_as_string(
            bucket_name=TEST_BUCKET_NAME,
            path_in_bucket=storage.path.join(data_gateway.packet_reader.cloud_output_directory, "configuration.json"),
        )

        Configuration.from_dict(json.loads(configuration))

    def test_data_gateway_with_baros_p_sensor(self):
        """Test that the packet reader works with the "Baros_P" sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([34])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        data_gateway = DataGateway(
            serial_port,
            save_locally=True,
            output_directory=coolname.generate_slug(2),
            window_size=self.WINDOW_SIZE,
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
        )

        data_gateway.start(stop_when_no_more_data_after=0.1)
        self._check_data_is_written_to_files(
            data_gateway.packet_reader.local_output_directory, sensor_names=["Baros_P"]
        )

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader.cloud_output_directory,
            sensor_names=["Baros_P"],
            number_of_windows_to_check=1,
        )

    def test_data_gateway_with_baros_t_sensor(self):
        """Test that the packet reader works with the Baro_T sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([34])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        data_gateway = DataGateway(
            serial_port,
            save_locally=True,
            output_directory=self.output_directory,
            window_size=self.WINDOW_SIZE,
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
        )
        data_gateway.start(stop_when_no_more_data_after=0.1)
        self._check_data_is_written_to_files(
            data_gateway.packet_reader.local_output_directory, sensor_names=["Baros_T"]
        )

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader.cloud_output_directory,
            sensor_names=["Baros_T"],
            number_of_windows_to_check=1,
        )

    def test_data_gateway_with_diff_baros_sensor(self):
        """Test that the packet reader works with the Diff_Baros sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([36])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        data_gateway = DataGateway(
            serial_port,
            save_locally=True,
            output_directory=self.output_directory,
            window_size=self.WINDOW_SIZE,
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
        )
        data_gateway.start(stop_when_no_more_data_after=0.1)

        self._check_data_is_written_to_files(
            data_gateway.packet_reader.local_output_directory,
            sensor_names=["Diff_Baros"],
        )

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader.cloud_output_directory,
            sensor_names=["Diff_Baros"],
            number_of_windows_to_check=1,
        )

    def test_data_gateway_with_mic_sensor(self):
        """Test that the packet reader works with the mic sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([38])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        data_gateway = DataGateway(
            serial_port,
            save_locally=True,
            output_directory=self.output_directory,
            window_size=self.WINDOW_SIZE,
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
        )
        data_gateway.start(stop_when_no_more_data_after=0.1)
        self._check_data_is_written_to_files(data_gateway.packet_reader.local_output_directory, sensor_names=["Mics"])

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader.cloud_output_directory,
            sensor_names=["Mics"],
            number_of_windows_to_check=1,
        )

    def test_data_gateway_with_acc_sensor(self):
        """Test that the packet reader works with the acc sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([42])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        data_gateway = DataGateway(
            serial_port,
            save_locally=True,
            output_directory=self.output_directory,
            window_size=self.WINDOW_SIZE,
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
        )
        data_gateway.start(stop_when_no_more_data_after=0.1)

        self._check_data_is_written_to_files(data_gateway.packet_reader.local_output_directory, sensor_names=["Acc"])

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader.cloud_output_directory,
            sensor_names=["Acc"],
            number_of_windows_to_check=1,
        )

    def test_data_gateway_with_gyro_sensor(self):
        """Test that the packet reader works with the gyro sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([44])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        data_gateway = DataGateway(
            serial_port,
            save_locally=True,
            output_directory=self.output_directory,
            window_size=self.WINDOW_SIZE,
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
        )
        data_gateway.start(stop_when_no_more_data_after=0.1)

        self._check_data_is_written_to_files(data_gateway.packet_reader.local_output_directory, sensor_names=["Gyro"])

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader.cloud_output_directory,
            sensor_names=["Gyro"],
            number_of_windows_to_check=1,
        )

    def test_data_gateway_with_mag_sensor(self):
        """Test that the packet reader works with the mag sensor."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([46])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        data_gateway = DataGateway(
            serial_port,
            save_locally=True,
            output_directory=self.output_directory,
            window_size=self.WINDOW_SIZE,
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
        )
        data_gateway.start(stop_when_no_more_data_after=0.1)
        self._check_data_is_written_to_files(data_gateway.packet_reader.local_output_directory, sensor_names=["Mag"])

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader.cloud_output_directory,
            sensor_names=["Mag"],
            number_of_windows_to_check=1,
        )

    def test_data_gateway_with_connections_statistics(self):
        """Test that the packet reader works with the connection statistics "sensor"."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([52])

        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((PACKET_KEY, packet_type, LENGTH, RANDOM_BYTES[1])))

        data_gateway = DataGateway(
            serial_port,
            save_locally=True,
            output_directory=self.output_directory,
            window_size=self.WINDOW_SIZE,
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
        )
        data_gateway.start(stop_when_no_more_data_after=0.1)

        self._check_data_is_written_to_files(
            data_gateway.packet_reader.local_output_directory, sensor_names=["Constat"]
        )

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader.cloud_output_directory,
            sensor_names=["Constat"],
            number_of_windows_to_check=1,
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

        data_gateway = DataGateway(
            serial_port,
            save_locally=True,
            upload_to_cloud=False,
            output_directory=self.output_directory,
            window_size=self.WINDOW_SIZE,
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
        )

        with patch("data_gateway.packet_reader.logger") as mock_logger:
            data_gateway.start(stop_when_no_more_data_after=0.1)

        self._check_data_is_written_to_files(
            data_gateway.packet_reader.local_output_directory, sensor_names=["Constat"]
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

        data_gateway = DataGateway(
            serial_port,
            save_locally=True,
            output_directory=self.output_directory,
            window_size=self.WINDOW_SIZE,
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
        )
        data_gateway.start(stop_when_no_more_data_after=0.1)

        self._check_data_is_written_to_files(
            data_gateway.packet_reader.local_output_directory, sensor_names=sensor_names
        )

        self._check_windows_are_uploaded_to_cloud(
            data_gateway.packet_reader.cloud_output_directory,
            sensor_names=sensor_names,
            number_of_windows_to_check=1,
        )

    def _check_windows_are_uploaded_to_cloud(self, output_directory, sensor_names, number_of_windows_to_check=5):
        """Check that non-trivial windows from a packet reader for a particular sensor are uploaded to cloud storage."""
        window_paths = [
            blob.name
            for blob in self.storage_client.scandir(
                cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, output_directory)
            )
            if not blob.name.endswith("configuration.json")
        ]

        self.assertTrue(len(window_paths) >= number_of_windows_to_check)

        for path in window_paths:
            data = json.loads(self.storage_client.download_as_string(bucket_name=TEST_BUCKET_NAME, path_in_bucket=path))

            for name in sensor_names:
                lines = data["sensor_data"][name]
                self.assertTrue(len(lines[0]) > 1)

    def _check_data_is_written_to_files(self, output_directory, sensor_names):
        """Check that non-trivial data is written to the given file."""
        windows = [file for file in os.listdir(output_directory) if file.startswith(TimeBatcher._file_prefix)]
        self.assertTrue(len(windows) > 0)

        for window in windows:
            with open(os.path.join(output_directory, window)) as f:
                data = json.load(f)

                for name in sensor_names:
                    lines = data["sensor_data"][name]
                    self.assertTrue(len(lines[0]) > 1)
