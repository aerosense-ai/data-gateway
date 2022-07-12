import json
import os
import shutil
import struct
import tempfile
from time import time
from unittest.mock import patch

import coolname
from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient

from data_gateway.configuration import Configuration
from data_gateway.data_gateway import DataGateway
from data_gateway.dummy_serial import DummySerial
from data_gateway.persistence import TimeBatcher
from tests import LENGTH, RANDOM_BYTES, TEST_BUCKET_NAME, ZEROTH_NODE_LEADING_BYTE
from tests.base import BaseTestCase


temporary_output_directories = []


def make_random_constats_packet(packet_origin="0", packet_timestamp=None, packet_type="52"):
    """Mimic a constats packet with random data but a correct timestamp

    Used to generate initial constats packets, which should be written to the serial port in order
    to set time offsets on the packet parser prior to issuing any test data that will require the time offsets to be set
    """
    packet_timestamp = packet_timestamp or time()
    current_time = int(packet_timestamp)
    current_time_bytes = struct.pack(">i", current_time)
    leading_byte = Configuration().get_leading_byte(int(packet_origin))
    packet = b"".join((leading_byte, bytes([int(packet_type)]), LENGTH, RANDOM_BYTES[0][0:240], current_time_bytes))
    return packet


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
        cls.storage_client = GoogleCloudStorageClient()

    def test_configuration_file_is_persisted(self):
        """Test that the configuration file is persisted."""
        serial_port = DummySerial(port="test")
        packet_type = bytes([34])

        serial_port.write(data=make_random_constats_packet())
        serial_port.write(data=b"".join((ZEROTH_NODE_LEADING_BYTE, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((ZEROTH_NODE_LEADING_BYTE, packet_type, LENGTH, RANDOM_BYTES[1])))

        try:
            data_gateway = DataGateway(
                serial_port=serial_port,
                save_locally=True,
                output_directory=self._generate_temporary_output_directory_name(),
                window_size=self.WINDOW_SIZE,
                bucket_name=TEST_BUCKET_NAME,
                stop_sensors_on_exit=False,
            )

            data_gateway.start(stop_when_no_more_data_after=0.1)

            # Check configuration file is present and valid locally.
            with open(os.path.join(data_gateway.packet_reader.local_output_directory, "configuration.json")) as f:
                Configuration.from_dict(json.load(f))

        finally:
            self._delete_temporary_output_directories()

    def test_sensors_individually(self):
        """Test that the data gateway works with each sensor individually."""
        serial_port = DummySerial(port="test")

        try:
            for packet_type, sensor_name in [
                (bytes([34]), "Baros_P"),
                (bytes([34]), "Baros_T"),
                (bytes([36]), "Diff_Baros"),
                (bytes([38]), "Mics"),
                (bytes([42]), "Acc"),
                (bytes([44]), "Gyro"),
                (bytes([46]), "Mag"),
                (bytes([52]), "Constat"),
            ]:
                with self.subTest(sensor_name=sensor_name):
                    serial_port.write(data=make_random_constats_packet())
                    serial_port.write(data=b"".join((ZEROTH_NODE_LEADING_BYTE, packet_type, LENGTH, RANDOM_BYTES[0])))
                    serial_port.write(data=b"".join((ZEROTH_NODE_LEADING_BYTE, packet_type, LENGTH, RANDOM_BYTES[1])))

                    data_gateway = DataGateway(
                        serial_port,
                        save_locally=True,
                        output_directory=self._generate_temporary_output_directory_name(),
                        window_size=self.WINDOW_SIZE,
                        bucket_name=TEST_BUCKET_NAME,
                        stop_sensors_on_exit=False,
                    )

                    data_gateway.start(stop_when_no_more_data_after=0.1)

                    self._check_data_is_written_to_files(
                        data_gateway.packet_reader.local_output_directory,
                        node_id="0",
                        sensor_names=[sensor_name],
                    )

                    self._check_windows_are_uploaded_to_cloud(
                        data_gateway.packet_reader.cloud_output_directory,
                        node_id="0",
                        sensor_names=[sensor_name],
                        number_of_windows_to_check=1,
                    )

        finally:
            self._delete_temporary_output_directories()

    def test_no_data_written_if_no_time_offset(self):
        """Test that if no constats are received prior to a data-containing packet, that no data is written
        and an error is logged
        """
        serial_port = DummySerial(port="test")

        packet_type = bytes([34])
        serial_port.write(data=b"".join((ZEROTH_NODE_LEADING_BYTE, packet_type, LENGTH, RANDOM_BYTES[0])))

        data_gateway = DataGateway(
            serial_port,
            save_locally=True,
            upload_to_cloud=False,
            output_directory=self._generate_temporary_output_directory_name(),
            window_size=self.WINDOW_SIZE,
            bucket_name=TEST_BUCKET_NAME,
            stop_sensors_on_exit=False,
        )

        # TODO something to do with multiprocessing kills this patch so we can't test that a missing timestamp issues an error...
        # We wish to make the following assertation:
        # with patch("data_gateway.packet_reader.logger") as mock_logger:
        #     # <start then check the sensor data>
        #     self.assertIn(
        #         "Unable to apply time offset to packet, skipped packet parsing (origin %s, type %s)",
        #         mock_logger.method_calls[0].args[0],
        #     )

        data_gateway.start(stop_when_no_more_data_after=0.1)

        self._check_sensor_data_is_not_written_to_files(
            data_gateway.packet_reader.local_output_directory,
            node_id="0",
            sensor_names=["Abs. Baros"],
        )

    def test_data_gateway_with_connections_statistics_in_sleep_mode(self):
        """Test that the data gateway works with the connection statistics "sensor" in sleep state. Normally,
        randomly generated payloads would trigger packet loss warning in logger. Check that this warning is suppressed
        in sleep mode.
        """
        serial_port = DummySerial(port="test")

        # Set the time offset for the node
        serial_port.write(data=make_random_constats_packet())

        # Enter sleep state
        serial_port.write(data=b"".join((ZEROTH_NODE_LEADING_BYTE, bytes([56]), bytes([1]), bytes([1]))))

        packet_type = bytes([52])
        serial_port.write(data=b"".join((ZEROTH_NODE_LEADING_BYTE, packet_type, LENGTH, RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((ZEROTH_NODE_LEADING_BYTE, packet_type, LENGTH, RANDOM_BYTES[1])))

        with tempfile.TemporaryDirectory() as temporary_directory:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                upload_to_cloud=False,
                output_directory=temporary_directory,
                window_size=self.WINDOW_SIZE,
                bucket_name=TEST_BUCKET_NAME,
                stop_sensors_on_exit=False,
            )

            with patch("data_gateway.packet_reader.logger") as mock_logger:
                data_gateway.start(stop_when_no_more_data_after=0.1)

                self._check_data_is_written_to_files(
                    data_gateway.packet_reader.local_output_directory,
                    node_id="0",
                    sensor_names=["Constat"],
                )

                self.assertEqual(0, mock_logger.warning.call_count)

    def test_all_sensors_together(self):
        """Test that the data gateway works with all sensors together."""
        serial_port = DummySerial(port="test")
        packet_types = (bytes([34]), bytes([36]), bytes([38]), bytes([42]), bytes([44]), bytes([46]))
        sensor_names = ("Baros_P", "Baros_T", "Diff_Baros", "Mics", "Acc", "Gyro", "Mag")

        serial_port.write(data=make_random_constats_packet())
        for packet_type in packet_types:
            serial_port.write(data=b"".join((ZEROTH_NODE_LEADING_BYTE, packet_type, LENGTH, RANDOM_BYTES[0])))
            serial_port.write(data=b"".join((ZEROTH_NODE_LEADING_BYTE, packet_type, LENGTH, RANDOM_BYTES[1])))

        try:
            data_gateway = DataGateway(
                serial_port,
                save_locally=True,
                output_directory=self._generate_temporary_output_directory_name(),
                window_size=self.WINDOW_SIZE,
                bucket_name=TEST_BUCKET_NAME,
                stop_sensors_on_exit=False,
            )

            data_gateway.start(stop_when_no_more_data_after=0.1)

            self._check_data_is_written_to_files(
                data_gateway.packet_reader.local_output_directory,
                node_id="0",
                sensor_names=sensor_names,
            )

            self._check_windows_are_uploaded_to_cloud(
                data_gateway.packet_reader.cloud_output_directory,
                node_id="0",
                sensor_names=sensor_names,
                number_of_windows_to_check=1,
            )

        finally:
            self._delete_temporary_output_directories()

    def test_with_two_nodes(self):
        """Test receiving data from different sensors on two separate nodes."""
        serial_port = DummySerial(port="test")

        packet_types = {
            "0": (bytes([34]), bytes([36])),
            "1": (bytes([38]), bytes([42]), bytes([44]), bytes([46])),
        }

        sensor_names = {
            "0": ("Baros_P", "Baros_T", "Diff_Baros"),
            "1": ("Mics", "Acc", "Gyro", "Mag"),
        }

        # Give both nodes the default node configuration.
        configuration = Configuration()
        configuration.nodes["1"] = configuration.nodes["0"]

        # Set the time offset for the nodes
        serial_port.write(data=make_random_constats_packet(packet_origin="0"))
        serial_port.write(data=make_random_constats_packet(packet_origin="1"))

        for packet_type in packet_types["0"]:
            serial_port.write(data=b"".join((ZEROTH_NODE_LEADING_BYTE, packet_type, LENGTH, RANDOM_BYTES[0])))
            serial_port.write(data=b"".join((ZEROTH_NODE_LEADING_BYTE, packet_type, LENGTH, RANDOM_BYTES[1])))

        first_node_leading_byte = configuration.get_leading_byte(node_id="1")

        for packet_type in packet_types["1"]:
            serial_port.write(data=b"".join((first_node_leading_byte, packet_type, LENGTH, RANDOM_BYTES[0])))
            serial_port.write(data=b"".join((first_node_leading_byte, packet_type, LENGTH, RANDOM_BYTES[1])))

        try:
            with patch("data_gateway.data_gateway.DataGateway._load_configuration", return_value=configuration):
                data_gateway = DataGateway(
                    serial_port,
                    save_locally=True,
                    output_directory=self._generate_temporary_output_directory_name(),
                    window_size=self.WINDOW_SIZE,
                    bucket_name=TEST_BUCKET_NAME,
                    stop_sensors_on_exit=False,
                )

            data_gateway.start(stop_when_no_more_data_after=0.1)

            for node_id in packet_types.keys():
                self._check_data_is_written_to_files(
                    data_gateway.packet_reader.local_output_directory,
                    node_id=node_id,
                    sensor_names=sensor_names[node_id],
                )

                self._check_windows_are_uploaded_to_cloud(
                    data_gateway.packet_reader.cloud_output_directory,
                    node_id=node_id,
                    sensor_names=sensor_names[node_id],
                    number_of_windows_to_check=1,
                )

        finally:
            self._delete_temporary_output_directories()

    def _generate_temporary_output_directory_name(self):
        """Generate a temporary output directory name. A regular `tempfile` temporary directory cannot be used as, on
        Windows, the path will contain a colon, which is invalid in a cloud path. The output directories are needed
        for both local paths and cloud paths.

        :return str:
        """
        directory_name = coolname.generate_slug(2)
        temporary_output_directories.append(directory_name)
        return directory_name

    def _delete_temporary_output_directories(self):
        """Delete any temporary directories with names generated by `self._generate_temporary_output_directory_name`.

        :return None:
        """
        for directory in temporary_output_directories:
            try:
                shutil.rmtree(directory)
            except FileNotFoundError:
                pass

    def _check_windows_are_uploaded_to_cloud(
        self,
        output_directory,
        node_id,
        sensor_names,
        number_of_windows_to_check=5,
    ):
        """Check that non-trivial windows from a packet reader for a particular sensor are uploaded to cloud storage.

        :return None:
        """
        window_paths = [
            blob.name
            for blob in self.storage_client.scandir(
                cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, output_directory)
            )
            if not blob.name.endswith("configuration.json")
        ]

        self.assertTrue(len(window_paths) >= number_of_windows_to_check)

        for path in window_paths:
            data = json.loads(
                self.storage_client.download_as_string(cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, path))
            )

            for name in sensor_names:
                lines = data[node_id][name]
                self.assertTrue(len(lines[0]) > 1)

    def _check_data_is_written_to_files(self, output_directory, node_id, sensor_names):
        """Check that non-trivial data is written to the given file.

        :return None:
        """
        windows = [file for file in os.listdir(output_directory) if file.startswith(TimeBatcher._file_prefix)]

        self.assertTrue(len(windows) > 0)

        for window in windows:
            with open(os.path.join(output_directory, window)) as f:
                data = json.load(f)

                for name in sensor_names:
                    lines = data[node_id][name]
                    self.assertTrue(len(lines[0]) > 1)

    def _check_sensor_data_is_not_written_to_files(self, output_directory, node_id, sensor_names):
        """Check that non-trivial data is written to the given file.

        :return None:
        """
        windows = [file for file in os.listdir(output_directory) if file.startswith(TimeBatcher._file_prefix)]

        self.assertTrue(len(windows) > 0)

        for window in windows:
            with open(os.path.join(output_directory, window)) as f:
                data = json.load(f)

                for name in sensor_names:
                    self.assertNotIn(name, data.get(node_id, {}))
