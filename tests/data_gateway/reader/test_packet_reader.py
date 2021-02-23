import json
import os
import tempfile
import unittest
from gcloud_storage_emulator.server import create_server
from octue.utils.cloud.persistence import GoogleCloudStorageClient

from data_gateway.reader.configuration import Configuration
from data_gateway.reader.packet_reader import PacketReader
from dummy_serial.dummy_serial import DummySerial


class TestPacketReader(unittest.TestCase):
    TEST_PROJECT_NAME = os.environ["TEST_PROJECT_NAME"]
    TEST_BUCKET_NAME = os.environ["TEST_BUCKET_NAME"]
    PACKET_KEY = Configuration().packet_key.to_bytes(1, "little")
    LENGTH = bytes([244])

    RANDOM_BYTES = [
        (
            b"\x02x\x8bf\x8d\xf5'\xa7\x0b\x9fW\xcf\x99.\x90\xa3\x00n:\xeaYN?Z\x1b\x0f\xd2\x11M\x00\\\xc1\xaf\x16-\xfal|"
            b"\x115\xe3>M\xf7\x0cq\xe6\xe2\n\xb4\x14\xbfb7\xcd\x97o\xd6\xacE\x82\x0cx\x9cBm\xe0s\xfb\x99\xde\xeb\x98"
            b"\x18vE\xf6\x9b\x14\xc8\xd7\xc6b\x9eM\xe5'\xdbTfD.\x0b\xcf\x92\xdc\x9d\x03\xda\r\xef\x84\xe8\xb7\x9d\xdb"
            b"\xaaQ\x87cX\xca\xd8\x80\xb6\x84\x8d\xc2m\xd2W\x8a \xb8\x8d\x89\xedT\xbakb)\xf8#\x84\xc4\xa7\xb7\x14\xef"
            b"\x88\xcb\x80,\xb0\x143\xc9\xf7<\xaf\xb5W\x91i\x13\x80\xea\xae\x9c\xad\x82\xf1\n\xb5\xe2\x92\xbd>\xa8\x82"
            b"\x90\r\x84k?_\xc9\xadx\xe15\xc3$\xbd\xb2\x80\xa8Aa|\x1a`\\\xb1\xf9?\xd9\xffM\x16\x03\x17xE?\x13\xb9\xf8"
            b"\xea\xc98a\x8f\x02\xbd\xfc\x8e\x07\x0b\x85\xc1\n\xc3\xa0\xb6\xc9\xce:%\x8bdc\x16\x90c\xea\x16/\xbc\xf3"
            b"\x93\x97\xb7\xca\x0e\x12.\xcb\x067>\xd3\xebBC"
        ),
        (
            b"r\xf8\x12=\xdc\xa73>\xbf\x84\x9d!\xb9\x1c\x18\xc4D0\xe8\xd4\x84\xf8\xd8Nao\x1f\xb0\xc6\xb1n\xe1\x90\xb6"
            b"\x8a\xe5{\xa0\x83\x18\xc4\xaa\xdd\x81/\xa0\r\x8a\x9c\x8d\t\xa7\xf0m\xcf\x1d\x81u.\xd6\xa0\x1b\xae<\x8f"
            b"\xdd\xd92aI\x808L\xfb\x9b\xd0p\x13\xa5E\x9d3\xab\x99X\xd7\x18Xqr \x1d\xc0 B\xf0\xb5<\x1f\x94\xdeJ2@\xed"
            b"\x08\xd6\xd6\xbaG\xed\xf4\xb8\xcd\x94\x0b\xb98g\xdbzIj;!pBt/\xbf!\xc7\xd8?\x13\xc9\x07\x03\x86\x9e\xd3"
            b"\xe0M\xfd\xb9\xaal\x1d9Ox\xda:\x7f\xcb\xa8\xb7\x9b\x01D\x1erM\x1dR\xb2\x8bjA]\xdc=i\xfenD\x02R \x9a\xef5N"
            b"\xb9\x18\xcb\x837<g\x8e\xc1dya/\xa4Rxb\x9f\x11'\xa1\xe2E\xa52\x93\x02q\x9fJ\xdc\xba\xec\xf8\x8b:\x81\x8c"
            b"\xe3\xe7\xfa =\x0e\xcdI\xefn\xe8\xed\xfe\xdd\xe6\xc0\xa8>\x18\xdek\x83\x81\x10,U+\x99\x07\xcb\xbf\xc6Mo1"
        ),
    ]

    BATCH_INTERVAL = 10
    storage_emulator = create_server("localhost", 9090, in_memory=True, default_bucket=TEST_BUCKET_NAME)
    storage_client = GoogleCloudStorageClient(project_name=TEST_PROJECT_NAME)

    @classmethod
    def setUpClass(cls):
        cls.storage_emulator.start()

    @classmethod
    def tearDownClass(cls):
        cls.storage_emulator.stop()

    def _check_batches_are_uploaded_to_cloud(self, packet_reader, sensor_names, number_of_batches_to_check=5):
        """Check that non-trivial batches from a packet reader for a particular sensor are uploaded to cloud storage."""
        number_of_batches = packet_reader.uploader._batch_number
        self.assertTrue(number_of_batches > 0)

        for i in range(number_of_batches_to_check):
            data = json.loads(
                self.storage_client.download_as_string(
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
        """Test that the configuration file is persisted."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([34])

        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[1])))

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
        configuration = self.storage_client.download_as_string(
            bucket_name=self.TEST_BUCKET_NAME,
            path_in_bucket=f"{packet_reader.uploader.output_directory}/configuration.json",
        )

        # Test configuration is valid.
        Configuration.from_dict(json.loads(configuration))

    def test_packet_reader_with_baro_sensor(self):
        """Test that the packet reader works with the baro sensor."""
        serial_port = DummySerial(port="test")
        sensor_type = bytes([34])

        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[1])))

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

        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[1])))

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

        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[1])))

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

        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[1])))

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

        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[1])))

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

        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[0])))
        serial_port.write(data=b"".join((self.PACKET_KEY, sensor_type, self.LENGTH, self.RANDOM_BYTES[1])))

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
