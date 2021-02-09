import os
import tempfile
import unittest

from dummy_serial.dummy_serial import DummySerial
from dummy_serial.utils import random_bytes
from gateway.readers.constants import PACKET_KEY
from gateway.readers.packet_reader import read_packets


class TestPacketReader(unittest.TestCase):
    """Testing operation of the PacketReader class"""

    # def setUp(self):
    #     super().setUp()
    #
    #     # powerCheck = '{0}{1:>4}\r'.format(SharpCodes['POWER'], SharpCodes['CHECK'])
    #     # com = dummyserial.Serial(port='COM1', baudrate=9600, ds_responses={powerCheck: powerCheck})
    #     # tv = SharpTV(com=com, TVID=999, tvInput='DVI')
    #     # tv.sendCmd(SharpCodes['POWER'], SharpCodes['CHECK'])
    #     # self.assertEqual(tv.recv(), powerCheck)
    #
    # def test_init_packet_reader(self):
    #     """ Ensures that the class can be instantiated
    #     """
    #     # test_data_file = self.path + "test_data/test_configuration.json"
    #     PacketReader()

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
