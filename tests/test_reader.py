import os
import time
import unittest
from multiprocessing import Process
from dummy_serial.dummy_serial import DummySerial
from dummy_serial.utils import random_bytes
from gateway.readers.packet_reader import PACKET_KEY, read_packets


class TestPacketReader(unittest.TestCase):
    """ Testing operation of the PacketReader class
    """

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

    def test_packet_reader_with_baro_sensor(self):
        serial_port = DummySerial(port="test")
        packet_key = PACKET_KEY.to_bytes(1, "little")
        sensor_type = bytes([34])
        length = bytes([244])

        for _ in range(2):
            serial_port.write(data=packet_key + sensor_type + length + random_bytes(256))

        process = Process(target=read_packets, args=(serial_port,))
        process.start()
        time.sleep(5)
        process.terminate()

        test_directory = os.path.dirname(__file__)
        directory_contents = sorted(os.listdir(test_directory))
        subdirectory_to_check = [item for item in directory_contents if item.startswith("2021")][-1]
        baros_file = os.path.join(test_directory, subdirectory_to_check, "baros.csv")

        with open(baros_file) as f:
            self.assertTrue(len(f.read()) > 0)


if __name__ == "__main__":
    unittest.main()
