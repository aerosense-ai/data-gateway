import unittest
from gateway.readers import PacketReader


class TestPacketReader(unittest.TestCase):
    """ Testing operation of the PacketReader class
     """

    def setUp(self):
        super().setUp()

        # powerCheck = '{0}{1:>4}\r'.format(SharpCodes['POWER'], SharpCodes['CHECK'])
        # com = dummyserial.Serial(port='COM1', baudrate=9600, ds_responses={powerCheck: powerCheck})
        # tv = SharpTV(com=com, TVID=999, tvInput='DVI')
        # tv.sendCmd(SharpCodes['POWER'], SharpCodes['CHECK'])
        # self.assertEqual(tv.recv(), powerCheck)

    def test_init_packet_reader(self):
        """ Ensures that the class can be instantiated
        """
        # test_data_file = self.path + "test_data/test_configuration.json"
        PacketReader()


if __name__ == "__main__":
    unittest.main()
