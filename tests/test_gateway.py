import os
import unittest

from data_gateway import GatewayServer


class TestGateway(unittest.TestCase):
    """Testing operation of the Gateway class"""

    def setUp(self):

        super().setUp()
        self.path = str(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", ""))

    def test_init_gateway(self):
        """Ensures that the twine class can be instantiated with a file"""
        # test_data_file = self.path + "test_data/test_configuration.json"
        GatewayServer()


if __name__ == "__main__":
    unittest.main()
