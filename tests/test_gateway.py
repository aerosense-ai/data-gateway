import unittest
from gateway import GatewayServer

from .base import BaseTestCase


class TestExampleModule(BaseTestCase):
    """ Testing operation of the ExampleModule class
     """

    def test_init_gateway(self):
        """ Ensures that the twine class can be instantiated with a file
        """
        # test_data_file = self.path + "test_data/test_configuration.json"
        GatewayServer()


if __name__ == "__main__":
    unittest.main()
