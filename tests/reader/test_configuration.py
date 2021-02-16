import unittest

from data_gateway.reader.configuration import Configuration
from tests.reader.valid_configuration import valid_configuration


class TestConfiguration(unittest.TestCase):
    def test_constructing_from_valid_configuration_dictionary(self):
        """Ensure a valid dictionary can be used to build a configuration."""
        Configuration.from_dict(valid_configuration)

    def test_dictionary_has_to_have_all_attributes_for_configuration_construction(self):
        """Test that a dictionary has to include all the attributes of a configuration to be able to construct one (i.e.
        that default arguments are unavailable when constructing from a dictionary).
        """
        invalid_configuration = valid_configuration
        del valid_configuration["baudrate"]

        with self.assertRaises(KeyError):
            Configuration.from_dict(invalid_configuration)
