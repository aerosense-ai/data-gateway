import json
import os

from data_gateway.configuration import Configuration
from tests.base import BaseTestCase


class TestConfiguration(BaseTestCase):

    configuration_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "valid_configuration.json")
    with open(configuration_path) as f:
        VALID_CONFIGURATION = json.load(f)

    def test_constructing_from_valid_configuration_dictionary(self):
        """Ensure a valid dictionary can be used to build a configuration."""
        Configuration.from_dict(self.VALID_CONFIGURATION)

    def test_dictionary_has_to_have_all_attributes_for_configuration_construction(self):
        """Test that a dictionary has to include all the attributes of a configuration to be able to construct one (i.e.
        that default arguments are unavailable when constructing from a dictionary).
        """
        invalid_configuration = self.VALID_CONFIGURATION.copy()
        del invalid_configuration["baudrate"]

        with self.assertRaises(KeyError):
            Configuration.from_dict(invalid_configuration)

    def test_to_dict(self):
        """Test that a configuration can be serialised to a dictionary."""
        configuration = Configuration.from_dict(self.VALID_CONFIGURATION)
        self.assertEqual(configuration.to_dict(), self.VALID_CONFIGURATION)
