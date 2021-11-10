import os
from unittest.mock import Mock

from wtforms.validators import StopValidation

import sys
from cloud_function.forms import SlugifiedValidator
from tests.base import BaseTestCase
from tests.test_cloud_function import REPOSITORY_ROOT


# Manually add the cloud_function package to the path (its imports have to be done in a certain way for Google Cloud
# Functions to accept them that doesn't work when running/testing the package locally).
sys.path.insert(0, os.path.abspath(os.path.join(REPOSITORY_ROOT, "cloud_function")))


class TestForms(BaseTestCase):
    def test_slugified_validator_raises_error_if_input_not_slugified(self):
        """Test that the slugified validator raises a `StopValidation` error if the input is not slugified."""
        validator = SlugifiedValidator()
        with self.assertRaises(StopValidation):
            validator(form=None, field=Mock(raw_data=["Hello World"]))

    def test_no_error_raised_by_slugified_validator_if_input_is_slugified(self):
        """Test that the slugified validator does not raise a `StopValidation` error if the input is slugified."""
        validator = SlugifiedValidator()
        validator(form=None, field=Mock(raw_data=["hello-world"]))
