import datetime
import unittest

from cloud_function.file_handler import FileHandler


class TestFileHandler(unittest.TestCase):
    def test_clean(self):
        """Test clean function returns batch in expected format."""
        batch = {
            "sensor_time_offset": datetime.datetime(2000, 1, 1).timestamp(),
            "sensor_data": {"Baros_P": [[0, 1, 2, 3], [1, 1, 2, 3]]},
        }

        cleaned_batch = FileHandler(
            source_project="test", source_bucket="test", destination_project="test", destination_bucket="test"
        ).clean_batch(batch, {"period": {"Baros_P": 1}}, {})

        self.assertTrue("cleaned" in cleaned_batch.keys())
