import unittest

from cloud_function.file_handler import FileHandler


class TestFileHandler(unittest.TestCase):
    def test_clean(self):
        """Test clean function returns batch in expected format."""
        batch = {"my_sensor": "1,2,3,4\n"}

        cleaned_batch = FileHandler(
            source_project="test", source_bucket="test", destination_project="test", destination_bucket="test"
        ).clean(batch, {}, {})

        self.assertEqual(cleaned_batch, {"my_sensor": "1,2,3,4\n", "cleaned": True})
