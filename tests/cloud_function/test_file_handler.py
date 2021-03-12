import unittest

from cloud_function.file_handler import FileHandler


class TestFileHandler(unittest.TestCase):
    def test_clean(self):
        """Test clean function returns batch in expected format."""
        batch = {"Baros": "1/1/2000,1,2,3,4\n"}

        cleaned_batch = FileHandler(
            source_project="test", source_bucket="test", destination_project="test", destination_bucket="test"
        ).clean(batch, {}, {})

        self.assertTrue("cleaned" in cleaned_batch.keys())
