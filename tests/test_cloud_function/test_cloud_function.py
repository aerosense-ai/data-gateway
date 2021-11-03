import json
import os
from unittest.mock import MagicMock, patch

from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.utils.encoders import OctueJSONEncoder

import sys


# Manually add the cloud_function package to the path (its imports have to be done in a certain way for Google Cloud
# Functions to accept them that doesn't work when running/testing the package locally).
REPOSITORY_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(REPOSITORY_ROOT, "cloud_function")))

from cloud_function import main  # noqa
from tests import TEST_BUCKET_NAME  # noqa
from tests.base import BaseTestCase  # noqa


SOURCE_PROJECT_NAME = "source-project"
SOURCE_BUCKET_NAME = TEST_BUCKET_NAME
DESTINATION_PROJECT_NAME = "destination-project"
BIG_QUERY_DATASET_NAME = "destination-dataset"


class TestCleanAndUploadWindow(BaseTestCase):
    def test_clean_and_upload_window(self):
        """Test that a window file is cleaned and uploaded to its destination bucket following the relevant Google Cloud
        storage trigger. The same source and destination bucket are used in this test although different ones will most
        likely be used in production.
        """
        window = self.random_window(10, 10)

        GoogleCloudStorageClient(SOURCE_PROJECT_NAME).upload_from_string(
            string=json.dumps(window, cls=OctueJSONEncoder),
            bucket_name=SOURCE_BUCKET_NAME,
            path_in_bucket="window-0.json",
            metadata={"data_gateway__configuration": self.VALID_CONFIGURATION},
        )

        event = {
            "bucket": SOURCE_BUCKET_NAME,
            "name": "window-0.json",
            "metageneration": "some-metageneration",
            "timeCreated": "0",
            "updated": "0",
        }

        with patch.dict(
            os.environ,
            {
                "SOURCE_PROJECT_NAME": SOURCE_PROJECT_NAME,
                "DESTINATION_PROJECT_NAME": DESTINATION_PROJECT_NAME,
                "BIG_QUERY_DATASET_NAME": BIG_QUERY_DATASET_NAME,
            },
        ):
            with patch("file_handler.BigQueryDataset") as mock_dataset:
                main.handle_upload(event=event, context=self._make_mock_context())

        # Check configuration without user data was added.
        del self.VALID_CONFIGURATION["user_data"]
        self.assertIn("add_configuration", mock_dataset.mock_calls[1][0])
        self.assertEqual(mock_dataset.mock_calls[1].args[0], self.VALID_CONFIGURATION)

        # Check data was persisted.
        self.assertIn("insert_sensor_data", mock_dataset.mock_calls[2][0])
        self.assertEqual(mock_dataset.mock_calls[2].kwargs["data"].keys(), {"Mics", "cleaned"})
        self.assertEqual(mock_dataset.mock_calls[2].kwargs["installation_reference"], "aventa_turbine")
        self.assertEqual(mock_dataset.mock_calls[2].kwargs["label"], "my_test_1")

    @staticmethod
    def _make_mock_context():
        """Make a mock Google Cloud Functions event context object.

        :return unittest.mock.MagicMock:
        """
        context = MagicMock()
        context.event_id = "some-id"
        context.event_type = "google.storage.object.finalize"
