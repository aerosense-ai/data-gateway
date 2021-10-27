import json
import os
from unittest.mock import MagicMock

from google.cloud.storage.client import Client
from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.utils.encoders import OctueJSONEncoder

import sys


# Manually add the cloud_function package to the path (its imports have to be done in a certain way for Google Cloud
# Functions to accept them that doesn't work when running/testing the package locally).
REPOSITORY_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(REPOSITORY_ROOT, "cloud_function")))

from cloud_function import main  # noqa
from tests.base import BaseTestCase  # noqa


SOURCE_PROJECT_NAME = "source-project"
SOURCE_BUCKET_NAME = "source-bucket"
DESTINATION_PROJECT_NAME = "destination-project"
DESTINATION_BUCKET_NAME = "destination-bucket"


class TestCleanAndUploadWindow(BaseTestCase):
    def test_persist_configuration(self):
        """Test that configuration files are persisted to the destination bucket."""
        self.source_storage_client.upload_from_string(
            string=json.dumps(self.VALID_CONFIGURATION),
            bucket_name=SOURCE_BUCKET_NAME,
            path_in_bucket="configuration.json",
        )

        event = {
            "bucket": SOURCE_BUCKET_NAME,
            "name": "configuration.json",
            "metageneration": "some-metageneration",
            "timeCreated": "0",
            "updated": "0",
        }

        main.handle_upload(event=event, context=self._make_mock_context())

        # Check configuration has been persisted in the right place.
        self.assertEqual(
            json.loads(
                self.destination_storage_client.download_as_string(
                    cloud_path=f"gs://{DESTINATION_BUCKET_NAME}/configuration.json"
                )
            ),
            self.VALID_CONFIGURATION,
        )

    def test_clean_and_upload_window(self):
        """Test that a window file is cleaned and uploaded to its destination bucket following the relevant Google Cloud
        storage trigger. The same source and destination bucket are used in this test although different ones will most
        likely be used in production.
        """
        window = self.random_window(10, 10)

        self.source_storage_client.upload_from_string(
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

        main.handle_upload(event=event, context=self._make_mock_context())

        # Check that cleaned window has been created and is in the right place.
        cleaned_window = json.loads(
            self.destination_storage_client.download_as_string(
                cloud_path=f"gs://{DESTINATION_BUCKET_NAME}/window-0.json"
            )
        )

        self.assertEqual(cleaned_window["cleaned"], True)
        self.assertIn("Mics", cleaned_window)

    @classmethod
    def setUpClass(cls):
        os.environ["SOURCE_PROJECT_NAME"] = SOURCE_PROJECT_NAME
        os.environ["DESTINATION_PROJECT_NAME"] = DESTINATION_PROJECT_NAME
        os.environ["DESTINATION_BUCKET_NAME"] = DESTINATION_BUCKET_NAME

        cls.source_storage_client = GoogleCloudStorageClient(SOURCE_PROJECT_NAME)
        cls.destination_storage_client = GoogleCloudStorageClient(DESTINATION_PROJECT_NAME)
        cls._create_buckets()

    @classmethod
    def tearDownClass(cls):
        del os.environ["SOURCE_PROJECT_NAME"]
        del os.environ["DESTINATION_PROJECT_NAME"]
        del os.environ["DESTINATION_BUCKET_NAME"]

    @staticmethod
    def _create_buckets():
        """Create the source and destination buckets.

        :return None:
        """
        Client(project=SOURCE_PROJECT_NAME).create_bucket(SOURCE_BUCKET_NAME)
        Client(project=DESTINATION_PROJECT_NAME).create_bucket(DESTINATION_BUCKET_NAME)

    @staticmethod
    def _make_mock_context():
        """Make a mock Google Cloud Functions event context object.

        :return unittest.mock.MagicMock:
        """
        context = MagicMock()
        context.event_id = "some-id"
        context.event_type = "google.storage.object.finalize"
