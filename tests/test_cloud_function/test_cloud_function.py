import json
import os
import unittest
from unittest import mock
from google.cloud.storage.client import Client
from octue.cloud.storage.client import GoogleCloudStorageClient

from cloud_function import main
from tests.base import BaseTestCase


SOURCE_PROJECT_NAME = "source-project"
SOURCE_BUCKET_NAME = "source-bucket"
DESTINATION_PROJECT_NAME = "destination-project"
DESTINATION_BUCKET_NAME = "destination-bucket"


class TestCleanAndUploadBatch(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["SOURCE_PROJECT_NAME"] = SOURCE_PROJECT_NAME
        os.environ["DESTINATION_PROJECT_NAME"] = DESTINATION_PROJECT_NAME
        os.environ["DESTINATION_BUCKET_NAME"] = DESTINATION_BUCKET_NAME

        cls.destination_storage_client = GoogleCloudStorageClient(DESTINATION_PROJECT_NAME)

        cls._create_buckets()
        cls._create_trigger_files()

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

    @classmethod
    def _create_trigger_files(cls):
        """Create a batch file and a configuration file in the source bucket.

        :return None:
        """
        source_storage_client = GoogleCloudStorageClient(SOURCE_PROJECT_NAME)

        source_storage_client.upload_from_string(
            string=json.dumps({"Baros": "blah,blah,hello,\n"}),
            bucket_name=SOURCE_BUCKET_NAME,
            path_in_bucket="window-0.json",
        )

        source_storage_client.upload_from_string(
            string=json.dumps({"baudrate": 10}),
            bucket_name=SOURCE_BUCKET_NAME,
            path_in_bucket="configuration.json",
        )

    @staticmethod
    def _make_mock_context():
        """Make a mock Google Cloud Functions event context object.

        :return unittest.mock.MagicMock:
        """
        context = unittest.mock.MagicMock()
        context.event_id = "some-id"
        context.event_type = "google.storage.object.finalize"

    def test_persist_configuration(self):
        """Test that configuration files are persisted to the destination bucket."""
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
                    bucket_name=DESTINATION_BUCKET_NAME, path_in_bucket=event["name"]
                )
            ),
            {"baudrate": 10},
        )

    def test_clean_and_upload_batch(self):
        """Test that a batch file is cleaned and uploaded to its destination bucket following the relevant Google Cloud
        storage trigger. The same source and destination bucket are used in this test although different ones will most
        likely be used in production.
        """
        event = {
            "bucket": SOURCE_BUCKET_NAME,
            "name": "window-0.json",
            "metageneration": "some-metageneration",
            "timeCreated": "0",
            "updated": "0",
        }

        with mock.patch("cloud_function.file_handler.FileHandler.clean", return_value={"baros": ["hello"]}):
            main.handle_upload(event=event, context=self._make_mock_context())

        # Check that cleaned batch has been created and is in the right place.
        self.assertEqual(
            json.loads(
                self.destination_storage_client.download_as_string(
                    bucket_name=DESTINATION_BUCKET_NAME,
                    path_in_bucket=event["name"],
                )
            ),
            {"baros": ["hello"]},
        )
