import json
import os
import unittest
from unittest import mock
from gcp_storage_emulator.server import create_server
from google.cloud.storage.client import Client
from octue.utils.cloud import storage
from octue.utils.cloud.storage.client import GoogleCloudStorageClient

from cloud_function import main
from cloud_function.file_handler import DATAFILES_DIRECTORY


class TestCleanAndUploadBatch(unittest.TestCase):
    SOURCE_PROJECT_NAME = "source-project"
    SOURCE_BUCKET_NAME = "source-bucket"
    DESTINATION_PROJECT_NAME = "destination-project"
    DESTINATION_BUCKET_NAME = "destination-bucket"
    storage_emulator = create_server("localhost", 9090, in_memory=True)

    @classmethod
    def setUpClass(cls):
        os.environ["GCP_PROJECT"] = cls.SOURCE_PROJECT_NAME
        os.environ["DESTINATION_PROJECT_NAME"] = cls.DESTINATION_PROJECT_NAME
        os.environ["DESTINATION_BUCKET"] = cls.DESTINATION_BUCKET_NAME

        cls.destination_storage_client = GoogleCloudStorageClient(cls.DESTINATION_PROJECT_NAME)

        cls.storage_emulator.start()
        cls._create_buckets()
        cls._create_trigger_files()

    @classmethod
    def tearDownClass(cls):
        cls.storage_emulator.stop()
        del os.environ["GCP_PROJECT"]
        del os.environ["DESTINATION_PROJECT_NAME"]
        del os.environ["DESTINATION_BUCKET"]

    @classmethod
    def _create_buckets(cls):
        """Create the source and destination buckets.

        :return None:
        """
        Client(project=cls.SOURCE_PROJECT_NAME).create_bucket(cls.SOURCE_BUCKET_NAME)
        Client(project=cls.DESTINATION_PROJECT_NAME).create_bucket(cls.DESTINATION_BUCKET_NAME)

    @classmethod
    def _create_trigger_files(cls):
        """Create a batch file and a configuration file in the source bucket.

        :return None:
        """
        source_storage_client = GoogleCloudStorageClient(cls.SOURCE_PROJECT_NAME)

        source_storage_client.upload_from_string(
            serialised_data=json.dumps({"Baros": "blah,blah,hello,\n"}),
            bucket_name=cls.SOURCE_BUCKET_NAME,
            path_in_bucket="window-0.json",
        )

        source_storage_client.upload_from_string(
            serialised_data=json.dumps({"baudrate": 10}),
            bucket_name=cls.SOURCE_BUCKET_NAME,
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
            "bucket": self.SOURCE_BUCKET_NAME,
            "name": "configuration.json",
            "metageneration": "some-metageneration",
            "timeCreated": "0",
            "updated": "0",
        }

        main.clean_and_upload_batch(event=event, context=self._make_mock_context())

        # Check configuration has been persisted in the right place.
        self.assertEqual(
            json.loads(
                self.destination_storage_client.download_as_string(
                    self.DESTINATION_BUCKET_NAME, path_in_bucket=event["name"]
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
            "bucket": self.SOURCE_BUCKET_NAME,
            "name": "window-0.json",
            "metageneration": "some-metageneration",
            "timeCreated": "0",
            "updated": "0",
        }

        with mock.patch("cloud_function.file_handler.FileHandler.clean", return_value={"baros": "hello,\n"}):
            main.clean_and_upload_batch(event=event, context=self._make_mock_context())

        # Check that cleaned batch has been created and is in the right place.
        self.assertEqual(
            json.loads(
                self.destination_storage_client.download_as_string(
                    bucket_name=self.DESTINATION_BUCKET_NAME,
                    path_in_bucket=event["name"],
                )
            ),
            {"baros": "hello,\n"},
        )

        # Check that datafile has been created.
        self.assertEqual(
            json.loads(
                self.destination_storage_client.download_as_string(
                    bucket_name=self.DESTINATION_BUCKET_NAME,
                    path_in_bucket=storage.path.join(DATAFILES_DIRECTORY, event["name"]),
                )
            )["name"],
            event["name"],
        )

    def test_clean(self):
        """Test clean function returns batch in expected format."""
        batch = {"my_sensor": "1,2,3,4\n"}
        cleaned_batch = main.clean(batch, {}, {})
        self.assertEqual(cleaned_batch, {"my_sensor": "1,2,3,4\n", "cleaned": True})
