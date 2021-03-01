import json
import os
import unittest
from unittest import mock
from gcp_storage_emulator.server import create_server
from octue.utils.cloud import storage
from octue.utils.cloud.storage.client import GoogleCloudStorageClient

from cloud_function import main
from cloud_function.file_handler import DATAFILES_DIRECTORY


class TestCleanAndUploadBatch(unittest.TestCase):

    TEST_PROJECT_NAME = "a-project-name"
    TEST_BUCKET_NAME = "a-bucket-name"
    storage_emulator = create_server("localhost", 9090, in_memory=True, default_bucket=TEST_BUCKET_NAME)
    storage_client = GoogleCloudStorageClient(TEST_PROJECT_NAME)

    @classmethod
    def setUpClass(cls):
        os.environ["GCP_PROJECT"] = cls.TEST_PROJECT_NAME
        os.environ["DESTINATION_PROJECT_NAME"] = cls.TEST_PROJECT_NAME
        os.environ["DESTINATION_BUCKET"] = cls.TEST_BUCKET_NAME
        cls.storage_emulator.start()

        # Create trigger files.
        cls.storage_client.upload_from_string(
            serialised_data=json.dumps({"Baros": "blah,blah,hello,\n"}),
            bucket_name=cls.TEST_BUCKET_NAME,
            path_in_bucket="window-0.json",
        )

        cls.storage_client.upload_from_string(
            serialised_data=json.dumps({"baudrate": 10}),
            bucket_name=cls.TEST_BUCKET_NAME,
            path_in_bucket="configuration.json",
        )

    @classmethod
    def tearDownClass(cls):
        cls.storage_emulator.stop()
        del os.environ["DESTINATION_PROJECT_NAME"]
        del os.environ["DESTINATION_BUCKET"]

    def _make_mock_context(self):
        """Make a mock Google Cloud Functions event context object.

        :return unittest.mock.MagicMock:
        """
        context = unittest.mock.MagicMock()
        context.event_id = "some-id"
        context.event_type = "google.storage.object.finalize"

    def test_persist_configuration(self):
        """Test that configuration files are persisted to the destination bucket."""
        event = {
            "bucket": self.TEST_BUCKET_NAME,
            "name": "configuration.json",
            "metageneration": "some-metageneration",
            "timeCreated": "0",
            "updated": "0",
        }

        main.clean_and_upload_batch(event=event, context=self._make_mock_context())

        # Check configuration has been persisted in the right place.
        self.assertEqual(
            json.loads(self.storage_client.download_as_string(self.TEST_BUCKET_NAME, path_in_bucket=event["name"])),
            {"baudrate": 10},
        )

    def test_clean_and_upload_batch(self):
        """Test that a batch file is cleaned and uploaded to its destination bucket following the relevant Google Cloud
        storage trigger. The same source and destination bucket are used in this test although different ones will most
        likely be used in production.
        """
        event = {
            "bucket": self.TEST_BUCKET_NAME,
            "name": "window-0.json",
            "metageneration": "some-metageneration",
            "timeCreated": "0",
            "updated": "0",
        }

        cleaned_batch_name = "cleaned-batch-0.json"

        with mock.patch("cloud_function.file_handler.FileHandler.clean", return_value={"baros": "hello,\n"}):
            main.clean_and_upload_batch(
                event=event, context=self._make_mock_context(), cleaned_batch_name=cleaned_batch_name
            )

        # Check that cleaned batch has been created and is in the right place.
        self.assertEqual(
            json.loads(
                self.storage_client.download_as_string(
                    bucket_name=self.TEST_BUCKET_NAME,
                    path_in_bucket=cleaned_batch_name,
                )
            ),
            {"baros": "hello,\n"},
        )

        # Check that datafile has been created.
        self.assertEqual(
            json.loads(
                self.storage_client.download_as_string(
                    bucket_name=self.TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(DATAFILES_DIRECTORY, cleaned_batch_name),
                )
            )["name"],
            cleaned_batch_name,
        )
