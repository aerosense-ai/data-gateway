import json
import os
import unittest
from unittest import mock
from gcloud_storage_emulator.server import create_server
from google.cloud import storage
from octue.utils.cloud.credentials import GCPCredentialsManager
from octue.utils.cloud.persistence import GoogleCloudStorageClient

from cloud_function import main


class TestCleanAndUploadBatch(unittest.TestCase):

    TEST_PROJECT_NAME = os.environ["TEST_PROJECT_NAME"]
    TEST_BUCKET_NAME = os.environ["TEST_BUCKET_NAME"]
    storage_emulator = create_server("localhost", 9090, in_memory=True)

    @classmethod
    def setUpClass(cls):
        os.environ["DESTINATION_PROJECT_NAME"] = cls.TEST_PROJECT_NAME
        os.environ["DESTINATION_BUCKET"] = cls.TEST_BUCKET_NAME

        cls.storage_emulator.start()

        storage.Client(
            project=cls.TEST_PROJECT_NAME, credentials=GCPCredentialsManager().get_credentials()
        ).create_bucket(bucket_or_name=cls.TEST_BUCKET_NAME)

        GoogleCloudStorageClient(cls.TEST_PROJECT_NAME).upload_from_string(
            serialised_data=json.dumps({"Baros": "blah,blah,hello,\n"}),
            bucket_name=cls.TEST_BUCKET_NAME,
            path_in_bucket="batch-0.json",
        )

    @classmethod
    def tearDownClass(cls):
        cls.storage_emulator.stop()
        del os.environ["DESTINATION_PROJECT_NAME"]
        del os.environ["DESTINATION_BUCKET"]

    def test_clean_and_upload_batch(self):
        """Test that a finalise event from a Google Cloud Storage bucket triggers cleaning of a batch file and its
        upload to a destination bucket.
        """
        event = {
            "bucket": self.TEST_BUCKET_NAME,
            "name": "batch-0.json",
            "metageneration": "some-metageneration",
            "timeCreated": "0",
            "updated": "0",
        }

        context = unittest.mock.MagicMock()
        context.event_id = "some-id"
        context.event_type = "gcs-event"

        with mock.patch("cloud_function.main.clean", return_value={"baros": "hello,\n"}):
            main.clean_and_upload_batch(event, context)

        self.assertEqual(
            json.loads(
                GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME).download_as_string(
                    bucket_name=self.TEST_BUCKET_NAME,
                    path_in_bucket="batch-0.json",
                )
            ),
            {"baros": "hello,\n"},
        )
