import os
import tempfile
import time
import unittest
from gcloud_storage_emulator.server import create_server
from google.cloud import storage
from octue.utils.cloud.credentials import GCPCredentialsManager
from octue.utils.cloud.persistence import GoogleCloudStorageClient

from gateway.uploaders import CLOUD_DIRECTORY_NAME, StreamingUploader


class TestStreamingUploader(unittest.TestCase):

    TEST_PROJECT_NAME = os.environ["TEST_PROJECT_NAME"]
    TEST_BUCKET_NAME = os.environ["TEST_BUCKET_NAME"]
    storage_emulator = create_server("localhost", 9090, in_memory=True)

    @classmethod
    def setUpClass(cls):
        cls.storage_emulator.start()
        storage.Client(
            project=cls.TEST_PROJECT_NAME, credentials=GCPCredentialsManager().get_credentials()
        ).create_bucket(bucket_or_name=cls.TEST_BUCKET_NAME)

    @classmethod
    def tearDownClass(cls):
        cls.storage_emulator.stop()

    def test_data_is_added_to_stream(self):
        """Test that data is added to the correct stream as expected."""
        uploader = StreamingUploader(
            sensor_types=[{"name": "test", "extension": ".csv"}],
            project_name=self.TEST_PROJECT_NAME,
            bucket_name=self.TEST_BUCKET_NAME,
            upload_interval=600,
        )

        self.assertEqual(uploader.streams["test"], {"name": "test", "data": [], "batch_number": 0, "extension": ".csv"})

        uploader.add_to_stream(sensor_type="test", data="blah,")
        self.assertEqual(uploader.streams["test"]["data"], ["blah,"])

    def test_data_is_uploaded_in_batches_and_can_be_retrieved_from_cloud_storage(self):
        """Test that data is uploaded in batches of whatever units it is added to the stream in, and that it can be
        retrieved from cloud storage.
        """
        uploader = StreamingUploader(
            sensor_types=[{"name": "test", "extension": ".csv"}],
            project_name=self.TEST_PROJECT_NAME,
            bucket_name=self.TEST_BUCKET_NAME,
            upload_interval=0.01,
        )

        with uploader:
            uploader.add_to_stream(sensor_type="test", data="ping,")
            uploader.add_to_stream(sensor_type="test", data="pong,\n")
            self.assertEqual(len(uploader.streams["test"]["data"]), 2)
            time.sleep(0.01)

            uploader.add_to_stream(sensor_type="test", data="ding,")
            self.assertEqual(len(uploader.streams["test"]["data"]), 0)

            uploader.add_to_stream(sensor_type="test", data="dong,\n")
            self.assertEqual(len(uploader.streams["test"]["data"]), 1)
            time.sleep(0.01)

        self.assertEqual(len(uploader.streams["test"]["data"]), 0)

        with tempfile.TemporaryDirectory() as temporary_directory:
            first_batch_download_path = os.path.join(temporary_directory, "batch.csv")

            GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME).download_file(
                bucket_name=self.TEST_BUCKET_NAME,
                path_in_bucket=f"{CLOUD_DIRECTORY_NAME}/test/batch-0.csv",
                local_path=first_batch_download_path,
            )

            with open(first_batch_download_path) as f:
                self.assertEqual(f.read(), "ping,pong,\nding,")

            second_batch_download_path = os.path.join(temporary_directory, "batch.csv")

            GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME).download_file(
                bucket_name=self.TEST_BUCKET_NAME,
                path_in_bucket=f"{CLOUD_DIRECTORY_NAME}/test/batch-1.csv",
                local_path=second_batch_download_path,
            )

            with open(second_batch_download_path) as f:
                self.assertEqual(f.read(), "dong,\n")
