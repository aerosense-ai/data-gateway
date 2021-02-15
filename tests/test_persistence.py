import os
import tempfile
import time
import unittest
from gcloud_storage_emulator.server import create_server
from google.cloud import storage
from octue.utils.cloud.credentials import GCPCredentialsManager
from octue.utils.cloud.persistence import GoogleCloudStorageClient

from data_gateway.persistence import BATCH_DIRECTORY_NAME, BatchingFileWriter, BatchingUploader


class TestBatchingWriter(unittest.TestCase):
    def test_data_is_batched(self):
        """Test that data is added to the correct stream as expected."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = BatchingFileWriter(
                sensor_specifications=[{"name": "test", "extension": ".csv"}],
                directory_path=temporary_directory,
                batch_interval=600,
            )

        stream = writer.current_batches["test"]
        self.assertEqual(stream["name"], "test"),
        self.assertEqual(stream["data"], [])
        self.assertEqual(stream["batch_number"], 0),
        self.assertEqual(stream["extension"], ".csv")

        writer.add_to_current_batch(sensor_name="test", data="blah,")
        self.assertEqual(writer.current_batches["test"]["data"], ["blah,"])

    def test_data_is_written_to_disk_in_batches(self):
        """Test that data is written to disk in batches of whatever units it is added to the stream in."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = BatchingFileWriter(
                sensor_specifications=[{"name": "test", "extension": ".csv"}],
                directory_path=temporary_directory,
                batch_interval=0.01,
            )

            with writer:
                writer.add_to_current_batch(sensor_name="test", data="ping,")
                writer.add_to_current_batch(sensor_name="test", data="pong,\n")
                self.assertEqual(len(writer.current_batches["test"]["data"]), 2)
                time.sleep(0.01)

                writer.add_to_current_batch(sensor_name="test", data="ding,")
                self.assertEqual(len(writer.current_batches["test"]["data"]), 0)

                writer.add_to_current_batch(sensor_name="test", data="dong,\n")
                time.sleep(0.01)

            self.assertEqual(len(writer.current_batches["test"]["data"]), 0)

            with open(os.path.join(temporary_directory, BATCH_DIRECTORY_NAME, "test", "batch-0.csv")) as f:
                self.assertEqual(f.read(), "ping,pong,\nding,")

            with open(os.path.join(temporary_directory, BATCH_DIRECTORY_NAME, "test", "batch-1.csv")) as f:
                self.assertEqual(f.read(), "dong,\n")


class TestBatchingUploader(unittest.TestCase):

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

    def test_data_is_batched(self):
        """Test that data is added to the correct stream as expected."""
        uploader = BatchingUploader(
            sensor_specifications=[{"name": "test", "extension": ".csv"}],
            project_name=self.TEST_PROJECT_NAME,
            bucket_name=self.TEST_BUCKET_NAME,
            batch_interval=600,
        )

        stream = uploader.current_batches["test"]
        self.assertEqual(stream["name"], "test"),
        self.assertEqual(stream["data"], [])
        self.assertEqual(stream["batch_number"], 0),
        self.assertEqual(stream["extension"], ".csv")

        uploader.add_to_current_batch(sensor_name="test", data="blah,")
        self.assertEqual(uploader.current_batches["test"]["data"], ["blah,"])

    def test_data_is_uploaded_in_batches_and_can_be_retrieved_from_cloud_storage(self):
        """Test that data is uploaded in batches of whatever units it is added to the stream in, and that it can be
        retrieved from cloud storage.
        """
        uploader = BatchingUploader(
            sensor_specifications=[{"name": "test", "extension": ".csv"}],
            project_name=self.TEST_PROJECT_NAME,
            bucket_name=self.TEST_BUCKET_NAME,
            batch_interval=0.01,
        )

        with uploader:
            uploader.add_to_current_batch(sensor_name="test", data="ping,")
            uploader.add_to_current_batch(sensor_name="test", data="pong,\n")
            self.assertEqual(len(uploader.current_batches["test"]["data"]), 2)
            time.sleep(0.01)

            uploader.add_to_current_batch(sensor_name="test", data="ding,")
            self.assertEqual(len(uploader.current_batches["test"]["data"]), 0)

            uploader.add_to_current_batch(sensor_name="test", data="dong,\n")
            time.sleep(0.01)

        self.assertEqual(len(uploader.current_batches["test"]["data"]), 0)

        self.assertEqual(
            GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME).download_as_string(
                bucket_name=self.TEST_BUCKET_NAME,
                path_in_bucket=f"{BATCH_DIRECTORY_NAME}/test/batch-0.csv",
            ),
            "ping,pong,\nding,",
        )

        self.assertEqual(
            GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME).download_as_string(
                bucket_name=self.TEST_BUCKET_NAME,
                path_in_bucket=f"{BATCH_DIRECTORY_NAME}/test/batch-1.csv",
            ),
            "dong,\n",
        )
