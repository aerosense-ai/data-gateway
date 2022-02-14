import csv
import datetime
import json
import os
import tempfile
import time
from unittest import mock

import google.api_core.exceptions
from google.cloud.storage.blob import Blob
from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient

from data_gateway.persistence import BatchingFileWriter, BatchingUploader
from tests import TEST_BUCKET_NAME, TEST_PROJECT_NAME
from tests.base import BaseTestCase


START_TIMESTAMP = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).timestamp()


class TestBatchingWriter(BaseTestCase):
    def test_data_is_batched(self):
        """Test that data is batched as expected."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = BatchingFileWriter(
                sensor_names=["test"],
                output_directory=os.path.join(temporary_directory, "this-session"),
                window_size=600,
            )

        writer.add_to_current_window(sensor_name="test", data="blah,")
        self.assertEqual(writer.current_window["sensor_data"]["test"], ["blah,"])

    def test_data_is_written_to_disk_in_windows(self):
        """Test that data is written to disk as time windows."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = BatchingFileWriter(
                sensor_names=["test"],
                output_directory=os.path.join(temporary_directory, "this-session"),
                window_size=0.01,
            )

            with writer:
                writer.add_to_current_window(sensor_name="test", data="ping")
                writer.add_to_current_window(sensor_name="test", data="pong")
                self.assertEqual(len(writer.current_window["sensor_data"]["test"]), 2)
                time.sleep(writer.window_size * 2)

                writer.add_to_current_window(sensor_name="test", data="ding")
                writer.add_to_current_window(sensor_name="test", data="dong")
                self.assertEqual(len(writer.current_window["sensor_data"]["test"]), 2)

            self.assertEqual(len(writer.current_window["sensor_data"]["test"]), 0)

            with open(os.path.join(writer.output_directory, "window-0.json")) as f:
                self.assertEqual(json.load(f)["sensor_data"], {"test": ["ping", "pong"]})

            with open(os.path.join(writer.output_directory, "window-1.json")) as f:
                self.assertEqual(json.load(f)["sensor_data"], {"test": ["ding", "dong"]})

    def test_oldest_window_is_deleted_when_storage_limit_reached(self):
        """Check that (only) the oldest window is deleted when the storage limit is reached."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            writer = BatchingFileWriter(
                sensor_names=["test"],
                output_directory=os.path.join(temporary_directory, "this-session"),
                window_size=0.01,
                storage_limit=1,
            )

            with writer:
                writer.add_to_current_window(sensor_name="test", data="ping,")

            first_window_path = os.path.join(writer.output_directory, "window-0.json")

            # Check first file is written to disk.
            self.assertTrue(os.path.exists(first_window_path))

            with writer:
                writer.add_to_current_window(sensor_name="test", data="pong,\n")

            # Check first (oldest) file has now been deleted.
            self.assertFalse(os.path.exists(first_window_path))

            # Check the second file has not been deleted.
            self.assertTrue(os.path.exists(os.path.join(writer.output_directory, "window-1.json")))

    def test_that_csv_files_are_written(self):
        """Test that data is written to disk as CSV-files if the `save_csv_files` option is `True`."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            writer = BatchingFileWriter(
                sensor_names=["sensor1", "sensor2"],
                output_directory=os.path.join(temporary_directory, "this-session"),
                save_csv_files=True,
                window_size=0.01,
            )

            with writer:
                writer.add_to_current_window(sensor_name="sensor1", data=[1, 2, 3])
                writer.add_to_current_window(sensor_name="sensor2", data=[1, 2, 3])
                writer.add_to_current_window(sensor_name="sensor1", data=[4, 5, 6])
                writer.add_to_current_window(sensor_name="sensor2", data=[4, 5, 6])

            with open(os.path.join(writer.output_directory, "sensor1.csv")) as f:
                reader = csv.reader(f)
                self.assertEqual([row for row in reader], [["1", "2", "3"], ["4", "5", "6"]])

            with open(os.path.join(writer.output_directory, "sensor2.csv")) as f:
                reader = csv.reader(f)
                self.assertEqual([row for row in reader], [["1", "2", "3"], ["4", "5", "6"]])


class TestBatchingUploader(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        """Add a Google Cloud Storage client to the class.

        :return None:
        """
        cls.storage_client = GoogleCloudStorageClient(project_name=TEST_PROJECT_NAME)

    def test_data_is_batched(self):
        """Test that data is batched as expected."""
        uploader = BatchingUploader(
            sensor_names=["test"],
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
            window_size=600,
            output_directory=storage.path.join(tempfile.TemporaryDirectory().name, "this-session"),
        )

        uploader.add_to_current_window(sensor_name="test", data="blah,")
        self.assertEqual(uploader.current_window["sensor_data"]["test"], ["blah,"])

    def test_data_is_uploaded_in_windows_and_can_be_retrieved_from_cloud_storage(self):
        """Test that data is uploaded in time windows that can be retrieved from cloud storage."""
        uploader = BatchingUploader(
            sensor_names=["test"],
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
            window_size=0.01,
            output_directory=storage.path.join(tempfile.TemporaryDirectory().name, "this-session"),
        )

        with uploader:
            uploader.add_to_current_window(sensor_name="test", data="ping")
            uploader.add_to_current_window(sensor_name="test", data="pong")
            self.assertEqual(len(uploader.current_window["sensor_data"]["test"]), 2)

            time.sleep(uploader.window_size)

            uploader.add_to_current_window(sensor_name="test", data="ding")
            uploader.add_to_current_window(sensor_name="test", data="dong")
            self.assertEqual(len(uploader.current_window["sensor_data"]["test"]), 2)

            time.sleep(uploader.window_size)

        self.assertEqual(len(uploader.current_window["sensor_data"]["test"]), 0)

        self.assertEqual(
            json.loads(
                self.storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(uploader.output_directory, "window-0.json"),
                )
            )["sensor_data"],
            {"test": ["ping", "pong"]},
        )

        self.assertEqual(
            json.loads(
                self.storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(uploader.output_directory, "window-1.json"),
                )
            )["sensor_data"],
            {"test": ["ding", "dong"]},
        )

    def test_window_is_written_to_disk_if_upload_fails(self):
        """Test that a window is written to disk if it fails to upload to the cloud."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            with mock.patch.object(
                Blob,
                "upload_from_string",
                side_effect=Exception("This is deliberately raised in this test to simulate cloud upload failing."),
            ):
                uploader = BatchingUploader(
                    sensor_names=["test"],
                    project_name=TEST_PROJECT_NAME,
                    bucket_name=TEST_BUCKET_NAME,
                    window_size=0.01,
                    output_directory=storage.path.join(temporary_directory, "this-session"),
                    upload_backup_files=False,
                )

                with uploader:
                    uploader.add_to_current_window(sensor_name="test", data="ping")
                    uploader.add_to_current_window(sensor_name="test", data="pong")

            # Check that the upload has failed.
            with self.assertRaises(google.api_core.exceptions.NotFound):
                self.storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(uploader.output_directory, "window-0.json"),
                )

            # Check that a backup file has been written.
            with open(os.path.join(uploader.output_directory, ".backup", "window-0.json")) as f:
                self.assertEqual(json.load(f)["sensor_data"], {"test": ["ping", "pong"]})

    def test_backup_files_are_uploaded_on_next_upload_attempt(self):
        """Test that backup files from a failed upload are uploaded on the next upload attempt."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            with mock.patch.object(
                Blob,
                "upload_from_string",
                side_effect=Exception("This is deliberately raised in this test to simulate cloud upload failing."),
            ):
                uploader = BatchingUploader(
                    sensor_names=["test"],
                    project_name=TEST_PROJECT_NAME,
                    bucket_name=TEST_BUCKET_NAME,
                    window_size=10,
                    output_directory=storage.path.join(temporary_directory, "this-session"),
                    upload_backup_files=True,
                )

                with uploader:
                    uploader.add_to_current_window(sensor_name="test", data="ping")
                    uploader.add_to_current_window(sensor_name="test", data="pong")

            # Check that the upload has failed.
            with self.assertRaises(google.api_core.exceptions.NotFound):
                self.storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(uploader.output_directory, "window-0.json"),
                )

            backup_path = os.path.join(uploader._backup_directory, "window-0.json")

            # Check that a backup file has been written.
            with open(backup_path) as f:
                self.assertEqual(json.load(f)["sensor_data"], {"test": ["ping", "pong"]})

            with uploader:
                uploader.add_to_current_window(sensor_name="test", data=["ding", "dong"])

        # Check that both windows are now in cloud storage.
        self.assertEqual(
            json.loads(
                self.storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(uploader.output_directory, "window-0.json"),
                )
            )["sensor_data"],
            {"test": ["ping", "pong"]},
        )

        self.assertEqual(
            json.loads(
                self.storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(uploader.output_directory, "window-1.json"),
                )
            )["sensor_data"],
            {"test": [["ding", "dong"]]},
        )

        # Check that the backup file has been removed now it's been uploaded to cloud storage.
        self.assertFalse(os.path.exists(backup_path))

    def test_metadata_is_added_to_uploaded_files(self):
        """Test that metadata is added to uploaded files and can be retrieved."""
        uploader = BatchingUploader(
            sensor_names=["test"],
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
            window_size=0.01,
            output_directory=storage.path.join(tempfile.TemporaryDirectory().name, "this-session"),
            metadata={"big": "rock"},
        )

        with uploader:
            uploader.add_to_current_window(sensor_name="test", data="ping,")

        metadata = self.storage_client.get_metadata(
            bucket_name=TEST_BUCKET_NAME,
            path_in_bucket=storage.path.join(uploader.output_directory, "window-0.json"),
        )

        self.assertEqual(metadata["custom_metadata"], {"big": "rock"})