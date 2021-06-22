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


class TestBatchingWriter(BaseTestCase):
    def test_data_is_batched(self):
        """Test that data is batched as expected."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = BatchingFileWriter(
                sensor_names=["test"],
                session_subdirectory="this-session",
                output_directory=temporary_directory,
                batch_interval=600,
            )

        writer.add_to_current_batch(sensor_name="test", data="blah,")
        self.assertEqual(writer.current_batch["test"], ["blah,"])

    def test_data_is_written_to_disk_in_batches(self):
        """Test that data is written to disk in batches of whatever units it is added in."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = BatchingFileWriter(
                sensor_names=["test"],
                session_subdirectory="this-session",
                output_directory=temporary_directory,
                batch_interval=0.01,
            )

            with writer:
                writer.add_to_current_batch(sensor_name="test", data="ping")
                writer.add_to_current_batch(sensor_name="test", data="pong")
                self.assertEqual(len(writer.current_batch["test"]), 2)
                time.sleep(writer.batch_interval * 2)

                writer.add_to_current_batch(sensor_name="test", data="ding")
                writer.add_to_current_batch(sensor_name="test", data="dong")
                self.assertEqual(len(writer.current_batch["test"]), 2)

            self.assertEqual(len(writer.current_batch["test"]), 0)

            with open(os.path.join(temporary_directory, writer._session_subdirectory, "window-0.json")) as f:
                self.assertEqual(json.load(f), {"test": ["ping", "pong"]})

            with open(os.path.join(temporary_directory, writer._session_subdirectory, "window-1.json")) as f:
                self.assertEqual(json.load(f), {"test": ["ding", "dong"]})

    def test_oldest_batch_is_deleted_when_storage_limit_reached(self):
        """Check that (only) the oldest batch is deleted when the storage limit is reached."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = BatchingFileWriter(
                sensor_names=["test"],
                session_subdirectory="this-session",
                output_directory=temporary_directory,
                batch_interval=0.01,
                storage_limit=1,
            )

            with writer:
                writer.add_to_current_batch(sensor_name="test", data="ping,")

            first_batch_path = os.path.join(temporary_directory, writer._session_subdirectory, "window-0.json")

            # Check first file is written to disk.
            self.assertTrue(os.path.exists(first_batch_path))

            with writer:
                writer.add_to_current_batch(sensor_name="test", data="pong,\n")

            # Check first (oldest) file has now been deleted.
            self.assertFalse(os.path.exists(first_batch_path))

            # Check the second file has not been deleted.
            self.assertTrue(
                os.path.exists(os.path.join(temporary_directory, writer._session_subdirectory, "window-1.json"))
            )


class TestBatchingUploader(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        cls.storage_client = GoogleCloudStorageClient(project_name=TEST_PROJECT_NAME)

    def test_data_is_batched(self):
        """Test that data is batched as expected."""
        uploader = BatchingUploader(
            sensor_names=["test"],
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
            batch_interval=600,
            session_subdirectory="this-session",
            output_directory=tempfile.TemporaryDirectory().name,
        )

        uploader.add_to_current_batch(sensor_name="test", data="blah,")
        self.assertEqual(uploader.current_batch["test"], ["blah,"])

    def test_data_is_uploaded_in_batches_and_can_be_retrieved_from_cloud_storage(self):
        """Test that data is uploaded in batches of whatever units it is added in, and that it can be retrieved from
        cloud storage.
        """
        uploader = BatchingUploader(
            sensor_names=["test"],
            project_name=TEST_PROJECT_NAME,
            bucket_name=TEST_BUCKET_NAME,
            batch_interval=0.01,
            session_subdirectory="this-session",
            output_directory=tempfile.TemporaryDirectory().name,
        )

        with uploader:
            uploader.add_to_current_batch(sensor_name="test", data="ping")
            uploader.add_to_current_batch(sensor_name="test", data="pong")
            self.assertEqual(len(uploader.current_batch["test"]), 2)

            time.sleep(uploader.batch_interval)

            uploader.add_to_current_batch(sensor_name="test", data="ding")
            uploader.add_to_current_batch(sensor_name="test", data="dong")
            self.assertEqual(len(uploader.current_batch["test"]), 2)

            time.sleep(uploader.batch_interval)

        self.assertEqual(len(uploader.current_batch["test"]), 0)

        self.assertEqual(
            json.loads(
                self.storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(
                        uploader.output_directory, uploader._session_subdirectory, "window-0.json"
                    ),
                )
            ),
            {"test": ["ping", "pong"]},
        )

        self.assertEqual(
            json.loads(
                self.storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(
                        uploader.output_directory, uploader._session_subdirectory, "window-1.json"
                    ),
                )
            ),
            {"test": ["ding", "dong"]},
        )

    def test_batch_is_written_to_disk_if_upload_fails(self):
        """Test that a batch is written to disk if it fails to upload to the cloud."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            with mock.patch.object(Blob, "upload_from_string", Exception):
                uploader = BatchingUploader(
                    sensor_names=["test"],
                    project_name=TEST_PROJECT_NAME,
                    bucket_name=TEST_BUCKET_NAME,
                    batch_interval=0.01,
                    session_subdirectory="this-session",
                    output_directory=temporary_directory,
                    upload_backup_files=False,
                )

                with uploader:
                    uploader.add_to_current_batch(sensor_name="test", data="ping")
                    uploader.add_to_current_batch(sensor_name="test", data="pong")

            # Check that the upload has failed.
            with self.assertRaises(google.api_core.exceptions.NotFound):
                self.storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(
                        uploader.output_directory, uploader._session_subdirectory, "window-0.json"
                    ),
                )

            # Check that a backup file has been written.
            with open(
                os.path.join(temporary_directory, ".backup", uploader._session_subdirectory, "window-0.json")
            ) as f:
                self.assertEqual(json.load(f), {"test": ["ping", "pong"]})

    def test_backup_files_are_uploaded_on_next_upload_attempt(self):
        """Test that backup files from a failed upload are uploaded on the next upload attempt."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            with mock.patch.object(Blob, "upload_from_string", Exception):
                uploader = BatchingUploader(
                    sensor_names=["test"],
                    project_name=TEST_PROJECT_NAME,
                    bucket_name=TEST_BUCKET_NAME,
                    batch_interval=10,
                    session_subdirectory="this-session",
                    output_directory=temporary_directory,
                    upload_backup_files=True,
                )

                with uploader:
                    uploader.add_to_current_batch(sensor_name="test", data="ping")
                    uploader.add_to_current_batch(sensor_name="test", data="pong")

            # Check that the upload has failed.
            with self.assertRaises(google.api_core.exceptions.NotFound):
                self.storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(
                        uploader.output_directory, uploader._session_subdirectory, "window-0.json"
                    ),
                )

            backup_path = os.path.join(temporary_directory, ".backup", uploader._session_subdirectory, "window-0.json")

            # Check that a backup file has been written.
            with open(backup_path) as f:
                self.assertEqual(json.load(f), {"test": ["ping", "pong"]})

            with uploader:
                uploader.add_to_current_batch(sensor_name="test", data=["ding", "dong"])

        # Check that both batches are now in cloud storage.
        self.assertEqual(
            json.loads(
                self.storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(
                        uploader.output_directory, uploader._session_subdirectory, "window-0.json"
                    ),
                )
            ),
            {"test": ["ping", "pong"]},
        )

        self.assertEqual(
            json.loads(
                self.storage_client.download_as_string(
                    bucket_name=TEST_BUCKET_NAME,
                    path_in_bucket=storage.path.join(
                        uploader.output_directory, uploader._session_subdirectory, "window-1.json"
                    ),
                )
            ),
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
            batch_interval=0.01,
            session_subdirectory="this-session",
            output_directory=tempfile.TemporaryDirectory().name,
            metadata={"big": "rock"},
        )

        with uploader:
            uploader.add_to_current_batch(sensor_name="test", data="ping,")

        metadata = self.storage_client.get_metadata(
            bucket_name=TEST_BUCKET_NAME,
            path_in_bucket=storage.path.join(
                uploader.output_directory, uploader._session_subdirectory, "window-0.json"
            ),
        )

        self.assertEqual(metadata["custom_metadata"], {"big": "rock"})
