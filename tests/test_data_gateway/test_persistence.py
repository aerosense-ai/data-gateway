import csv
import datetime
import json
import os
import shutil
import tempfile
import time
import uuid
from unittest import mock

import google.api_core.exceptions
from google.cloud.storage.blob import Blob
from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient

from data_gateway.persistence import BatchingFileWriter, BatchingUploader
from tests import TEST_BUCKET_NAME
from tests.base import BaseTestCase


START_TIMESTAMP = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).timestamp()


class TestBatchingWriter(BaseTestCase):
    def test_data_is_batched(self):
        """Test that data is batched as expected."""
        node_id = "0"

        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = BatchingFileWriter(
                node_ids=[node_id],
                output_directory=os.path.join(temporary_directory, "this-session"),
                window_size=600,
            )

        writer.add_to_current_window(node_id=node_id, sensor_name="test", data="blah")
        self.assertEqual(writer.current_window[node_id]["test"], ["blah"])

    def test_data_is_written_to_disk_in_windows(self):
        """Test that data is written to disk as time windows."""
        node_id = "0"

        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = BatchingFileWriter(
                node_ids=[node_id],
                output_directory=os.path.join(temporary_directory, "this-session"),
                window_size=0.01,
            )

            with writer:
                writer.add_to_current_window(node_id=node_id, sensor_name="test", data="ping")
                writer.add_to_current_window(node_id=node_id, sensor_name="test", data="pong")
                self.assertEqual(len(writer.current_window[node_id]["test"]), 2)
                time.sleep(writer.window_size * 2)

                writer.add_to_current_window(node_id=node_id, sensor_name="test", data="ding")
                writer.add_to_current_window(node_id=node_id, sensor_name="test", data="dong")
                self.assertEqual(len(writer.current_window[node_id]["test"]), 2)

            self.assertEqual(len(writer.current_window[node_id]["test"]), 0)

            with open(os.path.join(writer.output_directory, "window-0.json")) as f:
                self.assertEqual(json.load(f)[node_id], {"test": ["ping", "pong"]})

            with open(os.path.join(writer.output_directory, "window-1.json")) as f:
                self.assertEqual(json.load(f)[node_id], {"test": ["ding", "dong"]})

    def test_oldest_window_is_deleted_when_storage_limit_reached(self):
        """Check that (only) the oldest window is deleted when the storage limit is reached."""
        node_id = "0"

        with tempfile.TemporaryDirectory() as temporary_directory:

            writer = BatchingFileWriter(
                node_ids=[node_id],
                output_directory=os.path.join(temporary_directory, "this-session"),
                window_size=0.01,
                storage_limit=1,
            )

            with writer:
                writer.add_to_current_window(node_id=node_id, sensor_name="test", data="ping")

            first_window_path = os.path.join(writer.output_directory, "window-0.json")

            # Check first file is written to disk.
            self.assertTrue(os.path.exists(first_window_path))

            with writer:
                writer.add_to_current_window(node_id=node_id, sensor_name="test", data="pong\n")

            # Check first (oldest) file has now been deleted.
            self.assertFalse(os.path.exists(first_window_path))

            # Check the second file has not been deleted.
            self.assertTrue(os.path.exists(os.path.join(writer.output_directory, "window-1.json")))

    def test_that_csv_files_are_written(self):
        """Test that data is written to disk as CSV-files if the `save_csv_files` option is `True`."""
        node_id = "0"

        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = BatchingFileWriter(
                node_ids=[node_id],
                output_directory=os.path.join(temporary_directory, "this-session"),
                save_csv_files=True,
                window_size=0.01,
            )

            with writer:
                writer.add_to_current_window(node_id=node_id, sensor_name="sensor1", data=[1, 2, 3])
                writer.add_to_current_window(node_id=node_id, sensor_name="sensor2", data=[1, 2, 3])
                writer.add_to_current_window(node_id=node_id, sensor_name="sensor1", data=[4, 5, 6])
                writer.add_to_current_window(node_id=node_id, sensor_name="sensor2", data=[4, 5, 6])

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
        cls.storage_client = GoogleCloudStorageClient()

    def test_data_is_batched(self):
        """Test that data is batched as expected."""
        node_id = "0"

        uploader = BatchingUploader(
            node_ids=[node_id],
            bucket_name=TEST_BUCKET_NAME,
            window_size=600,
            output_directory=storage.path.join(tempfile.TemporaryDirectory().name, "this-session"),
        )

        uploader.add_to_current_window(node_id=node_id, sensor_name="test", data="blah")
        self.assertEqual(uploader.current_window[node_id]["test"], ["blah"])

    def test_data_is_uploaded_in_windows_and_can_be_retrieved_from_cloud_storage(self):
        """Test that data is uploaded in time windows that can be retrieved from cloud storage."""
        node_id = "0"

        try:
            uploader = BatchingUploader(
                node_ids=[node_id],
                bucket_name=TEST_BUCKET_NAME,
                window_size=0.01,
                output_directory=f"this-session-{uuid.uuid4()}",
            )

            with uploader:
                uploader.add_to_current_window(node_id=node_id, sensor_name="test", data="ping")
                uploader.add_to_current_window(node_id=node_id, sensor_name="test", data="pong")
                self.assertEqual(len(uploader.current_window[node_id]["test"]), 2)

                time.sleep(uploader.window_size)

                uploader.add_to_current_window(node_id=node_id, sensor_name="test", data="ding")
                uploader.add_to_current_window(node_id=node_id, sensor_name="test", data="dong")
                self.assertEqual(len(uploader.current_window[node_id]["test"]), 2)

                time.sleep(uploader.window_size)

            self.assertEqual(len(uploader.current_window[node_id]["test"]), 0)

            self.assertEqual(
                json.loads(
                    self.storage_client.download_as_string(
                        cloud_path=storage.path.generate_gs_path(
                            TEST_BUCKET_NAME, uploader.output_directory, "window-0.json"
                        ),
                    )
                )[node_id],
                {"test": ["ping", "pong"]},
            )

            self.assertEqual(
                json.loads(
                    self.storage_client.download_as_string(
                        cloud_path=storage.path.generate_gs_path(
                            TEST_BUCKET_NAME, uploader.output_directory, "window-1.json"
                        ),
                    )
                )[node_id],
                {"test": ["ding", "dong"]},
            )

        finally:
            shutil.rmtree(uploader.output_directory)

    def test_window_is_written_to_disk_if_upload_fails(self):
        """Test that a window is written to disk if it fails to upload to the cloud."""
        node_id = "0"

        try:
            with mock.patch.object(
                Blob,
                "upload_from_string",
                side_effect=Exception("This is deliberately raised in this test to simulate cloud upload failing."),
            ):
                uploader = BatchingUploader(
                    node_ids=[node_id],
                    bucket_name=TEST_BUCKET_NAME,
                    window_size=0.01,
                    output_directory=f"this-session-{uuid.uuid4()}",
                    upload_backup_files=False,
                )

                with uploader:
                    uploader.add_to_current_window(node_id=node_id, sensor_name="test", data="ping")
                    uploader.add_to_current_window(node_id=node_id, sensor_name="test", data="pong")

            # Check that the upload has failed.
            with self.assertRaises(google.api_core.exceptions.NotFound):
                self.storage_client.download_as_string(
                    cloud_path=storage.path.generate_gs_path(
                        TEST_BUCKET_NAME, uploader.output_directory, "window-0.json"
                    ),
                )

            # Check that a backup file has been written.
            with open(os.path.join(uploader.output_directory, ".backup", "window-0.json")) as f:
                self.assertEqual(json.load(f)[node_id], {"test": ["ping", "pong"]})

        finally:
            shutil.rmtree(uploader.output_directory)

    def test_backup_files_are_uploaded_on_next_upload_attempt(self):
        """Test that backup files from a failed upload are uploaded on the next upload attempt."""
        node_id = "0"

        try:
            with mock.patch.object(
                Blob,
                "upload_from_string",
                side_effect=Exception("This is deliberately raised in this test to simulate cloud upload failing."),
            ):
                uploader = BatchingUploader(
                    node_ids=[node_id],
                    bucket_name=TEST_BUCKET_NAME,
                    window_size=10,
                    output_directory=f"this-session-{uuid.uuid4()}",
                    upload_backup_files=True,
                )

                with uploader:
                    uploader.add_to_current_window(node_id=node_id, sensor_name="test", data="ping")
                    uploader.add_to_current_window(node_id=node_id, sensor_name="test", data="pong")

            # Check that the upload has failed.
            with self.assertRaises(google.api_core.exceptions.NotFound):
                self.storage_client.download_as_string(
                    cloud_path=storage.path.generate_gs_path(
                        TEST_BUCKET_NAME, uploader.output_directory, "window-0.json"
                    ),
                )

            backup_path = os.path.join(uploader._backup_directory, "window-0.json")

            # Check that a backup file has been written.
            with open(backup_path) as f:
                self.assertEqual(json.load(f)[node_id], {"test": ["ping", "pong"]})

            with uploader:
                uploader.add_to_current_window(node_id=node_id, sensor_name="test", data=["ding", "dong"])

            # Check that both windows are now in cloud storage.
            self.assertEqual(
                json.loads(
                    self.storage_client.download_as_string(
                        cloud_path=storage.path.generate_gs_path(
                            TEST_BUCKET_NAME, uploader.output_directory, "window-0.json"
                        ),
                    )
                )[node_id],
                {"test": ["ping", "pong"]},
            )

            self.assertEqual(
                json.loads(
                    self.storage_client.download_as_string(
                        cloud_path=storage.path.generate_gs_path(
                            TEST_BUCKET_NAME, uploader.output_directory, "window-1.json"
                        ),
                    )
                )[node_id],
                {"test": [["ding", "dong"]]},
            )

            # Check that the backup file has been removed now it's been uploaded to cloud storage.
            self.assertFalse(os.path.exists(backup_path))

        finally:
            shutil.rmtree(uploader.output_directory)

    def test_metadata_is_added_to_uploaded_files(self):
        """Test that metadata is added to uploaded files and can be retrieved."""
        node_id = "0"

        uploader = BatchingUploader(
            node_ids=[node_id],
            bucket_name=TEST_BUCKET_NAME,
            window_size=0.01,
            output_directory=f"this-session-{uuid.uuid4()}",
            metadata={"big": "rock"},
        )

        with uploader:
            uploader.add_to_current_window(node_id=node_id, sensor_name="test", data="ping")

        metadata = self.storage_client.get_metadata(
            cloud_path=storage.path.generate_gs_path(TEST_BUCKET_NAME, uploader.output_directory, "window-0.json"),
        )

        self.assertEqual(metadata["custom_metadata"], {"big": "rock"})
