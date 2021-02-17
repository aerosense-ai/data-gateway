import json
import os
import tempfile
import time
import unittest
from unittest import mock
import google.api_core.exceptions
from gcloud_storage_emulator.server import create_server
from google.cloud import storage
from google.cloud.storage.blob import Blob
from octue.utils.cloud.credentials import GCPCredentialsManager
from octue.utils.cloud.persistence import GoogleCloudStorageClient

from data_gateway.persistence import (
    BatchingFileWriter,
    BatchingUploader,
    calculate_disk_usage,
    get_oldest_file_in_directory,
)


def _create_file_of_size(path, size):
    """Create a file at the given path of the given size in bytes.

    :param str path:
    :param int size:
    :return None:
    """
    with open(path, "wb") as f:
        f.truncate(size)


class TestCalculateDiskUsage(unittest.TestCase):
    def test_calculate_disk_usage_with_single_file(self):
        """Test that the disk usage of a single file is calculated correctly."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            dummy_file_path = os.path.join(temporary_directory, "dummy_file")
            _create_file_of_size(dummy_file_path, 1)

            self.assertEqual(calculate_disk_usage(dummy_file_path), 1)

    def test_with_filter_satisfied(self):
        """Test that files meeting a filter are included in the calculated size."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            dummy_file_path = os.path.join(temporary_directory, "dummy_file")
            _create_file_of_size(dummy_file_path, 1)

            self.assertEqual(
                calculate_disk_usage(dummy_file_path, filter=lambda path: os.path.split(path)[-1].startswith("dummy")),
                1,
            )

    def test_with_filter_unsatisfied(self):
        """Test that files not meeting a filter are not included in the calculated size."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            dummy_file_path = os.path.join(temporary_directory, "dummy_file")
            _create_file_of_size(dummy_file_path, 1)

            self.assertEqual(
                calculate_disk_usage(dummy_file_path, filter=lambda path: os.path.split(path)[-1].startswith("hello")),
                0,
            )

    def test_calculate_disk_usage_with_shallow_directory(self):
        """Test that the disk usage of a directory with only files in is calculated correctly."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            for i in range(5):
                path = os.path.join(temporary_directory, f"dummy_file_{i}")
                _create_file_of_size(path, 1)

            self.assertEqual(calculate_disk_usage(temporary_directory), 5)

    def test_calculate_disk_usage_with_directory_of_directories(self):
        """Test that the disk usage of a directory of directories with files in is calculated correctly."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            for i in range(5):
                directory_path = os.path.join(temporary_directory, f"directory_{i}")
                os.mkdir(directory_path)

                for i in range(5):
                    file_path = os.path.join(directory_path, f"dummy_file_{i}")
                    _create_file_of_size(file_path, 1)

            self.assertEqual(calculate_disk_usage(temporary_directory), 25)

    def test_calculate_disk_usage_with_directory_of_directories_and_files(self):
        """Test that the disk usage of a directory of directories with files in and files next to the directories is
        calculated correctly.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:

            for i in range(5):
                directory_path = os.path.join(temporary_directory, f"directory_{i}")
                os.mkdir(directory_path)

                adjacent_file_path = os.path.join(temporary_directory, f"adjacent_dummy_file_{i}")
                _create_file_of_size(adjacent_file_path, 2)

                for i in range(5):
                    file_path = os.path.join(directory_path, f"dummy_file_{i}")
                    _create_file_of_size(file_path, 1)

            self.assertEqual(calculate_disk_usage(temporary_directory), 35)


class TestGetOldestFileInDirectory(unittest.TestCase):
    def test_get_oldest_file_in_directory(self):
        """Test that the path of the oldest file in a directory is retrieved."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            for i in range(5):
                directory_path = os.path.join(temporary_directory, f"directory_{i}")
                os.mkdir(directory_path)

            self.assertEqual(
                get_oldest_file_in_directory(temporary_directory), os.path.join(temporary_directory, "directory_0")
            )

    def test_empty_directory_results_in_none(self):
        """Test that None is returned for an empty directory."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            self.assertEqual(get_oldest_file_in_directory(temporary_directory), None)


class TestBatchingWriter(unittest.TestCase):
    def test_data_is_batched(self):
        """Test that data is added to the correct stream as expected."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = BatchingFileWriter(
                sensor_names=["test"],
                output_directory=temporary_directory,
                batch_interval=600,
            )

        writer.add_to_current_batch(sensor_name="test", data="blah,")
        self.assertEqual(writer.current_batch["test"], ["blah,"])

    def test_data_is_written_to_disk_in_batches(self):
        """Test that data is written to disk in batches of whatever units it is added to the stream in."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            writer = BatchingFileWriter(
                sensor_names=["test"],
                output_directory=temporary_directory,
                batch_interval=0.01,
            )

            with writer:
                writer.add_to_current_batch(sensor_name="test", data="ping,")
                writer.add_to_current_batch(sensor_name="test", data="pong,\n")
                self.assertEqual(len(writer.current_batch["test"]), 2)
                time.sleep(0.01)

                writer.add_to_current_batch(sensor_name="test", data="ding,")
                self.assertEqual(len(writer.current_batch["test"]), 1)

                writer.add_to_current_batch(sensor_name="test", data="dong,\n")
                time.sleep(0.01)

            self.assertEqual(len(writer.current_batch["test"]), 0)

            with open(os.path.join(temporary_directory, "batch-0.json")) as f:
                data = json.load(f)
                self.assertEqual(data, {"test": "ping,pong,\n"})

            with open(os.path.join(temporary_directory, "batch-1.json")) as f:
                data = json.load(f)
                self.assertEqual(data, {"test": "ding,dong,\n"})


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
            sensor_names=["test"],
            project_name=self.TEST_PROJECT_NAME,
            bucket_name=self.TEST_BUCKET_NAME,
            batch_interval=600,
            output_directory=tempfile.TemporaryDirectory().name,
        )

        uploader.add_to_current_batch(sensor_name="test", data="blah,")
        self.assertEqual(uploader.current_batch["test"], ["blah,"])

    def test_data_is_uploaded_in_batches_and_can_be_retrieved_from_cloud_storage(self):
        """Test that data is uploaded in batches of whatever units it is added to the stream in, and that it can be
        retrieved from cloud storage.
        """
        uploader = BatchingUploader(
            sensor_names=["test"],
            project_name=self.TEST_PROJECT_NAME,
            bucket_name=self.TEST_BUCKET_NAME,
            batch_interval=0.01,
            output_directory=tempfile.TemporaryDirectory().name,
        )

        with uploader:
            uploader.add_to_current_batch(sensor_name="test", data="ping,")
            uploader.add_to_current_batch(sensor_name="test", data="pong,\n")
            self.assertEqual(len(uploader.current_batch["test"]), 2)
            time.sleep(0.01)

            uploader.add_to_current_batch(sensor_name="test", data="ding,")
            uploader.add_to_current_batch(sensor_name="test", data="dong,\n")
            time.sleep(0.01)

        self.assertEqual(len(uploader.current_batch["test"]), 0)

        self.assertEqual(
            json.loads(
                GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME).download_as_string(
                    bucket_name=self.TEST_BUCKET_NAME,
                    path_in_bucket=f"{uploader.output_directory}/batch-0.json",
                )
            ),
            {"test": "ping,pong,\n"},
        )

        self.assertEqual(
            json.loads(
                GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME).download_as_string(
                    bucket_name=self.TEST_BUCKET_NAME,
                    path_in_bucket=f"{uploader.output_directory}/batch-1.json",
                )
            ),
            {"test": "ding,dong,\n"},
        )

    def test_batch_is_written_to_disk_if_upload_fails(self):
        """Test that a batch is written to disk if it fails to upload to the cloud."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            with mock.patch.object(Blob, "upload_from_string", Exception):
                uploader = BatchingUploader(
                    sensor_names=["test"],
                    project_name=self.TEST_PROJECT_NAME,
                    bucket_name=self.TEST_BUCKET_NAME,
                    batch_interval=0.01,
                    output_directory=temporary_directory,
                    upload_backup_files=False,
                )

                with uploader:
                    uploader.add_to_current_batch(sensor_name="test", data="ping,")
                    uploader.add_to_current_batch(sensor_name="test", data="pong,\n")

            # Check that the upload has failed.
            with self.assertRaises(google.api_core.exceptions.NotFound):
                GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME).download_as_string(
                    bucket_name=self.TEST_BUCKET_NAME,
                    path_in_bucket=f"{uploader.output_directory}/batch-0.json",
                )

            # Check that a backup file has been written.
            with open(os.path.join(temporary_directory, ".backup", "batch-0.json")) as f:
                data = json.load(f)
                self.assertEqual(data, {"test": "ping,pong,\n"})

    def test_backup_files_are_uploaded_on_next_upload_attempt(self):
        """Test that backup files from a failed upload are uploaded on the next upload attempt."""
        with tempfile.TemporaryDirectory() as temporary_directory:

            with mock.patch.object(Blob, "upload_from_string", Exception):
                uploader = BatchingUploader(
                    sensor_names=["test"],
                    project_name=self.TEST_PROJECT_NAME,
                    bucket_name=self.TEST_BUCKET_NAME,
                    batch_interval=1,
                    output_directory=temporary_directory,
                    upload_backup_files=True,
                )

                with uploader:
                    uploader.add_to_current_batch(sensor_name="test", data="ping,")
                    uploader.add_to_current_batch(sensor_name="test", data="pong,\n")

            # Check that the upload has failed.
            with self.assertRaises(google.api_core.exceptions.NotFound):
                GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME).download_as_string(
                    bucket_name=self.TEST_BUCKET_NAME,
                    path_in_bucket=f"{uploader.output_directory}/batch-0.json",
                )

            backup_path = os.path.join(temporary_directory, ".backup", "batch-0.json")

            # Check that a backup file has been written.
            with open(backup_path) as f:
                data = json.load(f)
                self.assertEqual(data, {"test": "ping,pong,\n"})

            with uploader:
                uploader.add_to_current_batch(sensor_name="test", data="ding,dong,\n")

            # Check that the backup file has now been removed after it's been uploaded to cloud storage.
            self.assertFalse(os.path.exists(backup_path))

        # Check that both batches are now in cloud storage.
        self.assertEqual(
            json.loads(
                GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME).download_as_string(
                    bucket_name=self.TEST_BUCKET_NAME,
                    path_in_bucket=f"{uploader.output_directory}/batch-0.json",
                )
            ),
            {"test": "ping,pong,\n"},
        )

        self.assertEqual(
            json.loads(
                GoogleCloudStorageClient(project_name=self.TEST_PROJECT_NAME).download_as_string(
                    bucket_name=self.TEST_BUCKET_NAME,
                    path_in_bucket=f"{uploader.output_directory}/batch-1.json",
                )
            ),
            {"test": "ding,dong,\n"},
        )
