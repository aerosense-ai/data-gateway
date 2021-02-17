import copy
import json
import logging
import os
import time
from octue.utils.cloud.persistence import GoogleCloudStorageClient

import abc


logger = logging.getLogger(__name__)


DEFAULT_OUTPUT_DIRECTORY = "data_gateway"


def calculate_disk_usage(path):
    """Calculate the the disk usage in bytes of the file or directory at the given path. The disk usage is calculated
    recursively (i.e. if a directory is given, it includes the usage of all the files and subdirectories and so on of
    the directory).

    :param str path:
    :return float:
    """
    if os.path.isfile(path):
        return os.path.getsize(path)

    return sum(calculate_disk_usage(item.path) for item in os.scandir(path))


def get_oldest_file_in_directory(path):
    """Get the oldest file in a directory. This is not a recursive function.

    :param str path:
    :return str|None:
    """
    try:
        return min(os.scandir(path), key=os.path.getctime).path
    except ValueError:
        return None


class TimeBatcher:
    def __init__(self, sensor_names, batch_interval, output_directory=DEFAULT_OUTPUT_DIRECTORY):
        """Instantiate a TimeBatcher. The batcher will group the data given to it into batches of the duration of the
        time interval.

        :param iter(str) sensor_names:
        :param float batch_interval: time interval with which to batch data (in seconds)
        :param str output_directory: directory to write batches to
        :return None:
        """
        self.current_batch = {name: [] for name in sensor_names}
        self.batch_interval = batch_interval
        self.output_directory = output_directory
        self.ready_batch = {}
        self._batch_number = 0
        self._start_time = time.perf_counter()
        self._batch_prefix = "batch"
        self._backup_directory = os.path.join(self.output_directory, ".backup")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.force_persist()

    def add_to_current_batch(self, sensor_name, data):
        """Add serialised data (a string) to the current batch for the given sensor name.

        :param str sensor_name:
        :param str data:
        :return None:
        """
        # Finalise the batch and persist it if enough time has elapsed.
        if time.perf_counter() - self._start_time >= self.batch_interval:
            self.finalise_current_batch()
            self._persist_batch()
            self._prepare_for_next_batch()

        # Then add data to the current/new batch.
        self.current_batch[sensor_name].append(data)

    def finalise_current_batch(self):
        """Finalise the current batch for the given sensor name. This puts the current batch into the queue of ready
        batches, resets the clock for the next batch, and increases the batch number for it.

        :return None:
        """
        for sensor_name, data in self.current_batch.items():
            if data:
                self.ready_batch[sensor_name] = "".join(copy.deepcopy(data))
            data.clear()

    def force_persist(self):
        """Persist all current batches, regardless of whether a complete batch interval has passed.

        :return None:
        """
        self.finalise_current_batch()
        self._persist_batch()
        self._prepare_for_next_batch()

    @abc.abstractmethod
    def _persist_batch(self):
        """Persist the batch to whatever medium is required (e.g. to disk, to a database, or to the cloud).

        :return None:
        """
        pass

    def _prepare_for_next_batch(self):
        self._batch_number += 1
        self.ready_batch = {}
        self._start_time = time.perf_counter()

    def _generate_batch_path(self, backup=False):
        """Generate the path that the batch should be persisted to.

        :param bool backup:
        :return str:
        """
        filename = f"batch-{self._batch_number}.json"

        if backup:
            return "/".join((self._backup_directory, filename))

        return "/".join((self.output_directory, filename))


class BatchingFileWriter(TimeBatcher):
    def __init__(
        self, sensor_names, batch_interval, output_directory=DEFAULT_OUTPUT_DIRECTORY, storage_limit=1024 ** 3
    ):
        """Initialise a file writer that batches the data given to it over time into batches of the duration of the
        given time interval, saving each batch to disk.

        :param iter(str) sensor_names:
        :param float batch_interval:
        :param str output_directory:
        :param int storage_limit: storage limit in bytes (default is 1 GB)
        :return None:
        """
        self.storage_limit = storage_limit
        super().__init__(sensor_names, batch_interval, output_directory)

    def _persist_batch(self, batch=None, backup=False):
        """Write a batch of serialised data to disk, deleting the oldest batch first if the storage limit has been
        reached.

        :param dict batch:
        :param bool backup:
        :return None:
        """
        self._manage_storage(backup=backup)
        batch_path = os.path.abspath(os.path.join(".", self._generate_batch_path(backup=backup)))
        batch_directory = os.path.split(batch_path)[0]

        if not os.path.exists(batch_directory):
            os.makedirs(batch_directory)

        with open(batch_path, "w") as f:
            json.dump(batch or self.ready_batch, f)

    def _manage_storage(self, backup=False):
        """Check if the output or backup directory has reached its storage limit and, if it has, delete the oldest
        batch.

        :param bool backup:
        :return None:
        """
        if backup:
            directory_to_check = self._backup_directory
        else:
            directory_to_check = self.output_directory

        if calculate_disk_usage(self.output_directory) >= self.storage_limit:
            logger.warning("Storage limit reached (%s MB) - deleting oldest batch.", self.storage_limit / 1024 ** 2)
            oldest_file = get_oldest_file_in_directory(directory_to_check)
            os.remove(oldest_file)

        elif calculate_disk_usage(self.output_directory) >= 0.9 * self.storage_limit:
            logger.warning("90% of storage limit reached - %s MB remaining.", 0.1 * self.storage_limit / 1024 ** 2)


class BatchingUploader(TimeBatcher):
    def __init__(
        self,
        sensor_names,
        project_name,
        bucket_name,
        batch_interval,
        output_directory=DEFAULT_OUTPUT_DIRECTORY,
        upload_timeout=60,
        upload_backup_files=True,
    ):
        """Initialise a BatchingUploader with a bucket from a given GCP project. The uploader will upload the data given
        to it to a GCP storage bucket at the given interval of time.

        :param iter(dict) sensor_names: a dictionary with "name" and "extension" entries
        :param str project_name:
        :param str bucket_name:
        :param float batch_interval: time interval with which to batch data (in seconds)
        :param str output_directory:
        :param float upload_timeout:
        :param bool upload_backup_files:
        :return None:
        """
        self.project_name = project_name
        self.client = GoogleCloudStorageClient(project_name=project_name)
        self.bucket_name = bucket_name
        self.upload_timeout = upload_timeout
        self.upload_backup_files = upload_backup_files
        self._backup_writer = BatchingFileWriter(sensor_names, batch_interval, output_directory)
        super().__init__(sensor_names, batch_interval, output_directory)

    def _persist_batch(self):
        """Upload serialised data to a path in the bucket. If the batch fails to upload, it is instead written to disk.

        :return None:
        """
        try:
            self.client.upload_from_string(
                serialised_data=json.dumps(self.ready_batch),
                bucket_name=self.bucket_name,
                path_in_bucket=self._generate_batch_path(),
                timeout=self.upload_timeout,
            )

        except Exception:
            logger.warning(
                "Upload of batch failed - writing to disk at %r instead.",
                self._backup_writer._generate_batch_path(backup=True),
            )

            self._backup_writer._persist_batch(batch=self.ready_batch, backup=True)
            return

        if self.upload_backup_files:
            self._attempt_to_upload_backup_files()

    def _attempt_to_upload_backup_files(self):
        """Check for backup files and attempt to upload them to cloud storage again."""
        if os.path.exists(self._backup_directory):
            backup_filenames = os.listdir(self._backup_directory)

            if not backup_filenames:
                return

            for filename in backup_filenames:

                if not filename.startswith(self._batch_prefix):
                    continue

                local_path = os.path.join(self._backup_directory, filename)
                path_in_bucket = "/".join((self.output_directory, filename))

                try:
                    self.client.upload_file(
                        local_path=local_path,
                        bucket_name=self.bucket_name,
                        path_in_bucket=path_in_bucket,
                        timeout=self.upload_timeout,
                    )

                except Exception:
                    return

                os.remove(local_path)
