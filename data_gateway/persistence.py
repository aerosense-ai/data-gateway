import copy
import json
import logging
import os
import time
from octue.utils.cloud.persistence import GoogleCloudStorageClient

import abc


logger = logging.getLogger(__name__)


DEFAULT_OUTPUT_DIRECTORY = "data_gateway"


def calculate_disk_usage(path, filter=None):
    """Calculate the the disk usage in bytes of the file or directory at the given path. The disk usage is calculated
    recursively (i.e. if a directory is given, it includes the usage of all the files and subdirectories and so on of
    the directory). The files considered can be filtered by a callable that returns True for paths that should be
    considered and False for those that shouldn't.

    :param str path:
    :param callable|None filter:
    :return float:
    """
    if os.path.isfile(path):
        if filter is None:
            return os.path.getsize(path)

        if filter(path):
            return os.path.getsize(path)

        return 0

    return sum(calculate_disk_usage(item.path) for item in os.scandir(path))


def get_oldest_file_in_directory(path, filter=None):
    """Get the oldest file in a directory. This is not a recursive function. The files considered can be filtered by a
    callable that returns True for paths that should be considered and False for those that shouldn't.

    :param str path:
    :param callable|None filter:
    :return str|None:
    """
    if filter is None:
        contents = [item for item in os.scandir(path) if item.is_file()]
    else:
        contents = [item for item in os.scandir(path) if item.is_file() and filter(item.path)]

    try:
        return min(contents, key=os.path.getmtime).path
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
        """Prepare the batcher for the next batch.

        :return None:
        """
        self._batch_number += 1
        self.ready_batch = {}
        self._start_time = time.perf_counter()

    def _generate_batch_path(self, backup=False):
        """Generate the path that the batch should be persisted to.

        :param bool backup:
        :return str:
        """
        filename = f"{self._batch_prefix}-{self._batch_number}.json"

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

        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)

    def _persist_batch(self, batch=None, backup=False):
        """Write a batch of serialised data to disk, deleting the oldest batch first if the storage limit has been
        reached.

        :param dict|None batch:
        :param bool backup:
        :return None:
        """
        self._manage_storage(backup=backup)
        batch_path = os.path.abspath(os.path.join(".", self._generate_batch_path(backup=backup)))

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

        filter = lambda path: os.path.split(path)[-1].startswith("batch")  # noqa
        storage_limit_in_mb = self.storage_limit / 1024 ** 2

        if calculate_disk_usage(self.output_directory, filter) >= self.storage_limit:
            oldest_batch = get_oldest_file_in_directory(directory_to_check, filter)

            logger.warning(
                "Storage limit reached (%s MB) - deleting oldest batch (%r).",
                storage_limit_in_mb,
                oldest_batch,
            )

            os.remove(oldest_batch)

        elif calculate_disk_usage(self.output_directory, filter) >= 0.9 * self.storage_limit:
            logger.warning("90% of storage limit reached - %s MB remaining.", 0.1 * storage_limit_in_mb)


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
        to it to a GCP storage bucket at the given interval of time. If upload fails for a batch, it will be written to
        the backup directory. If the `upload_backup_files` flag is `True`, its upload will then be reattempted after the
        upload of each subsequent batch.

        :param iter(str) sensor_names:
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

        if not os.path.exists(self._backup_directory):
            os.makedirs(self._backup_directory)

    def _persist_batch(self):
        """Upload a batch to Google Cloud storage. If the batch fails to upload, it is instead written to disk.

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
        """Check for backup files and attempt to upload them to cloud storage again.

        :return None:
        """
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
