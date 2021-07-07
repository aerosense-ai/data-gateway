import copy
import json
import logging
import os
import time
from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.utils.persistence import calculate_disk_usage, get_oldest_file_in_directory

import abc


logger = logging.getLogger(__name__)


DEFAULT_OUTPUT_DIRECTORY = "data_gateway"


class NoOperationContextManager:
    """A no-operation context manager that can be used to fill in for cases where the context-managed object is not
    needed but the context-managed block is.

    :return None:
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class TimeBatcher:
    """A batcher that groups the data given to it into batches of the duration of the time interval.

    :param iter(str) sensor_names: names of sensors to make batches for
    :param float batch_interval: time interval with which to batch data (in seconds)
    :param str session_subdirectory: directory within output directory to persist into
    :param str output_directory: directory to write batches to
    :return None:
    """

    def __init__(self, sensor_names, batch_interval, session_subdirectory, output_directory=DEFAULT_OUTPUT_DIRECTORY):
        self.current_batch = {"sensor_time_offset": None, "sensor_data": {name: [] for name in sensor_names}}
        self.batch_interval = batch_interval
        self.output_directory = output_directory
        self.ready_batch = {"sensor_time_offset": None, "sensor_data": {}}
        self._session_subdirectory = session_subdirectory
        self._start_time = time.perf_counter()
        self._batch_number = 0
        self._batch_prefix = "window"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.force_persist()

    def add_to_current_batch(self, sensor_name, data):
        """Add serialised data (a string) to the current batch for the given sensor name.

        :param str sensor_name: name of sensor
        :param iter data: data to add to batch
        :return None:
        """
        # Finalise the batch and persist it if enough time has elapsed.
        if time.perf_counter() - self._start_time >= self.batch_interval:
            self.finalise_current_batch()
            self._persist_batch()
            self._prepare_for_next_batch()

        # Then add data to the current/new batch.
        self.current_batch["sensor_data"][sensor_name].append(data)

    def finalise_current_batch(self):
        """Finalise the current batch for the given sensor name. This puts the current batch into the queue of ready
        batches, resets the clock for the next batch, and increases the batch number for it.

        :return None:
        """
        for sensor_name, data in self.current_batch["sensor_data"].items():
            if data:
                self.ready_batch["sensor_data"][sensor_name] = copy.deepcopy(data)
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
        self.ready_batch["sensor_data"] = {}
        self._start_time = time.perf_counter()

    @abc.abstractmethod
    def _generate_batch_path(self):
        """Generate the path that the batch should be persisted to. This should start by joining the output directory
        and the session subdirectory.

        :return str:
        """
        pass


class BatchingFileWriter(TimeBatcher):
    """A file writer that batches the data given to it over time into batches of the duration of the given time
    interval, saving each batch to disk.

    :param iter(str) sensor_names: names of sensors to make batches for
    :param float batch_interval: time interval with which to batch data (in seconds)
    :param str session_subdirectory: directory within output directory to persist into
    :param str output_directory: directory to write batches to
    :param int storage_limit: storage limit in bytes (default is 1 GB)
    :return None:
    """

    def __init__(
        self,
        sensor_names,
        batch_interval,
        session_subdirectory,
        output_directory=DEFAULT_OUTPUT_DIRECTORY,
        storage_limit=1024 ** 3,
    ):
        self.storage_limit = storage_limit
        super().__init__(sensor_names, batch_interval, session_subdirectory, output_directory)
        os.makedirs(os.path.join(self.output_directory, self._session_subdirectory), exist_ok=True)

    def _persist_batch(self, batch=None):
        """Write a batch of serialised data to disk, deleting the oldest batch first if the storage limit has been
        reached.

        :param dict|None batch: batch to persist
        :return None:
        """
        self._manage_storage()
        batch_path = os.path.abspath(os.path.join(".", self._generate_batch_path()))

        with open(batch_path, "w") as f:
            json.dump(batch or self.ready_batch, f)

        logger.info(f"{self._batch_prefix.capitalize()} {self._batch_number} written to disk.")

    def _manage_storage(self):
        """Check if the output directory has reached its storage limit and, if it has, delete the oldest batch.

        :return None:
        """
        session_directory = os.path.join(self.output_directory, self._session_subdirectory)

        filter = lambda path: os.path.split(path)[-1].startswith("window")  # noqa
        storage_limit_in_mb = self.storage_limit / 1024 ** 2

        if calculate_disk_usage(session_directory, filter) >= self.storage_limit:
            oldest_batch = get_oldest_file_in_directory(session_directory, filter)

            logger.warning(
                "Storage limit reached (%s MB) - deleting oldest batch (%r).",
                storage_limit_in_mb,
                oldest_batch,
            )

            os.remove(oldest_batch)

        elif calculate_disk_usage(session_directory, filter) >= 0.9 * self.storage_limit:
            logger.warning("90% of storage limit reached - %s MB remaining.", 0.1 * storage_limit_in_mb)

    def _generate_batch_path(self):
        """Generate the path that the batch should be persisted to.

        :return str:
        """
        filename = f"{self._batch_prefix}-{self._batch_number}.json"
        return os.path.join(self.output_directory, self._session_subdirectory, filename)


class BatchingUploader(TimeBatcher):
    """A Google Cloud Storage uploader that will upload the data given to it to a GCP storage bucket at the given
    interval of time. If upload fails for a batch, it will be written to the backup directory. If the
    `upload_backup_files` flag is `True`, its upload will then be reattempted after the upload of each subsequent batch.

    :param iter(str) sensor_names: names of sensors to make batches for
    :param str project_name: name of Google Cloud project to upload to
    :param str bucket_name: name of Google Cloud bucket to upload to
    :param float batch_interval: time interval with which to batch data (in seconds)
    :param str session_subdirectory: directory within output directory to persist into
    :param str output_directory: directory to write batches to
    :param float upload_timeout: time after which to give up trying to upload to the cloud
    :param bool upload_backup_files: attempt to upload backed-up batches on next batch upload
    :return None:
    """

    def __init__(
        self,
        sensor_names,
        project_name,
        bucket_name,
        batch_interval,
        session_subdirectory,
        output_directory=DEFAULT_OUTPUT_DIRECTORY,
        metadata=None,
        upload_timeout=60,
        upload_backup_files=True,
    ):
        self.project_name = project_name
        self.client = GoogleCloudStorageClient(project_name=project_name)
        self.bucket_name = bucket_name
        self.metadata = metadata or {}
        self.upload_timeout = upload_timeout
        self.upload_backup_files = upload_backup_files
        super().__init__(sensor_names, batch_interval, session_subdirectory, output_directory)
        self._backup_directory = os.path.join(self.output_directory, ".backup")
        self._backup_writer = BatchingFileWriter(
            sensor_names, batch_interval, session_subdirectory, output_directory=self._backup_directory
        )

    def _persist_batch(self):
        """Upload a batch to Google Cloud storage. If the batch fails to upload, it is instead written to disk.

        :return None:
        """
        try:
            self.client.upload_from_string(
                string=json.dumps(self.ready_batch),
                bucket_name=self.bucket_name,
                path_in_bucket=self._generate_batch_path(),
                metadata=self.metadata,
                timeout=self.upload_timeout,
            )

        except Exception:  # noqa
            logger.warning(
                "Upload of batch failed - writing to disk at %r instead.",
                self._backup_writer._generate_batch_path(),
            )

            self._backup_writer._persist_batch(batch=self.ready_batch)
            return

        logger.info(f"{self._batch_prefix.capitalize()} {self._batch_number} uploaded to cloud.")

        if self.upload_backup_files:
            self._attempt_to_upload_backup_files()

    def _generate_batch_path(self):
        """Generate the path that the batch should be persisted to.

        :param bool backup: generate batch path for a backup
        :return str:
        """
        filename = f"{self._batch_prefix}-{self._batch_number}.json"
        return storage.path.join(self.output_directory, self._session_subdirectory, filename)

    def _attempt_to_upload_backup_files(self):
        """Check for backup files and attempt to upload them to cloud storage again.

        :return None:
        """
        backup_filenames = os.listdir(os.path.join(self._backup_directory, self._session_subdirectory))

        if not backup_filenames:
            return

        for filename in backup_filenames:

            if not filename.startswith(self._batch_prefix):
                continue

            local_path = os.path.join(self._backup_directory, self._session_subdirectory, filename)
            path_in_bucket = storage.path.join(self.output_directory, self._session_subdirectory, filename)

            try:
                self.client.upload_file(
                    local_path=local_path,
                    bucket_name=self.bucket_name,
                    path_in_bucket=path_in_bucket,
                    timeout=self.upload_timeout,
                    metadata=self.metadata,
                )

            except Exception:
                return

            os.remove(local_path)
