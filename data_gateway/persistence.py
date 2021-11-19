import abc
import copy
import csv
import json
import logging
import os
import time

from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.utils.persistence import calculate_disk_usage, get_oldest_file_in_directory


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
    """A batcher that groups the given data into time windows.

    :param iter(str) sensor_names: names of sensors to group data for
    :param float window_size: length of time window in seconds
    :param str session_subdirectory: directory within output directory to persist into
    :param str output_directory: directory to write windows to
    :return None:
    """

    def __init__(
        self,
        sensor_names,
        window_size,
        session_subdirectory,
        output_directory=DEFAULT_OUTPUT_DIRECTORY,
        save_csv_files=None,
    ):
        self.current_window = {"sensor_time_offset": None, "sensor_data": {name: [] for name in sensor_names}}
        self.window_size = window_size
        self.output_directory = output_directory
        self.ready_window = {"sensor_time_offset": None, "sensor_data": {}}
        self._session_subdirectory = session_subdirectory
        self._save_csv_files = save_csv_files
        self._start_time = time.perf_counter()
        self._window_number = 0
        self._file_prefix = "window"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.force_persist()

    def add_to_current_window(self, sensor_name, data):
        """Add serialised data (a string) to the current window for the given sensor name.

        :param str sensor_name: name of sensor
        :param iter data: data to add to window
        :return None:
        """
        # Finalise the window and persist it if enough time has elapsed.
        if time.perf_counter() - self._start_time >= self.window_size:
            self.finalise_current_window()
            self._persist_window()
            self._prepare_for_next_window()

        # Then add data to the current/new window.
        self.current_window["sensor_data"][sensor_name].append(data)

    def finalise_current_window(self):
        """Finalise the current window for the given sensor name. This puts the current window into the queue of ready
        windows, resets the clock for the next window, and increases the window number for it.

        :return None:
        """
        for sensor_name, data in self.current_window["sensor_data"].items():
            if data:
                self.ready_window["sensor_data"][sensor_name] = copy.deepcopy(data)
                data.clear()

    def force_persist(self):
        """Persist all current windows, regardless of whether a complete time window has passed.

        :return None:
        """
        self.finalise_current_window()
        self._persist_window()
        self._prepare_for_next_window()

    @abc.abstractmethod
    def _persist_window(self):
        """Persist the window to whatever medium is required (e.g. to disk, to a database, or to the cloud).

        :return None:
        """
        pass

    def _prepare_for_next_window(self):
        """Prepare the batcher for the next window.

        :return None:
        """
        self._window_number += 1
        self.ready_window["sensor_data"] = {}
        self._start_time = time.perf_counter()

    @abc.abstractmethod
    def _generate_window_path(self):
        """Generate the path that the window should be persisted to. This should start by joining the output directory
        and the session subdirectory.

        :return str:
        """
        pass


class BatchingFileWriter(TimeBatcher):
    """A file writer that groups the given into time windows, saving each window to disk.

    :param iter(str) sensor_names: names of sensors to make windows for
    :param float window_size:     :param float window_size: length of time window in seconds
    :param str session_subdirectory: directory within output directory to persist into
    :param str output_directory: directory to write windows to
    :param int storage_limit: storage limit in bytes (default is 1 GB)
    :return None:
    """

    def __init__(
        self,
        sensor_names,
        window_size,
        session_subdirectory,
        save_csv_files=None,
        output_directory=DEFAULT_OUTPUT_DIRECTORY,
        storage_limit=1024 ** 3,
    ):
        self.storage_limit = storage_limit
        super().__init__(sensor_names, window_size, session_subdirectory, output_directory, save_csv_files)
        os.makedirs(os.path.join(self.output_directory, self._session_subdirectory), exist_ok=True)

    def _persist_window(self, window=None):
        """Write a window of serialised data to disk, deleting the oldest window first if the storage limit has been
        reached.

        :param dict|None window: window to persist
        :return None:
        """
        self._manage_storage()
        window_path = os.path.abspath(os.path.join(".", self._generate_window_path()))

        with open(window_path, "w") as f:
            json.dump(window or self.ready_window, f)

        logger.info(f"{self._file_prefix.capitalize()} {self._window_number} written to disk.")

        if self._save_csv_files:
            for sensor in window or self.ready_window["sensor_data"]:
                csv_path = os.path.join(os.path.dirname(window_path), f"{sensor}.csv")
                logger.info(f"Saving {sensor} data to csv file.")
                with open(csv_path, "w") as f:
                    writer = csv.writer(f, delimiter=",")
                    for row in self.ready_window["sensor_data"][sensor]:
                        writer.writerow(row)

    def _manage_storage(self):
        """Check if the output directory has reached its storage limit and, if it has, delete the oldest window.

        :return None:
        """
        session_directory = os.path.join(self.output_directory, self._session_subdirectory)

        filter = lambda path: os.path.split(path)[-1].startswith("window")  # noqa
        storage_limit_in_mb = self.storage_limit / 1024 ** 2

        if calculate_disk_usage(session_directory, filter) >= self.storage_limit:
            oldest_window = get_oldest_file_in_directory(session_directory, filter)

            logger.warning(
                "Storage limit reached (%s MB) - deleting oldest window (%r).",
                storage_limit_in_mb,
                oldest_window,
            )

            os.remove(oldest_window)

        elif calculate_disk_usage(session_directory, filter) >= 0.9 * self.storage_limit:
            logger.warning("90% of storage limit reached - %s MB remaining.", 0.1 * storage_limit_in_mb)

    def _generate_window_path(self):
        """Generate the path that the window should be persisted to.

        :return str:
        """
        filename = f"{self._file_prefix}-{self._window_number}.json"
        return os.path.join(self.output_directory, self._session_subdirectory, filename)


class BatchingUploader(TimeBatcher):
    """A Google Cloud Storage uploader that will group the given data into time windows and upload it to a Google Cloud
    Storage.  If upload fails for a window, it will be written to the backup directory. If the `upload_backup_files`
    flag is `True`, its upload will then be reattempted after the upload of each subsequent window.

    :param iter(str) sensor_names: names of sensors to group data for
    :param str project_name: name of Google Cloud project to upload to
    :param str bucket_name: name of Google Cloud bucket to upload to
    :param float window_size: length of time window in seconds
    :param str session_subdirectory: directory within output directory to persist into
    :param str output_directory: directory to write windows to
    :param float upload_timeout: time after which to give up trying to upload to the cloud
    :param bool upload_backup_files: attempt to upload backed-up windows on next window upload
    :return None:
    """

    def __init__(
        self,
        sensor_names,
        project_name,
        bucket_name,
        window_size,
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
        super().__init__(sensor_names, window_size, session_subdirectory, output_directory)
        self._backup_directory = os.path.join(self.output_directory, ".backup")
        self._backup_writer = BatchingFileWriter(
            sensor_names, window_size, session_subdirectory, output_directory=self._backup_directory
        )

    def _persist_window(self):
        """Upload a window to Google Cloud storage. If the window fails to upload, it is instead written to disk.

        :return None:
        """
        try:
            self.client.upload_from_string(
                string=json.dumps(self.ready_window),
                bucket_name=self.bucket_name,
                path_in_bucket=self._generate_window_path(),
                metadata=self.metadata,
                timeout=self.upload_timeout,
            )

        except Exception:  # noqa
            logger.warning(
                "Upload of window failed - writing to disk at %r instead.",
                self._backup_writer._generate_window_path(),
            )

            self._backup_writer._persist_window(window=self.ready_window)
            return

        logger.info(f"{self._file_prefix.capitalize()} {self._window_number} uploaded to cloud.")

        if self.upload_backup_files:
            self._attempt_to_upload_backup_files()

    def _generate_window_path(self):
        """Generate the path that the window should be persisted to.

        :return str:
        """
        filename = f"{self._file_prefix}-{self._window_number}.json"
        return storage.path.join(self.output_directory, self._session_subdirectory, filename)

    def _attempt_to_upload_backup_files(self):
        """Check for backup files and attempt to upload them to cloud storage again.

        :return None:
        """
        backup_filenames = os.listdir(os.path.join(self._backup_directory, self._session_subdirectory))

        if not backup_filenames:
            return

        for filename in backup_filenames:

            if not filename.startswith(self._file_prefix):
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
