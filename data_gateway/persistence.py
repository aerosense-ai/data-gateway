import copy
import logging
import os
import time
from queue import SimpleQueue
from octue.utils.cloud.persistence import GoogleCloudStorageClient

import abc


logger = logging.getLogger(__name__)


DEFAULT_OUTPUT_DIRECTORY = "data_gateway"


class TimeBatcher:
    def __init__(self, sensor_specifications, batch_interval, output_directory=DEFAULT_OUTPUT_DIRECTORY):
        """Instantiate a TimeBatcher. The batcher will group the data given to it into batches of the duration of the
        time interval.

        :param iter(dict) sensor_specifications: a dictionary with "name" and "extension" entries
        :param float batch_interval: time interval with which to batch data (in seconds)
        :param str output_directory: directory to write batches to
        :return None:
        """
        self.output_directory = output_directory
        self.current_batches = {}
        self.ready_batches = {}

        for sensor_specification in sensor_specifications:
            self.current_batches[sensor_specification["name"]] = {
                "name": sensor_specification["name"],
                "data": [],
                "batch_number": 0,
                "extension": sensor_specification["extension"],
            }

            self.ready_batches[sensor_specification["name"]] = SimpleQueue()

        self.batch_interval = batch_interval
        self._start_time = time.perf_counter()

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
        current_batch = self.current_batches[sensor_name]
        current_batch["data"].append(data)

        # Finalise the batch and persist it if enough time has elapsed.
        if time.perf_counter() - self._start_time >= self.batch_interval:
            self.finalise_current_batch(sensor_name)
            self._persist(batch=self.pop_next_ready_batch(sensor_name))

    def finalise_current_batch(self, sensor_name):
        """Finalise the current batch for the given sensor name. This puts the current batch into the queue of ready
        batches, resets the clock for the next batch, and increases the batch number for it.

        :param str sensor_name:
        :return None:
        """
        self._start_time = time.perf_counter()
        batch = self.current_batches[sensor_name]
        self.ready_batches[sensor_name].put(copy.deepcopy(batch))
        batch["data"].clear()
        batch["batch_number"] += 1

    def pop_next_ready_batch(self, sensor_name):
        """Pop the next ready batch from the queue of ready batches for the given sensor name.

        :param str sensor_name:
        :return dict:
        """
        return self.ready_batches[sensor_name].get(block=False)

    def force_persist(self):
        """Persist all current batches, regardless of whether a complete batch interval has passed.

        :return None:
        """
        for sensor_name in self.current_batches:
            self.finalise_current_batch(sensor_name)
            self._persist(batch=self.pop_next_ready_batch(sensor_name))

    @abc.abstractmethod
    def _persist(self, batch):
        """Persist the batch to whatever medium is required (e.g. to disk, to a database, or to the cloud).

        :param dict batch:
        :return None:
        """
        pass

    def _generate_batch_path(self, batch):
        """Generate the path that the batch should be persisted to.

        :param dict batch:
        :return str:
        """
        return "/".join((self.output_directory, batch["name"], f"batch-{batch['batch_number']}{batch['extension']}"))


class BatchingFileWriter(TimeBatcher):
    """A writer that batches the data given to it over time into batches of the duration of the given time interval,
    saving each batch to disk.
    """

    def _persist(self, batch):
        """Write a batch of serialised data to disk.

        :param dict batch:
        :return None:
        """
        if len(batch["data"]) == 0:
            logger.warning("No data to write for %r.", batch["name"])
            return

        path = os.path.abspath(os.path.join(".", self._generate_batch_path(batch)))
        directory = os.path.split(path)[0]

        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(path, "w") as f:
            f.write("".join(batch["data"]))


class BatchingUploader(TimeBatcher):
    def __init__(
        self,
        sensor_specifications,
        project_name,
        bucket_name,
        batch_interval,
        output_directory=DEFAULT_OUTPUT_DIRECTORY,
        upload_timeout=60,
    ):
        """Initialise a BatchingUploader with a bucket from a given GCP project. The uploader will upload the data given
        to it to a GCP storage bucket at the given interval of time.

        :param iter(dict) sensor_specifications: a dictionary with "name" and "extension" entries
        :param str project_name:
        :param str bucket_name:
        :param float batch_interval: time interval with which to batch data (in seconds)
        :return None:
        """
        self.project_name = project_name
        self.bucket_name = bucket_name
        self.client = GoogleCloudStorageClient(project_name=project_name)
        self.upload_timeout = upload_timeout
        self._backup_writer = BatchingFileWriter(sensor_specifications, batch_interval, output_directory)
        super().__init__(sensor_specifications, batch_interval, output_directory)

    def _persist(self, batch):
        """Upload serialised data to a path in the bucket. If the batch fails to upload, it is instead written to disk.

        :param dict batch:
        :return None:
        """
        if len(batch["data"]) == 0:
            logger.warning("No data to upload for %r.", batch["name"])
            return

        try:
            self.client.upload_from_string(
                serialised_data="".join(batch["data"]),
                bucket_name=self.bucket_name,
                path_in_bucket=self._generate_batch_path(batch),
                timeout=self.upload_timeout,
            )

        except Exception:
            logger.warning(
                "Upload of batch %r failed - writing to disk at %r instead.",
                batch,
                self._backup_writer._generate_batch_path(batch),
            )

            self._backup_writer._persist(batch)
