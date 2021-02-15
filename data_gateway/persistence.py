import copy
import logging
import os
import time
from queue import SimpleQueue
from octue.utils.cloud.persistence import GoogleCloudStorageClient

import abc


logger = logging.getLogger(__name__)


BATCH_DIRECTORY_NAME = "data_gateway"


class TimeBatcher:
    def __init__(self, sensor_specifications, batch_interval):
        """Instantiate a TimeBatcher. The batcher will group the data given to it into batches of the duration of the
        time interval.

        :param iter(dict) sensor_specifications: a dictionary with "name" and "extension" entries
        :param float batch_interval: time interval with which to batch data (in seconds)
        :return None:
        """
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

    @abc.abstractmethod
    def _persist(self, batch):
        """Persist the batch to whatever medium is required (e.g. to disk, to a database, or to the cloud).

        :param dict batch:
        :return None:
        """
        pass


class BatchingFileWriter(TimeBatcher):
    def __init__(self, sensor_specifications, directory_path, batch_interval):
        """Initialise a BatchingFileWriter. The writer will save the data given to it to disk at the given interval of
        time.

        :param iter(dict) sensor_specifications: a dictionary with "name" and "extension" entries
        :param str directory_path: directory to write batches to
        :param float batch_interval: time interval with which to batch data (in seconds)
        :return None:
        """
        self.directory_path = directory_path
        super().__init__(sensor_specifications, batch_interval)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.force_write()

    def force_write(self):
        """Write all current batches to disk, regardless of whether a complete upload interval has passed.

        :return None:
        """
        for sensor_name in self.current_batches:
            self.finalise_current_batch(sensor_name)
            self._persist(batch=self.pop_next_ready_batch(sensor_name))

    def _persist(self, batch):
        """Write a batch of serialised data to disk.

        :param dict batch:
        :return None:
        """
        if len(batch["data"]) == 0:
            logger.warning(f"No data to upload for {batch['name']} during force upload.")
            return

        path = self._generate_write_path(batch)
        directory = os.path.split(path)[0]

        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(path, "w") as f:
            f.write("".join(batch["data"]))

    def _generate_write_path(self, batch):
        """Generate the path the batch should be written to.

        :param dict batch:
        :return str:
        """
        return os.path.join(
            self.directory_path,
            BATCH_DIRECTORY_NAME,
            batch["name"],
            f"batch-{batch['batch_number']}{batch['extension']}",
        )


class BatchingUploader(TimeBatcher):
    def __init__(self, sensor_specifications, project_name, bucket_name, batch_interval):
        """Initialise a BatchingUploader with a bucket from a given GCP project. The uploader will upload the data given
        to it to a GCP storage bucket at the given interval of time.

        :param iter(dict) sensor_specifications: a dictionary with "name" and "extension" entries
        :param str project_name:
        :param str bucket_name:
        :param float batch_interval: time interval with which to batch data (in seconds)
        :return None:
        """
        self.bucket_name = bucket_name
        self.client = GoogleCloudStorageClient(project_name=project_name)
        super().__init__(sensor_specifications, batch_interval=batch_interval)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.force_upload()

    def force_upload(self):
        """Upload all current batches, regardless of whether a complete upload interval has passed.

        :return None:
        """
        for sensor_name in self.current_batches:
            self.finalise_current_batch(sensor_name)
            self._persist(batch=self.pop_next_ready_batch(sensor_name))

    def _persist(self, batch):
        """Upload serialised data to a path in the bucket.

        :param dict batch:
        :return None:
        """
        if len(batch) == 0:
            logger.warning(f"No data to upload for {batch['name']} during force upload.")
            return

        self.client.upload_from_string(
            serialised_data="".join(batch["data"]),
            bucket_name=self.bucket_name,
            path_in_bucket=self._generate_path_in_bucket(batch),
        )

    def _generate_path_in_bucket(self, batch):
        """Generate the path in the bucket that the batch should be uploaded to.

        :param dict batch:
        :return str:
        """
        return f"{BATCH_DIRECTORY_NAME}/{batch['name']}/batch-{batch['batch_number']}{batch['extension']}"
