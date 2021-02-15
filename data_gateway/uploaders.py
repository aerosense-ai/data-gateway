import copy
import logging
import time
from queue import Empty, SimpleQueue
from octue.utils.cloud.persistence import GoogleCloudStorageClient


logger = logging.getLogger(__name__)


CLOUD_DIRECTORY_NAME = "data_gateway"


class TimeBatcher:
    def __init__(self, sensor_specifications, batch_interval):
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

        # Move to the next batch if enough time has elapsed.
        if time.perf_counter() - self._start_time >= self.batch_interval:
            self.finalise_current_batch(sensor_name)

    def finalise_current_batch(self, sensor_name):
        self._start_time = time.perf_counter()
        batch = self.current_batches[sensor_name]
        self.ready_batches[sensor_name].put(copy.deepcopy(batch))
        batch["data"].clear()
        batch["batch_number"] += 1

    def pop_next_ready_batch(self, sensor_name):
        return self.ready_batches[sensor_name].get(block=False)


class BatchingUploader:
    def __init__(self, sensor_specifications, project_name, bucket_name, upload_interval):
        """Initialise a StreamingUploader with a bucket from a given GCP project.

        :param iter(dict) sensor_specifications: a dictionary with "name" and "extension" entries
        :param str project_name:
        :param str bucket_name:
        :param float upload_interval: time in seconds between cloud uploads
        :return None:
        """
        self.batcher = TimeBatcher(sensor_specifications, batch_interval=upload_interval)
        self.bucket_name = bucket_name
        self.upload_interval = upload_interval
        self.client = GoogleCloudStorageClient(project_name=project_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.force_upload()

    def add_to_current_batch(self, sensor_name, data):
        """Add serialised data (a string) to the current batch for the given sensor name.

        :param str sensor_name:
        :param str data:
        :return None:
        """
        self.batcher.add_to_current_batch(sensor_name, data)

        # Send a batch to the cloud if enough time has elapsed.
        try:
            ready_batch = self.batcher.pop_next_ready_batch(sensor_name)
        except Empty:
            return

        self._upload_batch(batch=ready_batch)

    def force_upload(self):
        """Upload all current batches, regardless of whether a complete upload interval has passed."""
        for sensor_name in self.batcher.current_batches:
            self.batcher.finalise_current_batch(sensor_name)
            self._upload_batch(batch=self.batcher.pop_next_ready_batch(sensor_name))

    def _upload_batch(self, batch):
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
        return f"{CLOUD_DIRECTORY_NAME}/{batch['name']}/batch-{batch['batch_number']}{batch['extension']}"
