import logging
import time
from queue import Empty, SimpleQueue
from octue.utils.cloud.persistence import GoogleCloudStorageClient


logger = logging.getLogger(__name__)


CLOUD_DIRECTORY_NAME = "data_gateway"


class TimeBatcher:
    def __init__(self, sensor_types, batch_interval):
        self.streams = {
            sensor_type["name"]: {
                "name": sensor_type["name"],
                "data": [],
                "batch_number": 0,
                "extension": sensor_type["extension"],
                "ready_batches": SimpleQueue(),
            }
            for sensor_type in sensor_types
        }
        self.batch_interval = batch_interval
        self._start_time = time.perf_counter()

    def add_to_stream(self, sensor_type, data):
        """Add serialised data (a string) to the stream for the given sensor type.

        :param str sensor_type:
        :param str data:
        :return None:
        """
        stream = self.streams[sensor_type]
        stream["data"].append(data)

        # Move to the next batch if enough time has elapsed.
        if time.perf_counter() - self._start_time >= self.batch_interval:
            self.prepare_next_batch(stream)

    def prepare_next_batch(self, stream):
        self._start_time = time.perf_counter()

        batch = {
            "name": stream["name"],
            "data": stream["data"].copy(),
            "batch_number": stream["batch_number"],
            "extension": stream["extension"],
        }

        stream["ready_batches"].put(batch)

        stream["data"].clear()
        stream["batch_number"] += 1

    def pop_next_batch(self, sensor_type):
        return self.streams[sensor_type]["ready_batches"].get(block=False)


class StreamingUploader:
    def __init__(self, sensor_types, project_name, bucket_name, upload_interval):
        """Initialise a StreamingUploader with a bucket from a given GCP project.

        :param iter(dict) sensor_types: a dictionary with "name" and "extension" entries
        :param str project_name:
        :param str bucket_name:
        :param float upload_interval: time in seconds between cloud uploads
        :return None:
        """
        self.batcher = TimeBatcher(sensor_types, batch_interval=upload_interval)
        self.bucket_name = bucket_name
        self.upload_interval = upload_interval
        self.client = GoogleCloudStorageClient(project_name=project_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.force_upload()

    def add_to_stream(self, sensor_type, data):
        """Add serialised data (a string) to the stream for the given sensor type.

        :param str sensor_type:
        :param str data:
        :return None:
        """
        self.batcher.add_to_stream(sensor_type, data)

        # Send a batch to the cloud if enough time has elapsed.
        try:
            ready_batch = self.batcher.pop_next_batch(sensor_type)
        except Empty:
            return

        self._upload_batch(stream=ready_batch)

    def force_upload(self):
        """Upload all the streams, regardless of whether a complete upload interval has passed."""
        for stream in self.batcher.streams.values():
            self.batcher.prepare_next_batch(stream)
            self._upload_batch(stream=stream)

    def _upload_batch(self, stream):
        """Upload serialised data to a path in the bucket.

        :param dict stream:
        :return None:
        """
        batch = stream["data"]

        if len(batch) == 0:
            logger.warning(f"No data to upload for {stream['name']} during force upload.")
            return

        self.client.upload_from_string(
            serialised_data="".join(batch),
            bucket_name=self.bucket_name,
            path_in_bucket=self._generate_path_in_bucket(stream),
        )

    def _generate_path_in_bucket(self, stream):
        """Generate the path in the bucket that the next batch of the stream should be uploaded to.

        :param dict stream:
        :return str:
        """
        return f"{CLOUD_DIRECTORY_NAME}/{stream['name']}/batch-{stream['batch_number']}{stream['extension']}"
