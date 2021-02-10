import logging
import time
import pkg_resources
from octue.utils.cloud.persistence import GoogleCloudStorageClient


logger = logging.getLogger(__name__)


CLOUD_DIRECTORY_NAME = "data_gateway"


class Uploader:
    """Handler class for HTTPS-based uploads of events and audio files"""

    def __init__(self, configuration=None, **kwargs):
        """Instantiate and configure gateway server"""
        self.streams = {sensor_type: [] for sensor_type in configuration["sensor_types"]}

    def add_to_stream(self, sensor_type, data):
        self.streams[sensor_type].append(data)

    def upload(self, project_name, bucket_name, directory_in_bucket, extension):
        client = GoogleCloudStorageClient(project_name=project_name)

        for stream_name, data in self.streams.items():
            client.upload_from_string(
                serialised_data="".join(data),
                bucket_name=bucket_name,
                path_in_bucket=f"{directory_in_bucket}/{stream_name}{extension}",
            )
            self.streams[stream_name] = []

    def _validate_event(self):
        """Validate event data against the required schema"""
        file_name = pkg_resources.resource_string("gateway", "schema/event_schema.json")
        logger.info("file name is %s", file_name)

    def _validate_audio(self):
        """Validate audio+meta data against the required schema"""
        file_name = pkg_resources.resource_string("gateway", "schema/audio_meta_schema.json")
        logger.info("file name is %s", file_name)


class StreamingUploader:
    def __init__(self, sensor_types, project_name, bucket_name, upload_interval):
        """Initialise a StreamingUploader with a bucket from a given GCP project.

        :param iter(dict) sensor_types: a dictionary with "name" and "extension" entries
        :param str project_name:
        :param str bucket_name:
        :param float upload_interval: time in seconds between cloud uploads
        :return None:
        """
        self.streams = {
            sensor_type["name"]: {
                "name": sensor_type["name"],
                "data": [],
                "batch_number": 0,
                "extension": sensor_type["extension"],
            }
            for sensor_type in sensor_types
        }
        self.bucket_name = bucket_name
        self.upload_interval = upload_interval
        self.client = GoogleCloudStorageClient(project_name=project_name)
        self.start_time = time.perf_counter()

    def add_to_stream(self, sensor_type, data):
        """Add serialised data (a string) to the stream for the given sensor type.

        :param str sensor_type:
        :param str data:
        :return None:
        """
        self.streams[sensor_type]["data"].append(data)

        # Send a batch to the cloud if enough time has elapsed.
        if time.perf_counter() - self.start_time >= self.upload_interval:
            self._upload_batch(stream=self.streams[sensor_type])

    def force_upload(self):
        """Upload all the streams, regardless of whether a complete upload interval has passed."""
        for stream in self.streams.values():
            self._upload_batch(stream=stream)

    def _upload_batch(self, stream):
        """Upload serialised data to a path in the bucket.

        :param dict stream:
        :return None:
        """
        self.client.upload_from_string(
            serialised_data="".join(stream["data"]),
            bucket_name=self.bucket_name,
            path_in_bucket=self._generate_path_in_bucket(stream),
        )

        stream["data"].clear()
        stream["batch_number"] += 1
        self.start_time = time.perf_counter()

    def _generate_path_in_bucket(self, stream):
        """Generate the path in the bucket that the next batch of the stream should be uploaded to.

        :param dict stream:
        :return str:
        """
        return f"{CLOUD_DIRECTORY_NAME}/{stream['name']}/batch-{stream['batch_number']}{stream['extension']}"
