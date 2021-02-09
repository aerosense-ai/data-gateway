import logging
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
    def __init__(self, sensor_types, project_name, bucket_name, batch_size=100):
        self.streams = {
            sensor_type["name"]: {"data": [], "counts": 0, "extension": sensor_type["extension"]}
            for sensor_type in sensor_types
        }
        self.batch_size = batch_size
        self.bucket_name = bucket_name
        self.client = GoogleCloudStorageClient(project_name=project_name)

    def add_to_stream(self, sensor_type, data):
        self.streams[sensor_type]["data"].append(data)

        # Send a batch to the cloud if enough data has been collected.
        if len(self.streams[sensor_type]["data"]) >= self.batch_size:
            sensor = self.streams[sensor_type]
            path_in_bucket = f"{CLOUD_DIRECTORY_NAME}/{sensor_type}/batch-{sensor['counts']}{sensor['extension']}"
            self._upload(data=sensor["data"][: self.batch_size], path_in_bucket=path_in_bucket)
            self.streams[sensor_type]["data"] = sensor["data"][self.batch_size :]
            self.streams[sensor_type]["counts"] += 1

    def _upload(self, data, path_in_bucket):
        self.client.upload_from_string(
            serialised_data="".join(data),
            bucket_name=self.bucket_name,
            path_in_bucket=path_in_bucket,
        )
