import logging
import pkg_resources
from octue.utils.cloud.persistence import GoogleCloudStorageClient


logger = logging.getLogger(__name__)


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
