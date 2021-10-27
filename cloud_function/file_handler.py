import json
import logging
import os

from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.resources import Datafile
from preprocessing import preprocess


logger = logging.getLogger(__name__)


class FileHandler:
    """A handler for data windows that gets them from a source bucket, cleans them, and persists them into a destination
    bucket.

    :param str source_project:
    :param str source_bucket:
    :param str destination_project:
    :param str destination_bucket:
    :return None:
    """

    def __init__(self, source_project, source_bucket, destination_project, destination_bucket):
        self.source_project = source_project
        self.source_bucket = source_bucket
        self.source_client = GoogleCloudStorageClient(project_name=source_project, credentials=None)

        self.destination_project = destination_project
        self.destination_bucket = destination_bucket
        self.destination_client = GoogleCloudStorageClient(project_name=destination_project, credentials=None)

    def persist_configuration(self, path):
        """Persist a configuration file to the destination bucket.

        :param str path:
        :return None:
        """
        configuration = self.source_client.download_as_string(bucket_name=self.source_bucket, path_in_bucket=path)

        self.destination_client.upload_from_string(
            string=configuration, bucket_name=self.destination_bucket, path_in_bucket=path
        )

    def get_window(self, window_cloud_path):
        """Get the window from Google Cloud storage.

        :param str window_cloud_path:
        :return (dict, dict):
        """
        window = json.loads(
            self.source_client.download_as_string(bucket_name=self.source_bucket, path_in_bucket=window_cloud_path)
        )

        logger.info("Downloaded window %r from bucket %r.", window_cloud_path, self.source_bucket)

        cloud_metadata = self.source_client.get_metadata(
            bucket_name=self.source_bucket,
            path_in_bucket=window_cloud_path,
        )

        window_metadata = cloud_metadata["custom_metadata"]["data_gateway__configuration"]
        logger.info("Downloaded metadata for window %r from bucket %r.", window_cloud_path, self.source_bucket)

        return window, window_metadata

    def clean_window(self, window, window_metadata, event):
        """Clean and return the given window.

        :param dict window:
        :param dict window_metadata:
        :param dict event: Google Cloud event (currently unused)
        :return dict:
        """
        window = preprocess.run(window, window_metadata)
        window["cleaned"] = True
        logger.info("Cleaned window.")
        return window

    def persist_window(self, window, window_cloud_path):
        """Persist the window to the destination bucket in the destination project, along with an associated Datafile.

        :param dict window:
        :param str window_cloud_path:
        :return None:
        """
        cloud_path = storage.path.generate_gs_path(self.destination_bucket, window_cloud_path)

        with Datafile(path=cloud_path, project_name=self.destination_project, mode="w") as (datafile, f):
            json.dump(window, f)
            datafile.tags["sequence"] = int(os.path.splitext(window_cloud_path)[0].split("-")[-1])

        logger.info("Uploaded window to %r.", cloud_path)
