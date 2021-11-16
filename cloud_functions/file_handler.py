import json
import logging

from big_query import BigQueryDataset
from exceptions import ConfigurationAlreadyExists
from octue.cloud.storage.client import GoogleCloudStorageClient
from preprocessing import preprocess


logger = logging.getLogger(__name__)


class FileHandler:
    """A handler for data windows that gets them from a source bucket, cleans them, and inserts them into a Google
    BigQuery dataset.

    :param str source_project: name of the project the source bucket belongs to
    :param str source_bucket: name of the bucket the files to be cleaned are stored in
    :param str destination_project: name of the project the BigQuery dataset belongs to
    :param str destination_biq_query_dataset: name of the BigQuery dataset to store the cleaned data in
    :return None:
    """

    def __init__(self, source_project, source_bucket, destination_project, destination_biq_query_dataset):
        self.source_project = source_project
        self.source_bucket = source_bucket
        self.source_client = GoogleCloudStorageClient(project_name=source_project, credentials=None)

        self.destination_project = destination_project
        self.destination_big_query_dataset = destination_biq_query_dataset

    def get_window(self, window_cloud_path):
        """Get the window from Google Cloud storage.

        :param str window_cloud_path: the Google Cloud Storage path to the window file
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

    def clean_window(self, window, window_metadata):
        """Clean and return the given window.

        :param dict window: the window of data to clean
        :param dict window_metadata: useful metadata about how the data was produced (currently the configuration the data gateway used to read it from the sensors)
        :return dict:
        """
        window = preprocess.run(window, window_metadata)
        logger.info("Cleaned window.")
        return window

    def persist_window(self, window, window_metadata):
        """Persist the window to the Google BigQuery dataset.

        :param dict window: the window of data to persist to the BigQuery dataset
        :param dict window_metadata: useful metadata about how the data was produced (currently the configuration the data gateway used to read it from the sensors)
        :return None:
        """
        dataset = BigQueryDataset(
            project_name=self.destination_project,
            dataset_name=self.destination_big_query_dataset,
        )

        user_data = window_metadata.pop("user_data")

        try:
            configuration_id = dataset.add_configuration(window_metadata)
        except ConfigurationAlreadyExists as e:
            configuration_id = e.args[1]

        dataset.insert_sensor_data(
            data=window,
            configuration_id=configuration_id,
            installation_reference=user_data["installation_reference"],
            label=user_data["label"],
        )

        logger.info("Uploaded window to BigQuery dataset %r.", self.destination_big_query_dataset)