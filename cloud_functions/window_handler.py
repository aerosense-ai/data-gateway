import json
import logging
import os

from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.resources.datafile import Datafile

from big_query import BigQueryDataset
from exceptions import ConfigurationAlreadyExists
from preprocessing import preprocess


logger = logging.getLogger(__name__)


MICROPHONE_SENSOR_NAME = "Mics"


class WindowHandler:
    """A handler for data windows that gets them from a source bucket, cleans them, sends any microphone data to a
    cloud storage bucket, and sends all other data into a Google BigQuery dataset. Metadata about the microphone data,
    including its location in cloud storage, is also added to the BigQuery dataset.

    :param str window_cloud_path: the Google Cloud Storage path to the window file
    :param str source_project: name of the project the source bucket belongs to
    :param str source_bucket: name of the bucket the files to be cleaned are stored in
    :param str destination_project: name of the project the BigQuery dataset belongs to
    :param str destination_bucket: name of the bucket to store cleaned microphone data in
    :param str destination_biq_query_dataset: name of the BigQuery dataset to store the cleaned data in
    :return None:
    """

    def __init__(
        self,
        window_cloud_path,
        source_project,
        source_bucket,
        destination_project,
        destination_bucket,
        destination_biq_query_dataset,
    ):
        self.window_cloud_path = window_cloud_path
        self.source_project = source_project
        self.source_bucket = source_bucket
        self.source_client = GoogleCloudStorageClient(project_name=source_project, credentials=None)

        self.destination_project = destination_project
        self.destination_bucket = destination_bucket
        self.destination_big_query_dataset = destination_biq_query_dataset

        self.dataset = BigQueryDataset(
            project_name=self.destination_project,
            dataset_name=self.destination_big_query_dataset,
        )

    def get_window(self):
        """Get the window from Google Cloud storage.

        :return (dict, dict):
        """
        window = json.loads(
            self.source_client.download_as_string(bucket_name=self.source_bucket, path_in_bucket=self.window_cloud_path)
        )

        logger.info("Downloaded window %r from bucket %r.", self.window_cloud_path, self.source_bucket)

        cloud_metadata = self.source_client.get_metadata(
            bucket_name=self.source_bucket,
            path_in_bucket=self.window_cloud_path,
        )

        window_metadata = cloud_metadata["custom_metadata"]["data_gateway__configuration"]
        logger.info("Downloaded metadata for window %r from bucket %r.", self.window_cloud_path, self.source_bucket)

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
        session_data = window_metadata.pop("session_data")

        try:
            configuration_id = self.dataset.add_configuration(window_metadata)
        except ConfigurationAlreadyExists as e:
            configuration_id = e.args[1]

        if MICROPHONE_SENSOR_NAME in window:
            self._store_microphone_data(
                data=window.pop(MICROPHONE_SENSOR_NAME),
                configuration_id=configuration_id,
                installation_reference=window_metadata["installation_data"]["installation_reference"],
                label=session_data.get("label"),
            )

        self.dataset.add_sensor_data(
            data=window,
            configuration_id=configuration_id,
            installation_reference=window_metadata["installation_data"]["installation_reference"],
            label=session_data.get("label"),
        )

        logger.info("Uploaded window to BigQuery dataset %r.", self.destination_big_query_dataset)

    def _store_microphone_data(self, data, configuration_id, installation_reference, label):
        """Store microphone data as an HDF5 file in the destination cloud storage bucket and record its location and
        metadata in a BigQuery table.

        :param list(list) data:
        :param str configuration_id:
        :param str installation_reference:
        :param str label:
        :return None:
        """
        _, upload_path = storage.path.split_bucket_name_from_gs_path(self.window_cloud_path)
        upload_path = os.path.splitext(upload_path)[0] + ".hdf5"

        if label:
            labels = [label]
        else:
            labels = None

        datafile = Datafile(
            path=storage.path.generate_gs_path(self.destination_bucket, "microphone", upload_path),
            project_name=self.destination_project,
            tags={"configuration_id": configuration_id, "installation_reference": installation_reference},
            labels=labels,
        )

        with datafile.open("w") as f:
            json.dump(data, f)

        self.dataset.record_microphone_data_location_and_metadata(
            path=datafile.cloud_path,
            project_name=self.destination_project,
            configuration_id=configuration_id,
            installation_reference=installation_reference,
            label=label,
        )
