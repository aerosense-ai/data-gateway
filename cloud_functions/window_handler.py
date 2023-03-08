import json
import logging
import os

from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.resources.datafile import Datafile

from big_query import BigQueryDataset
from exceptions import ConfigurationAlreadyExists


logger = logging.getLogger(__name__)


MICROPHONE_SENSOR_NAME = "Mics"


class WindowHandler:
    """A handler for data windows that gets them from a source bucket, sends any microphone data to a
    cloud storage bucket, and sends all other data into a Google BigQuery dataset. Metadata about the microphone data,
    including its location in cloud storage, is also added to the BigQuery dataset.

    :param str window_cloud_path: the Google Cloud Storage path to the window file
    :param str source_bucket: name of the bucket the files to be processed are stored in
    :param str destination_project: name of the project the BigQuery dataset belongs to
    :param str destination_bucket: name of the bucket to store raw microphone data in
    :param str destination_biq_query_dataset: name of the BigQuery dataset to store the raw data in
    :return None:
    """

    def __init__(
        self,
        window_cloud_path,
        source_bucket,
        destination_project,
        destination_bucket,
        destination_biq_query_dataset,
    ):
        self.window_cloud_path = storage.path.generate_gs_path(source_bucket, window_cloud_path)
        self.source_client = GoogleCloudStorageClient()

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
        window = json.loads(self.source_client.download_as_string(cloud_path=self.window_cloud_path))
        logger.info("Downloaded window %r.", self.window_cloud_path)

        cloud_metadata = self.source_client.get_metadata(self.window_cloud_path)
        logger.info("Custom metadata (logged for debugging upload race condition): %s", cloud_metadata)
        window_metadata = cloud_metadata["custom_metadata"]["data_gateway__configuration"]

        logger.info("Downloaded metadata for window %r.", self.window_cloud_path)
        return window, window_metadata

    def persist_window(self, window, window_metadata):
        """Persist the window to the Google BigQuery dataset.

        :param dict window: the window of data to persist to the BigQuery dataset
        :param dict window_metadata: useful metadata about how the data was produced (currently the configuration the data gateway used to read it from the sensors)
        :return None:
        """
        measurement_campaign_data = window_metadata.pop("measurement_campaign")
        self.dataset.add_or_update_measurement_campaign(measurement_campaign_data)

        try:
            configuration_id = self.dataset.add_configuration(window_metadata)
        except ConfigurationAlreadyExists as e:
            configuration_id = e.args[1]

        for node_id, node_data in window.items():
            if MICROPHONE_SENSOR_NAME in node_data:
                self._store_microphone_data(
                    data=node_data.pop(MICROPHONE_SENSOR_NAME),
                    node_id=node_id,
                    configuration_id=configuration_id,
                    installation_reference=window_metadata["gateway"]["installation_reference"],
                    measurement_campaign_reference=measurement_campaign_data["reference"],
                )

            self.dataset.add_sensor_data(
                data=node_data,
                node_id=node_id,
                configuration_id=configuration_id,
                installation_reference=window_metadata["gateway"]["installation_reference"],
                measurement_campaign_reference=measurement_campaign_data["reference"],
            )

        logger.info("Uploaded window to BigQuery dataset %r.", self.destination_big_query_dataset)

    def _store_microphone_data(
        self,
        data,
        node_id,
        configuration_id,
        installation_reference,
        measurement_campaign_reference,
    ):
        """Store microphone data as an HDF5 file in the destination cloud storage bucket and record its location and
        metadata in a BigQuery table.

        :param list(list) data:
        :param str node_id:
        :param str configuration_id:
        :param str installation_reference:
        :param str measurement_campaign_reference:
        :return None:
        """
        _, upload_path = storage.path.split_bucket_name_from_cloud_path(self.window_cloud_path)
        upload_path = os.path.splitext(upload_path)[0] + ".hdf5"

        # Record the start time of this chunk of data, both for the octue file metadata and for the db ordering
        window_timestamp = data[0][0]

        microphone_file = Datafile(
            path=storage.path.generate_gs_path(self.destination_bucket, "microphone", upload_path),
            timestamp=window_timestamp,
            tags={
                "node_id": node_id,
                "configuration_id": configuration_id,
                "installation_reference": installation_reference,
                "measurement_campaign_reference": measurement_campaign_reference,
            },
            ignore_stored_metadata=True,
        )

        with microphone_file.open("w") as f:
            f["dataset"] = data

        logger.info(f"Uploaded {len(data)} microphone data entries to {microphone_file.cloud_path!r}.")

        self.dataset.record_microphone_data_location_and_metadata(
            path=microphone_file.cloud_path,
            node_id=node_id,
            configuration_id=configuration_id,
            installation_reference=installation_reference,
            timestamp=window_timestamp,
            measurement_campaign_reference=measurement_campaign_reference,
        )
