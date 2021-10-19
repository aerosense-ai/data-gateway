import json
import logging
import os
from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.resources import Datafile
from preprocessing import preprocess


logger = logging.getLogger(__name__)


class FileHandler:
    """A class that get batches from a source bucket, cleans them, and persists them in a destination bucket.

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

    def get_batch(self, batch_path):
        """Get the batch from Google Cloud storage.

        :param octue.cloud.storage.client.GoogleCloudStorageClient storage_client: client for accessing Google Cloud storage
        :param dict event: Google Cloud event
        :return (dict, dict, str):
        """
        batch = json.loads(
            self.source_client.download_as_string(bucket_name=self.source_bucket, path_in_bucket=batch_path)
        )
        logger.info("Downloaded batch %r from bucket %r.", batch_path, self.source_bucket)

        batch_metadata = self.source_client.get_metadata(bucket_name=self.source_bucket, path_in_bucket=batch_path)
        logger.info("Downloaded metadata for batch %r from bucket %r.", batch_path, self.source_bucket)
        return batch, batch_metadata, batch_path

    def clean(self, batch, batch_metadata, event):
        """Clean and return the given batch.

        :param dict batch: batch to clean
        :param dict batch_metadata: metadata about batch
        :param dict event: Google Cloud event
        :return dict:
        """
        batch = preprocess.run(batch, batch_metadata)
        batch["cleaned"] = True
        logger.info("Cleaned batch.")
        return batch

    def persist_batch(self, batch, batch_path):
        """Persist the batch to the destination bucket in the destination project, along with an associated Datafile.

        :param dict patch: batch to persist
        :param str batch_path: path to persist batch to
        :return None:
        """
        cloud_path = storage.path.generate_gs_path(self.destination_bucket, batch_path)

        with Datafile(path=cloud_path, project_name=self.destination_project, mode="w") as (datafile, f):
            json.dump(batch, f)
            datafile.tags["sequence"] = int(os.path.splitext(batch_path)[0].split("-")[-1])

        logger.info("Uploaded batch to %r.", cloud_path, self.destination_project)
