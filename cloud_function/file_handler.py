import json
import logging
import os
from octue.resources import Datafile
from octue.utils.cloud.storage.client import GoogleCloudStorageClient


logger = logging.getLogger(__name__)
DATAFILES_DIRECTORY = "datafiles"


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
            serialised_data=configuration, bucket_name=self.destination_bucket, path_in_bucket=path
        )

    def get_batch(self, batch_path):
        """Get the batch from Google Cloud storage.

        :param octue.utils.cloud.storage.client.GoogleCloudStorageClient storage_client: client for accessing Google Cloud storage
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
        batch["cleaned"] = True
        logger.info("Cleaned batch.")
        return batch

    def persist_batch(self, batch, batch_path):
        """Persist the batch to the destination bucket in the destination project, along with an associated Datafile.

        :param dict patch: batch to persist
        :param str batch_path: path to persist batch to
        :return None:
        """
        self.destination_client.upload_from_string(
            serialised_data=json.dumps(batch),
            bucket_name=self.destination_bucket,
            path_in_bucket=batch_path,
            metadata={"sequence": int(os.path.splitext(batch_path)[0].split("-")[-1])},
        )

        logger.info(
            "Uploaded batch to %r in bucket %r of project %r.",
            batch_path,
            self.destination_bucket,
            self.destination_project,
        )

        datafile = Datafile.from_google_cloud_storage(
            project_name=self.destination_project,
            bucket_name=self.destination_bucket,
            path_in_bucket=batch_path,
        )

        path_from, name = os.path.split(batch_path)
        datafile_path = os.path.join(path_from, DATAFILES_DIRECTORY, name)

        self.destination_client.upload_from_string(
            serialised_data=datafile.serialise(to_string=True),
            bucket_name=self.destination_bucket,
            path_in_bucket=datafile_path,
        )

        logger.info(
            "Uploaded Datafile for %r to %r in bucket %r of project %r.",
            batch_path,
            datafile_path,
            self.destination_bucket,
            self.destination_project,
        )
