import json
import logging
import os
from octue.resources import Datafile
from octue.utils.cloud.storage.client import GoogleCloudStorageClient

from data_preprocess import preprocess


logger = logging.getLogger(__name__)


DATAFILES_DIRECTORY = "datafiles"


def clean_and_upload_batch(event, context, cleaned_batch_name=None):
    """Clean a batch of data received from the gateway and upload to long-term storage.

    :param dict event: Google Cloud event
    :param google.cloud.functions.Context context: metadata for the event
    :param str cleaned_batch_name: new name for cleaned batch file
    :return None:
    """
    batch, batch_metadata, batch_path = get_batch(event)
    cleaned_batch = clean(batch, batch_metadata, event)

    if cleaned_batch_name:
        cleaned_batch_path = os.path.join(os.path.split(batch_path)[0], cleaned_batch_name)
    else:
        cleaned_batch_path = batch_path

    persist_batch(cleaned_batch, cleaned_batch_path)


def get_batch(event):
    """Get the batch from Google Cloud storage.

    :param octue.utils.cloud.storage.client.GoogleCloudStorageClient storage_client: client for accessing Google Cloud storage
    :param dict event: Google Cloud event
    :return (dict, dict, str):
    """
    source_bucket_name = event["bucket"]
    batch_path = event["name"]
    source_client = GoogleCloudStorageClient(project_name=os.environ["GCP_PROJECT"], credentials=None)

    batch = json.loads(source_client.download_as_string(bucket_name=source_bucket_name, path_in_bucket=batch_path))
    logger.info("Downloaded batch %r from bucket %r.", batch_path, source_bucket_name)

    batch_metadata = source_client.get_metadata(bucket_name=source_bucket_name, path_in_bucket=batch_path)
    logger.info("Downloaded metadata for batch %r from bucket %r.", batch_path, source_bucket_name)
    return batch, batch_metadata, batch_path


def clean(batch, batch_metadata, event):
    """Clean and return the given batch.

    :param dict batch: batch to clean
    :param dict batch_metadata: metadata about batch
    :param dict event: Google Cloud event
    :return dict:
    """
    batch = preprocess.run(batch)
    batch["cleaned"] = True
    logger.info("Cleaned batch.")
    return batch


def persist_batch(batch, batch_path):
    """Persist the batch to the destination bucket in the destination project, along with an associated Datafile.

    :param dict patch: batch to persist
    :param str batch_path: path to persist batch to
    :return None:
    """
    destination_project_name = os.environ["DESTINATION_PROJECT_NAME"]
    destination_bucket_name = os.environ["DESTINATION_BUCKET"]
    destination_client = GoogleCloudStorageClient(project_name=destination_project_name, credentials=None)

    destination_client.upload_from_string(
        serialised_data=json.dumps(batch),
        bucket_name=destination_bucket_name,
        path_in_bucket=batch_path,
        metadata={"sequence": int(os.path.splitext(batch_path)[0].split("-")[-1])},
    )

    logger.info(
        "Uploaded batch to %r in bucket %r of project %r.",
        batch_path,
        destination_bucket_name,
        destination_project_name,
    )

    datafile = Datafile.from_google_cloud_storage(
        project_name=destination_project_name,
        bucket_name=destination_bucket_name,
        path_in_bucket=batch_path,
    )

    path_from, name = os.path.split(batch_path)
    datafile_path = os.path.join(path_from, DATAFILES_DIRECTORY, name)

    destination_client.upload_from_string(
        serialised_data=datafile.serialise(to_string=True),
        bucket_name=destination_bucket_name,
        path_in_bucket=datafile_path,
    )

    logger.info(
        "Uploaded Datafile for %r to %r in bucket %r of project %r.",
        batch_path,
        datafile_path,
        destination_bucket_name,
        destination_project_name,
    )
