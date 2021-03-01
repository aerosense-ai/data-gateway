import json
import logging
import os
from octue.resources import Datafile
from octue.utils.cloud.storage.client import GoogleCloudStorageClient


logger = logging.getLogger(__name__)


DATAFILES_DIRECTORY = "datafiles"


def clean_and_upload_batch(event, context, cleaned_batch_name=None):
    """Clean a batch of data received from the gateway and upload to long-term storage.

    :param dict event: Google Cloud event
    :param google.cloud.functions.Context context: metadata for the event
    :param str cleaned_batch_name: new name for cleaned batch file
    :return None:
    """
    source_bucket_name = event["bucket"]
    file_path = event["name"]

    if file_path.endswith("configuration.json"):
        persist_configuration(source_bucket_name, file_path)
        return

    batch, batch_metadata, file_path = get_batch(source_bucket_name, file_path)
    cleaned_batch = clean(batch, batch_metadata, event)

    if cleaned_batch_name:
        cleaned_batch_path = os.path.join(os.path.split(file_path)[0], cleaned_batch_name)
    else:
        cleaned_batch_path = file_path

    persist_batch(cleaned_batch, cleaned_batch_path)


def persist_configuration(source_bucket_name, path):
    """Persist a configuration file to the destination bucket.

    :param str source_bucket_name:
    :param str path:
    :return None:
    """
    configuration = get_source_client().download_as_string(bucket_name=source_bucket_name, path_in_bucket=path)
    destination_client, _, destination_bucket_name = get_destination_cloud_objects()

    destination_client.upload_from_string(
        serialised_data=configuration, bucket_name=destination_bucket_name, path_in_bucket=path
    )


def get_batch(bucket_name, batch_path):
    """Get the batch from Google Cloud storage.

    :param octue.utils.cloud.storage.client.GoogleCloudStorageClient storage_client: client for accessing Google Cloud storage
    :param dict event: Google Cloud event
    :return (dict, dict, str):
    """
    source_client = get_source_client()

    batch = json.loads(source_client.download_as_string(bucket_name=bucket_name, path_in_bucket=batch_path))
    logger.info("Downloaded batch %r from bucket %r.", batch_path, bucket_name)

    batch_metadata = source_client.get_metadata(bucket_name=bucket_name, path_in_bucket=batch_path)
    logger.info("Downloaded metadata for batch %r from bucket %r.", batch_path, bucket_name)
    return batch, batch_metadata, batch_path


def clean(batch, batch_metadata, event):
    """Clean and return the given batch.

    :param dict batch: batch to clean
    :param dict batch_metadata: metadata about batch
    :param dict event: Google Cloud event
    :return dict:
    """
    batch["cleaned"] = True
    logger.info("Cleaned batch.")
    return batch


def persist_batch(batch, batch_path):
    """Persist the batch to the destination bucket in the destination project, along with an associated Datafile.

    :param dict patch: batch to persist
    :param str batch_path: path to persist batch to
    :return None:
    """
    destination_client, destination_project_name, destination_bucket_name = get_destination_cloud_objects()

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


def get_source_client():
    """Get a storage client for the source bucket.

    :return None:
    """
    return GoogleCloudStorageClient(project_name=os.environ["GCP_PROJECT"], credentials=None)


def get_destination_cloud_objects():
    """Get a storage client for the destination bucket, along with the destination project and bucket names.

    :return (octue.utils.cloud.storage.client.GoogleCloudStorageClient, str, str):
    """
    destination_project_name = os.environ["DESTINATION_PROJECT_NAME"]
    return (
        GoogleCloudStorageClient(project_name=destination_project_name, credentials=None),
        destination_project_name,
        os.environ["DESTINATION_BUCKET"],
    )
