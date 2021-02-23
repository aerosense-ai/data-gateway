import json
import logging
import os
from octue.resources import Datafile
from octue.utils.cloud.persistence import GoogleCloudStorageClient


logger = logging.getLogger(__name__)


DATAFILES_DIRECTORY = "datafiles"


def clean_and_upload_batch(event, context, cleaned_batch_name=None):
    """Clean a batch from the data-gateway when one is uploaded to the Google Cloud Storage bucket that this
    cloud function is set up to trigger for. The cleaned batch is then uploaded to another Google Cloud storage bucket
    along with an associated Octue Datafile.

    :param dict event:
    :param google.cloud.functions.Context context: metadata for the event
    :param str cleaned_batch_name:
    :return None:
    """
    destination_project_name = os.environ["DESTINATION_PROJECT_NAME"]
    destination_bucket_name = os.environ["DESTINATION_BUCKET"]
    client = GoogleCloudStorageClient(project_name=destination_project_name, credentials=None)

    batch, batch_metadata, batch_path = get_batch(client, event)
    cleaned_batch = clean(batch, batch_metadata, event)
    cleaned_batch_path = os.path.join(os.path.split(batch_path)[0], cleaned_batch_name) or batch_path

    client.upload_from_string(
        serialised_data=json.dumps(cleaned_batch),
        bucket_name=destination_bucket_name,
        path_in_bucket=cleaned_batch_path,
    )

    datafile = Datafile.from_google_cloud_storage(
        project_name=destination_project_name,
        bucket_name=destination_bucket_name,
        path_in_bucket=cleaned_batch_path,
        sequence=int(os.path.splitext(cleaned_batch_path)[0].split("-")[-1]),
    )

    client.upload_from_string(
        serialised_data=datafile.serialise(to_string=True),
        bucket_name=destination_bucket_name,
        path_in_bucket=os.path.join(os.path.split(batch_path)[0], DATAFILES_DIRECTORY, cleaned_batch_name),
    )

    logger.info("Cleaned and uploaded batch %r to bucket %r.", batch_path, destination_bucket_name)

    client.delete(bucket_name=event["bucket"], path_in_bucket=batch_path)


def get_batch(storage_client, event):
    """Get the batch from Google Cloud storage.

    :param octue.utils.cloud.persistence.GoogleCloudStorageClient storage_client:
    :param dict event:
    :return (dict, dict, str):
    """
    source_bucket_name = event["bucket"]
    batch_path = event["name"]

    batch = json.loads(storage_client.download_as_string(bucket_name=source_bucket_name, path_in_bucket=batch_path))
    batch_metadata = storage_client.get_metadata(bucket_name=source_bucket_name, path_in_bucket=batch_path)
    logger.debug("Received batch %r from bucket %r for cleaning.", batch_path, source_bucket_name)

    return batch, batch_metadata, batch_path


def clean(batch, batch_metadata, event):
    """Clean and return the given batch.

    :param dict batch:
    :param dict batch_metadata:
    :param dict event:
    :return dict:
    """
    batch["cleaned"] = True
    return batch
