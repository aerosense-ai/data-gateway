import json
import logging
import os
from octue.resources import Datafile
from octue.utils.cloud.persistence import GoogleCloudStorageClient


logger = logging.getLogger(__name__)


DATAFILES_DIRECTORY = "datafiles"


def clean_and_upload_batch(event, context, cleaned_batch_name=None):
    """Triggered by a change to a Cloud Storage bucket.

    :param dict event:
    :param google.cloud.functions.Context context: metadata for the event
    :return None
    """
    source_bucket_name = event["bucket"]
    batch_path = event["name"]
    cleaned_batch_path = os.path.join(os.path.split(batch_path)[0], cleaned_batch_name) or batch_path
    destination_project_name = os.environ["DESTINATION_PROJECT_NAME"]
    destination_bucket_name = os.environ["DESTINATION_BUCKET"]

    client = GoogleCloudStorageClient(project_name=destination_project_name, credentials=None)

    batch = json.loads(client.download_as_string(bucket_name=source_bucket_name, path_in_bucket=batch_path))
    logger.debug("Received batch %r from bucket %r for cleaning.", batch_path, source_bucket_name)

    cleaned_batch = clean(batch, event)

    client.upload_from_string(
        serialised_data=json.dumps(cleaned_batch),
        bucket_name=destination_bucket_name,
        path_in_bucket=cleaned_batch_path,
    )

    serialised_datafile = _make_serialised_google_cloud_storage_datafile(
        cleaned_batch, cleaned_batch_path, destination_bucket_name
    )

    client.upload_from_string(
        serialised_data=json.dumps(serialised_datafile),
        bucket_name=destination_bucket_name,
        path_in_bucket=os.path.join(os.path.split(batch_path)[0], DATAFILES_DIRECTORY, cleaned_batch_name),
    )

    logger.info("Cleaned and uploaded cleaned batch %r to bucket %r.", batch_path, destination_bucket_name)

    client.delete(bucket_name=source_bucket_name, path_in_bucket=batch_path)


def _make_serialised_google_cloud_storage_datafile(cleaned_batch, cleaned_batch_path, destination_bucket_name):
    """Make and return a serialised Google Cloud storage datafile.

    :param dict cleaned_batch:
    :param str cleaned_batch_path:
    :param str destination_bucket_name:
    :return dict:
    """
    with open(cleaned_batch_path, "w") as f:
        json.dump(cleaned_batch, f)

    batch_number = cleaned_batch_path.split(".")[0].split("-")[-1]
    datafile = Datafile(path=cleaned_batch_path, sequence=batch_number)
    serialised_datafile = datafile.serialise()
    serialised_datafile["path"] = f"gs://{destination_bucket_name}/{cleaned_batch_path}"
    serialised_datafile["absolute_path"] = serialised_datafile["path"]
    os.remove(cleaned_batch_path)

    return serialised_datafile


def clean(batch, event):
    """Clean and return the given batch.

    :param dict batch:
    :param dict event:
    :return dict:
    """
    batch["cleaned"] = True
    return batch
