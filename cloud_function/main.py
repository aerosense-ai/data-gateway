import json
import logging
import os
from octue.utils.cloud.persistence import GoogleCloudStorageClient


logger = logging.getLogger(__name__)


def clean_and_upload_batch(event, context):
    """Triggered by a change to a Cloud Storage bucket.

    :param dict event:
    :param google.cloud.functions.Context context: metadata for the event
    :return None
    """
    destination_project_name = os.environ["DESTINATION_PROJECT_NAME"]
    destination_bucket = os.environ["DESTINATION_BUCKET"]
    source_bucket = event["bucket"]
    batch_path = event["name"]

    client = GoogleCloudStorageClient(project_name=destination_project_name, credentials=None)

    batch = json.loads(client.download_as_string(bucket_name=source_bucket, path_in_bucket=batch_path))
    logger.debug("Received batch %r from bucket %r for cleaning.", batch_path, source_bucket)

    cleaned_batch = clean(batch, event)

    client.upload_from_string(
        serialised_data=json.dumps(cleaned_batch), bucket_name=destination_bucket, path_in_bucket=batch_path
    )
    logger.info("Cleaned and uploaded cleaned batch %r to bucket %r.", batch_path, destination_bucket)

    client.delete(bucket_name=source_bucket, path_in_bucket=batch_path)


def clean(batch, event):
    """Clean and return the given batch.

    :param dict batch:
    :param dict event:
    :return dict:
    """
    batch["cleaned"] = True
    return batch
