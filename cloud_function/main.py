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

    client = GoogleCloudStorageClient(project_name=destination_project_name)

    batch = json.loads(client.download_as_string(bucket_name=event["bucket"], path_in_bucket=event["name"]))
    logger.debug("Received batch %r from bucket %r for cleaning.", event["name"], event["bucket"])

    cleaned_batch = clean(batch, event)

    client.upload_from_string(
        serialised_data=json.dumps(cleaned_batch), bucket_name=destination_bucket, path_in_bucket=event["name"]
    )
    logger.info("Cleaned and uploaded cleaned batch %r to bucket %r.", event["name"], destination_bucket)


def clean(batch, event):
    """Clean and return the given batch.

    :param dict batch:
    :param dict event:
    :return dict:
    """
    batch["cleaned"] = True
    return batch
