import os
from file_handler import FileHandler


def clean_and_upload_batch(event, context):
    """Clean a batch of data received from the gateway and upload to long-term storage.

    :param dict event: Google Cloud event
    :param google.cloud.functions.Context context: metadata for the event
    :param str cleaned_batch_name: new name for cleaned batch file
    :return None:
    """
    file_path = event["name"]

    file_handler = FileHandler(
        source_project=os.environ["GCP_PROJECT"],
        source_bucket=event["bucket"],
        destination_project=os.environ["DESTINATION_PROJECT_NAME"],
        destination_bucket=os.environ["DESTINATION_BUCKET"],
    )

    if file_path.endswith("configuration.json"):
        file_handler.persist_configuration(file_path)
        return

    batch, batch_metadata, file_path = file_handler.get_batch(file_path)
    cleaned_batch = file_handler.clean(batch, batch_metadata, event)
    file_handler.persist_batch(cleaned_batch, file_path)
