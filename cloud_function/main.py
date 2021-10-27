import os

from file_handler import FileHandler


def handle_upload(event, context):
    """Clean a data window received from the gateway and upload to long-term storage.

    :param dict event: Google Cloud event
    :param google.cloud.functions.Context context: metadata for the event
    :return None:
    """
    file_path = event["name"]

    file_handler = FileHandler(
        source_project=os.environ["SOURCE_PROJECT_NAME"],
        source_bucket=event["bucket"],
        destination_project=os.environ["DESTINATION_PROJECT_NAME"],
        destination_bucket=os.environ["DESTINATION_BUCKET_NAME"],
    )

    if file_path.endswith("configuration.json"):
        file_handler.persist_configuration(file_path)
        return

    window, window_metadata = file_handler.get_window(file_path)
    cleaned_window = file_handler.clean_window(window, window_metadata, event)
    file_handler.persist_window(cleaned_window, file_path)
