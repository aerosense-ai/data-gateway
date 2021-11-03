import os

from file_handler import FileHandler


def handle_upload(event, context):
    """Clean a data window received from the gateway and upload it to Google BigQuery for long-term storage.

    :param dict event: Google Cloud event
    :param google.cloud.functions.Context context: metadata for the event
    :return None:
    """
    file_handler = FileHandler(
        source_project=os.environ["SOURCE_PROJECT_NAME"],
        source_bucket=event["bucket"],
        destination_project=os.environ["DESTINATION_PROJECT_NAME"],
        destination_biq_query_dataset=os.environ["BIG_QUERY_DATASET_NAME"],
    )

    window, window_metadata = file_handler.get_window(window_cloud_path=event["name"])
    cleaned_window = file_handler.clean_window(window, window_metadata, event)
    file_handler.persist_window(cleaned_window, window_metadata)
