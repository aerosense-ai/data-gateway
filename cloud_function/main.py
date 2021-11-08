import logging
import os

import shapely.geometry
import shapely.wkt
from big_query import BigQueryDataset
from errors import clean_errors
from file_handler import FileHandler
from forms import CreateInstallationForm


logger = logging.getLogger(__name__)


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


def create_installation(request):
    form = CreateInstallationForm(meta={"csrf": False})

    if request.method != "POST":
        return {"nonFieldErrors": "Method Not Allowed. Try 'POST'."}, 405

    if form.validate_on_submit():
        try:
            dataset = BigQueryDataset(
                project_name=os.environ["DESTINATION_PROJECT_NAME"],
                dataset_name=os.environ["DESTINATION_DATASET_NAME"],
            )

            # TODO Put this into form validation
            reference = form.reference.replace("_", "-").replace(" ", "-").lower()

            # TODO Should this be easting and northing?
            location = shapely.geometry.Point(form.longitude, form.latitude)

            errors = dataset.add_installation(
                reference=reference,
                hardware_version=form.hardware_version,
                location=shapely.wkt.dumps(location),
            )

            if errors:
                raise ValueError(errors)

        except Exception:
            # Blanket exception because we don't want to show internal errors to customers.
            logger.exception(f"An error occurred. Form data was: {form.data}")

        return form.data, 200

    else:
        return clean_errors(form.errors), 400
