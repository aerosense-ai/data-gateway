import json
import logging
import os

import shapely.geometry
import shapely.wkt
from octue.log_handlers import apply_log_handler

from big_query import BigQueryDataset
from exceptions import InstallationWithSameNameAlreadyExists
from file_handler import FileHandler
from forms import CreateInstallationForm


apply_log_handler()


logger = logging.getLogger(__name__)


def clean_and_upload_window(event, context):
    """Clean a data window received from the gateway and upload it to Google BigQuery for long-term storage. This is
    the entrypoint for the `ingress-eu` cloud function.

    :param dict event: Google Cloud event
    :param google.cloud.functions.Context context: metadata for the event
    :return None:
    """
    file_handler = FileHandler(
        window_cloud_path=event["name"],
        source_project=os.environ["SOURCE_PROJECT_NAME"],
        source_bucket=event["bucket"],
        destination_project=os.environ["DESTINATION_PROJECT_NAME"],
        destination_bucket=os.environ["DESTINATION_BUCKET_NAME"],
        destination_biq_query_dataset=os.environ["BIG_QUERY_DATASET_NAME"],
    )

    window, window_metadata = file_handler.get_window()
    cleaned_window = file_handler.clean_window(window, window_metadata)
    file_handler.persist_window(cleaned_window, window_metadata)


def create_installation(request):
    """Create a new installation in the BigQuery dataset. This is the entrypoint for the `create-installation` cloud
    function.
    """
    form = CreateInstallationForm(meta={"csrf": False})

    if request.method != "POST":
        return {"nonFieldErrors": "Method Not Allowed. Try 'POST'."}, 405

    if form.validate_on_submit():
        try:
            if form.latitude.data and form.longitude.data:
                location = shapely.wkt.dumps(shapely.geometry.Point(form.latitude.data, form.longitude.data))
            else:
                location = None

            dataset = BigQueryDataset(
                project_name=os.environ["DESTINATION_PROJECT_NAME"],
                dataset_name=os.environ["BIG_QUERY_DATASET_NAME"],
            )

            dataset.add_installation(
                reference=form.reference.data,
                hardware_version=form.hardware_version.data,
                location=location,
            )

        except InstallationWithSameNameAlreadyExists:
            return {
                "fieldErrors": {
                    "reference": f"An installation with the reference {form.reference.data!r} already exists."
                }
            }, 409

        except Exception as e:
            logger.exception(e)
            return {"nonFieldErrors": f"An error occurred. Form data was: {form.data}"}, 500

        return form.data, 200

    logger.info(json.dumps(form.errors))

    # Reduce lists of form field errors to single items.
    for field, error_messages in form.errors.items():
        form.errors[field] = error_messages[0] if len(error_messages) > 0 else "Unknown field error"

    return {"fieldErrors": form.errors}, 400
