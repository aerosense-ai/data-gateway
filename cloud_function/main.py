import json
import logging
import os

import shapely.geometry
import shapely.wkt
from big_query import BigQueryDataset
from exceptions import InstallationWithSameNameAlreadyExists
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
    """Create a new installation in the BigQuery dataset. This is the entrypoint for the `create-installation` cloud
    function. To deploy it, run:

    ```
    gcloud functions deploy create-installation \
        --source cloud_function \
        --entry-point create_installation \
        --runtime python39 \
        --trigger-http \
        --security-level secure-always \
        --region europe-west6 \
        --set-env-vars DESTINATION_PROJECT_NAME=aerosense-twined,BIG_QUERY_DATASET_NAME=greta
    ```

    from the repository root.
    """
    form = CreateInstallationForm(meta={"csrf": False})

    if request.method != "POST":
        return {"nonFieldErrors": "Method Not Allowed. Try 'POST'."}, 405

    if form.validate_on_submit():
        try:
            # TODO Put this into form validation
            reference = form.reference.data.replace("_", "-").replace(" ", "-").lower()

            # TODO Should this be easting and northing?
            if form.longitude.data and form.latitude.data:
                location = shapely.wkt.dumps(shapely.geometry.Point(form.longitude.data, form.latitude.data))
            else:
                location = None

            dataset = BigQueryDataset(
                project_name=os.environ["DESTINATION_PROJECT_NAME"],
                dataset_name=os.environ["BIG_QUERY_DATASET_NAME"],
            )

            dataset.add_installation(
                reference=reference,
                hardware_version=form.hardware_version.data,
                location=location,
            )

        except InstallationWithSameNameAlreadyExists:
            return f"An installation with the reference {form.reference.data!r} already exists.", 409

        except Exception:
            # Blanket exception because we don't want to show internal errors to customers.
            error_message = f"An error occurred. Form data was: {form.data}"
            logger.exception(error_message)
            return error_message, 500

        return form.data, 200

    else:
        logger.info(json.dumps(form.errors))

        # Reduce lists of form field errors to single items.
        for field, error_messages in form.errors.items():
            form.errors[field] = error_messages[0] if len(error_messages) > 0 else "Unknown field error"

        return form.errors
