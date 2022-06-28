import json
import logging
import os

import shapely.geometry
import shapely.wkt
from octue.log_handlers import apply_log_handler

from big_query import BigQueryDataset
from exceptions import InstallationWithSameNameAlreadyExists, SensorTypeWithSameReferenceAlreadyExists
from forms import AddSensorTypeForm, CreateInstallationForm
from window_handler import WindowHandler


apply_log_handler()


logger = logging.getLogger(__name__)


def upload_window(event, context):
    """Upload data window received from the gateway to Google BigQuery for long-term storage. This is
    the entrypoint for the `ingress-eu` cloud function.

    :param dict event: Google Cloud event
    :param google.cloud.functions.Context context: metadata for the event
    :return None:
    """
    window_handler = WindowHandler(
        window_cloud_path=event["name"],
        source_bucket=event["bucket"],
        destination_project=os.environ["DESTINATION_PROJECT_NAME"],
        destination_bucket=os.environ["DESTINATION_BUCKET_NAME"],
        destination_biq_query_dataset=os.environ["BIG_QUERY_DATASET_NAME"],
    )

    window, window_metadata = window_handler.get_window()
    window_handler.persist_window(window, window_metadata)


def add_sensor_type(request):
    """Add a new sensor type to the BigQuery dataset. This is the entrypoint for the `add-sensor-type` cloud function.

    :return (dict, int):
    """
    form = AddSensorTypeForm(meta={"csrf": False})

    if request.method != "POST":
        return {"nonFieldErrors": "Method Not Allowed. Try 'POST'."}, 405

    if form.validate_on_submit():
        try:
            dataset = BigQueryDataset(
                project_name=os.environ["DESTINATION_PROJECT_NAME"],
                dataset_name=os.environ["BIG_QUERY_DATASET_NAME"],
            )

            dataset.add_sensor_type(
                reference=form.reference.data,
                name=form.name.data,
                description=form.description.data,
                measuring_unit=form.measuring_unit.data,
                metadata=form.metadata.data,
            )

        except SensorTypeWithSameReferenceAlreadyExists:
            return {
                "fieldErrors": {
                    "reference": f"A sensor type with the reference {form.reference.data!r} already exists."
                }
            }, 409

        except Exception as e:
            logger.exception(e)
            return {"nonFieldErrors": f"An error occurred. Form data was: {form.data}"}, 500

        return form.data, 200

    logger.error(json.dumps(form.errors))

    # Reduce lists of form field errors to single items.
    for field, error_messages in form.errors.items():
        form.errors[field] = error_messages[0] if len(error_messages) > 0 else "Unknown field error"

    return {"fieldErrors": form.errors}, 400


def create_installation(request):
    """Create a new installation in the BigQuery dataset. This is the entrypoint for the `create-installation` cloud
    function.

    :return (dict, int):
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
                turbine_id=form.turbine_id.data,
                blade_id=form.blade_id.data,
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

    logger.error(json.dumps(form.errors))

    # Reduce lists of form field errors to single items.
    for field, error_messages in form.errors.items():
        form.errors[field] = error_messages[0] if len(error_messages) > 0 else "Unknown field error"

    return {"fieldErrors": form.errors}, 400
