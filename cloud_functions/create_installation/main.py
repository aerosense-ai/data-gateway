import logging

import shapely.geometry
import shapely.wkt
from errors import clean_errors
from forms import CreateInstallationForm
from google.cloud import bigquery


logger = logging.getLogger(__name__)

INVALID_METHOD_RESPONSE = {"nonFieldErrors": "Method Not Allowed. Try 'POST'."}
PROJECT_NAME = "aerosense-twined"
DATASET_NAME = "greta"


def contact(request):
    form = CreateInstallationForm(meta={"csrf": False})

    if request.method != "POST":
        return INVALID_METHOD_RESPONSE, 405

    if form.validate_on_submit():
        try:
            client = bigquery.Client()

            # TODO Put this into form validation
            reference = form.reference.replace("_", "-").replace(" ", "-").lower()

            # TODO Should this be easting and northing?
            location = shapely.geometry.Point(form.longitude, form.latitude)

            errors = client.insert_rows(
                table=client.get_table(f"{PROJECT_NAME}.{DATASET_NAME}.installation"),
                rows=[
                    {
                        "reference": reference,
                        "hardware_version": form.hardware_version,
                        "location": shapely.wkt.dumps(location),
                    }
                ],
            )

            if errors:
                raise ValueError(errors)

        except Exception:
            # Blanket exception because we don't want to show internal errors to customers.
            logger.exception(f"An error occurred. Form data was: {form.data}")

        return form.data, 200

    else:
        return clean_errors(form.errors), 400
