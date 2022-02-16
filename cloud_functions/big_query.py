import copy
import datetime
import json
import logging
import uuid

from blake3 import blake3
from google.cloud import bigquery
from slugify import slugify

from exceptions import ConfigurationAlreadyExists, InstallationWithSameNameAlreadyExists


logger = logging.getLogger(__name__)


SENSOR_NAME_MAPPING = {
    "Mics": "microphone",
    "Baros_P": "barometer",
    "Baros_T": "barometer_thermometer",
    "Diff_Baros": "differential_barometer",
    "Acc": "accelerometer",
    "Gyro": "gyroscope",
    "Mag": "magnetometer",
    "Analog Vbat": "battery_voltmeter",
    "Constat": "connection_statistics",
}


class BigQueryDataset:
    """A wrapper for the Google BigQuery client for adding sensor data for an installation to a BigQuery dataset.

    :param str project_name: name of Google Cloud project the BigQuery dataset belongs to
    :param str dataset_name: name of the BigQuery dataset
    :return None:
    """

    def __init__(self, project_name, dataset_name):
        self.client = bigquery.Client()
        self.dataset_id = f"{project_name}.{dataset_name}"

        self.table_names = {
            "configuration": f"{self.dataset_id}.configuration",
            "installation": f"{self.dataset_id}.installation",
            "sensor_type": f"{self.dataset_id}.sensor_type",
            "sensor_data": f"{self.dataset_id}.sensor_data",
            "microphone_data": f"{self.dataset_id}.microphone_data",
        }

    def add_sensor_data(self, data, configuration_id, installation_reference, label=None):
        """Insert sensor data into the dataset for the given configuration and installation references.

        :param dict data: data from the sensors - the keys are the sensor names and the values are samples in the form of lists of lists
        :param str configuration_id: the UUID of the configuration used to produce the given data
        :param str installation_reference: the reference (name) of the installation that produced the data
        :param str|None label: an optional label relevant to the given data
        :raise ValueError: if the insertion fails
        :return None:
        """
        rows = []

        for sensor_name, samples in data.items():
            sensor_type_reference = SENSOR_NAME_MAPPING[sensor_name]

            for sample in samples:
                rows.append(
                    {
                        "datetime": datetime.datetime.fromtimestamp(sample[0]),
                        "sensor_type_reference": sensor_type_reference,
                        "sensor_value": sample[1:],
                        "configuration_id": configuration_id,
                        "installation_reference": installation_reference,
                        "label": label,
                    }
                )

        errors = self.client.insert_rows(table=self.client.get_table(self.table_names["sensor_data"]), rows=rows)

        if errors:
            raise ValueError(errors)

        logger.info("Uploaded %d samples of sensor data to BigQuery dataset %r.", len(rows), self.dataset_id)

    def record_microphone_data_location_and_metadata(
        self,
        path,
        project_name,
        configuration_id,
        installation_reference,
        label=None,
    ):
        """Record the file location and metadata for a window of microphone data.

        :param str path: the Google Cloud Storage path to the microphone data
        :param str project_name: the name of the project the storage bucket belongs to
        :param str configuration_id: the UUID of the configuration used to produce the data
        :param str installation_reference: the reference for the installation that produced the data
        :param str|None label: the label applied to the gateway session that produced the data
        :raise ValueError: if the addition fails
        :return None:
        """
        errors = self.client.insert_rows(
            table=self.client.get_table(self.table_names["microphone_data"]),
            rows=[
                {
                    "path": path,
                    "project_name": project_name,
                    "configuration_id": configuration_id,
                    "installation_reference": installation_reference,
                    "label": label,
                }
            ],
        )

        if errors:
            raise ValueError(errors)

        logger.info("Added microphone data location and metadata to BigQuery dataset %r.", self.dataset_id)

    def add_sensor_type(self, name, description=None, measuring_unit=None, metadata=None):
        """Add a new sensor type to the BigQuery dataset. The sensor name is slugified on receipt.

        :param str name: the name of the new sensor
        :param str|None description: a description of what the sensor is and does
        :param str|None measuring_unit: the unit the sensor measures its relevant quantity in
        :param dict|None metadata: any useful metadata about the sensor e.g. sensitivities
        :raise ValueError: if the addition fails
        :return None:
        """
        reference = slugify(name)
        metadata = json.dumps(metadata or {})

        errors = self.client.insert_rows(
            table=self.client.get_table(self.table_names["sensor_type"]),
            rows=[
                {
                    "reference": reference,
                    "name": name,
                    "description": description,
                    "unit": measuring_unit,
                    "metadata": metadata,
                }
            ],
        )

        if errors:
            raise ValueError(errors)

        logger.info("Added new sensor %r to BigQuery dataset %r.", reference, self.dataset_id)

    def add_installation(self, reference, turbine_id, blade_id, hardware_version, sensor_coordinates, location=None):
        """Add a new installation to the BigQuery dataset.

        :param str reference: the name to give to the installation
        :param str turbine_id:
        :param str blade_id:
        :param str hardware_version: the version of the sensor hardware at this installation
        :param dict sensor_coordinates: sensor name mapped to an array of (x, y, r) coordinates for each individual sensor
        :param str|None location: the geographical location of the installation in WKT format if relevant (it may not be if it's a wind tunnel which could be set up anywhere)
        :raise cloud_functions.exceptions.InstallationWithSameNameAlreadyExists: if an installation with the given name already exists
        :raise ValueError: if the addition fails
        :return None:
        """
        installation_already_exists = (
            len(
                list(
                    self.client.query(
                        f"SELECT 1 FROM `{self.table_names['installation']}` WHERE `reference`='{reference}' LIMIT 1"
                    ).result()
                )
            )
            > 0
        )

        if installation_already_exists:
            raise InstallationWithSameNameAlreadyExists(
                f"An installation with the reference {reference!r} already exists."
            )

        errors = self.client.insert_rows(
            table=self.client.get_table(self.table_names["installation"]),
            rows=[
                {
                    "reference": reference,
                    "turbine_id": turbine_id,
                    "blade_id": blade_id,
                    "hardware_version": hardware_version,
                    "sensor_coordinates": json.dumps(sensor_coordinates),
                    "location": location,
                }
            ],
        )

        if errors:
            raise ValueError(errors)

        logger.info("Added new installation %r to BigQuery dataset %r.", reference, self.dataset_id)

    def add_configuration(self, configuration):
        """Add a configuration to the BigQuery dataset.

        :param dict configuration: the configuration to add
        :raise cloud_functions.exceptions.ConfigurationAlreadyExists: if an identical configuration already exists in the dataset or the write operation fails; this error includes the UUID of the existing configuration as an argument
        :raise ValueError: if the addition fails
        :return str: UUID of the configuration
        """
        configuration = copy.deepcopy(configuration)

        # Installation data is stored in a separate column, so pop it before the next step.
        installation_data = configuration.pop("installation_data")

        software_configuration_json = json.dumps(configuration)
        software_configuration_hash = blake3(software_configuration_json.encode()).hexdigest()

        configurations = list(
            self.client.query(
                f"SELECT id FROM `{self.table_names['configuration']}` WHERE `software_configuration_hash`='{software_configuration_hash}' "
                f"LIMIT 1"
            ).result()
        )

        if len(configurations) > 0:
            raise ConfigurationAlreadyExists(
                f"An identical configuration already exists in the database with UUID {configurations[0].id}.",
                configurations[0].id,
            )

        configuration_id = str(uuid.uuid4())
        installation_data_json = json.dumps(installation_data)
        installation_data_hash = blake3(installation_data_json.encode()).hexdigest()

        errors = self.client.insert_rows(
            table=self.client.get_table(self.table_names["configuration"]),
            rows=[
                {
                    "id": configuration_id,
                    "software_configuration": software_configuration_json,
                    "software_configuration_hash": software_configuration_hash,
                    "installation_data": installation_data_json,
                    "installation_data_hash": installation_data_hash,
                }
            ],
        )

        if errors:
            raise ValueError(errors)

        logger.info("Added configuration %r to BigQuery dataset %r.", configuration_id, self.dataset_id)
        return configuration_id
