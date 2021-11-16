import datetime
import json
import logging
import uuid

from blake3 import blake3
from exceptions import ConfigurationAlreadyExists, InstallationWithSameNameAlreadyExists
from google.cloud import bigquery
from slugify import slugify


logger = logging.getLogger(__name__)


SENSOR_NAME_MAPPING = {
    "Mics": "microphone",
    "Baros_P": "barometer",
    "Baros_T": "barometer_thermometer",
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

    def insert_sensor_data(self, data, configuration_id, installation_reference, label=None):
        """Insert sensor data into the dataset for the given configuration and installation references.

        :param dict data: data from the sensors - the keys are the sensor names and the values are samples in the form of lists of lists
        :param str configuration_id: the UUID of the configuration used to produce the given data
        :param str installation_reference: the reference (name) of the installation that produced the data
        :param str|None label: an optional label relevant to the given data
        :raise ValueError: if the insertion fails
        :return None:
        """
        rows = []
        table_name = f"{self.dataset_id}.sensor_data"

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

        errors = self.client.insert_rows(table=self.client.get_table(table_name), rows=rows)

        if errors:
            raise ValueError(errors)

        logger.info("Uploaded %d samples of sensor data to BigQuery dataset %r.", len(rows), self.dataset_id)

    def add_new_sensor_type(self, name, description=None, measuring_unit=None, metadata=None):
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
            table=self.client.get_table(f"{self.dataset_id}.sensor_type"),
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

    def add_installation(self, reference, hardware_version, location=None):
        """Add a new installation to the BigQuery dataset.

        :param str reference: the name to give to the installation
        :param str hardware_version: the version of the sensor hardware at this installation
        :param str|None location: the geographical location of the installation in WKT format if relevant (it may not be if it's a wind tunnel which could be set up anywhere)
        :raise cloud_functions.exceptions.InstallationWithSameNameAlreadyExists: if an installation with the given name already exists
        :raise ValueError: if the addition fails
        :return None:
        """
        table_name = f"{self.dataset_id}.installation"

        installation_already_exists = (
            len(
                list(
                    self.client.query(f"SELECT 1 FROM `{table_name}` WHERE `reference`='{reference}' LIMIT 1").result()
                )
            )
            > 0
        )

        if installation_already_exists:
            raise InstallationWithSameNameAlreadyExists(
                f"An installation with the reference {reference!r} already exists."
            )

        errors = self.client.insert_rows(
            table=self.client.get_table(table_name),
            rows=[{"reference": reference, "hardware_version": hardware_version, "location": location}],
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
        table_name = f"{self.dataset_id}.configuration"

        configuration_json = json.dumps(configuration)
        configuration_hash = blake3(configuration_json.encode()).hexdigest()

        configurations = list(
            self.client.query(f"SELECT id FROM `{table_name}` WHERE `hash`='{configuration_hash}' LIMIT 1").result()
        )

        if len(configurations) > 0:
            raise ConfigurationAlreadyExists(
                f"An identical configuration already exists in the database with UUID {configurations[0].id}.",
                configurations[0].id,
            )

        configuration_id = str(uuid.uuid4())

        errors = self.client.insert_rows(
            table=self.client.get_table(table_name),
            rows=[{"id": configuration_id, "configuration": configuration_json, "hash": configuration_hash}],
        )

        if errors:
            raise ValueError(errors)

        logger.info("Added configuration %r to BigQuery dataset %r.", configuration_id, self.dataset_id)
        return configuration_id
