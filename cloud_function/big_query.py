import datetime
import json
import uuid

from blake3 import blake3
from exceptions import ConfigurationAlreadyExists
from google.cloud import bigquery


sensor_name_mapping = {
    "Mics": "microphone",
    "Baros_P": "barometer_pressure_sensor",
    "Baros_T": "barometer_thermometer",
    "Acc": "accelerometer",
    "Gyro": "gyroscope",
    "Mag": "magnetometer",
    "Analog Vbat": "analogue_battery_voltmeter",
    "Constat": "connection_statistics",
}


class BigQueryDataset:
    """A wrapper for the Google BigQuery client for adding sensor data for an installation to a BigQuery dataset.

    :param str project_name:
    :param str dataset_name:
    :return None:
    """

    def __init__(self, project_name, dataset_name):
        self.client = bigquery.Client()
        self.dataset_id = f"{project_name}.{dataset_name}"

    def insert_sensor_data(self, data, configuration_id, installation_reference, label=None):
        """Insert sensor data into the dataset for the given configuration and installation references.

        :param dict data:
        :param str configuration_id:
        :param str installation_reference:
        :param str|None label:
        :raise ValueError: if the write operation fails
        :return None:
        """
        rows = []
        table_name = f"{self.dataset_id}.sensor_data"

        for sensor_name, samples in data["sensor_data"].items():
            sensor_type_reference = sensor_name_mapping[sensor_name]

            for sample in samples:
                rows.append(
                    {
                        "datetime": datetime.datetime.fromtimestamp(data["sensor_time_offset"] + sample[0]),
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

    def add_new_sensor_type(self, name, description=None, measuring_unit=None, metadata=None):
        """Add a new sensor type to the BigQuery dataset.

        :param str name:
        :param str|None description:
        :param str|None measuring_unit:
        :param dict|None metadata:
        :raise ValueError: if the write operation fails
        :return None:
        """
        reference = name.replace("_", "-").replace(" ", "-").lower()
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

    def add_installation(self, reference, hardware_version, location=None):
        """Add a new installation to the BigQuery dataset.

        :param str reference:
        :param str hardware_version:
        :param str|None location:
        :raise ValueError: if the write operation fails
        :return None:
        """
        reference = reference.replace("_", "-").replace(" ", "-").lower()

        errors = self.client.insert_rows(
            table=self.client.get_table(f"{self.dataset_id}.installation"),
            rows=[{"reference": reference, "hardware_version": hardware_version, "location": location}],
        )

        if errors:
            raise ValueError(errors)

    def add_configuration(self, configuration):
        """Add a configuration to the BigQuery dataset.

        :param dict configuration:
        :raise cloud_function.exceptions.ConfigurationAlreadyExists: if an identical configuration already exists in the dataset or the write operation fails; this error includes the UUID of the existing configuration as an argument
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

        return configuration_id
