import copy
import datetime
import hashlib
import importlib.util
import json
import logging
import uuid

from google.cloud import bigquery

from exceptions import (
    ConfigurationAlreadyExists,
    InstallationWithSameNameAlreadyExists,
    SensorTypeWithSameReferenceAlreadyExists,
)


logger = logging.getLogger(__name__)


if importlib.util.find_spec("blake3"):
    from blake3 import blake3
else:
    blake3 = hashlib.sha256
    logger.warning(
        "The blake3 package is not available, so hashlib.sha256 is being used instead. This is probably because blake3 "
        "is only required by the cloud function, where it is separately specified as a requirement. The reason blake3 "
        "is not in the development or production dependencies is because it requires the rust language/bindings to be "
        "available, which adds multiple unnecessary steps when installing data-gateway on Raspberry Pi. blake3 being "
        "unavailable is not a problem for general development, testing, or gateway-only production, but if this warning "
        "shows up in the production cloud function, it is a problem. Pip install blake3 to resume normal behaviour."
    )

INSERT_BATCH_SIZE = 1000

SENSOR_NAME_MAPPING = {
    "Mics": "microphone",
    "Baros_P": "barometer",
    "Baros_T": "barometer_thermometer",
    "Diff_Baros": "differential_barometer",
    "Acc": "accelerometer",
    "Gyro": "gyroscope",
    "Mag": "magnetometer",
    "Analog Vbat": "battery_voltmeter",
    "battery_info": "battery_info",
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
            "measurement_campaign": f"{self.dataset_id}.measurement_campaign",
            "configuration": f"{self.dataset_id}.configuration",
            "installation": f"{self.dataset_id}.installation",
            "sensor_type": f"{self.dataset_id}.sensor_type",
            "sensor_data": f"{self.dataset_id}.sensor_data",
            "microphone_data": f"{self.dataset_id}.microphone_data",
        }

    def add_sensor_data(self, data, node_id, configuration_id, installation_reference, measurement_campaign_reference):
        """Insert sensor data into the dataset for the given configuration and installation references.

        :param dict data: data from the sensors - the keys are the sensor names and the values are samples in the form of lists of lists
        :param str node_id:
        :param str configuration_id: the UUID of the configuration used to produce the given data
        :param str installation_reference: the reference (name) of the installation that produced the data
        :param str measurement_campaign_reference: the reference of the measurement campaign that produced the data
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
                        "node_id": node_id,
                        "sensor_type_reference": sensor_type_reference,
                        "sensor_value": sample[1:],
                        "configuration_id": configuration_id,
                        "installation_reference": installation_reference,
                        "measurement_campaign_reference": measurement_campaign_reference,
                    }
                )

        if len(rows) > 0:
            logger.info("Inserting %s rows into database in batches of %s", len(rows), INSERT_BATCH_SIZE)

            batches = [rows[i : i + INSERT_BATCH_SIZE] for i in range(0, len(rows), INSERT_BATCH_SIZE)]
            for batch in batches:
                errors = self.client.insert_rows(
                    table=self.client.get_table(self.table_names["sensor_data"]), rows=batch
                )

                if errors:
                    raise ValueError(errors)

                logger.info(
                    "Inserted %d of %d samples of sensor data from node %s to BigQuery dataset %r.",
                    len(batch),
                    len(rows),
                    node_id,
                    self.dataset_id,
                )

        else:
            logger.warning(
                "Received 0 samples of sensor data from node %s, skipping insert of data to BigQuery dataset %r",
                node_id,
                self.dataset_id,
            )

    def record_microphone_data_location_and_metadata(
        self,
        path,
        node_id,
        configuration_id,
        installation_reference,
        timestamp,
        measurement_campaign_reference,
    ):
        """Record the file location and metadata for a window of microphone data.

        :param str path: the Google Cloud Storage path to the microphone data
        :param str node_id:
        :param str configuration_id: the UUID of the configuration used to produce the data
        :param str installation_reference: the reference for the installation that produced the data
        :param float timestamp: The posix timestamp coinciding with the first entry in the window
        :param str measurement_campaign_reference: the reference of the measurement campaign that produced the data
        :raise ValueError: if the addition fails
        :return None:
        """
        errors = self.client.insert_rows(
            table=self.client.get_table(self.table_names["microphone_data"]),
            rows=[
                {
                    "datetime": datetime.datetime.fromtimestamp(timestamp),
                    "path": path,
                    "node_id": node_id,
                    "configuration_id": configuration_id,
                    "installation_reference": installation_reference,
                    "measurement_campaign_reference": measurement_campaign_reference,
                }
            ],
        )

        if errors:
            raise ValueError(errors)

        logger.info("Added microphone data location and metadata to BigQuery dataset %r.", self.dataset_id)

    def add_sensor_type(self, name, reference, description=None, measuring_unit=None, metadata=None):
        """Add a new sensor type to the BigQuery dataset. The sensor name is slugified on receipt.

        :param str name: the name of the new sensor
        :param str reference: the reference name for the sensor (usually slugified)
        :param str|None description: a description of what the sensor is and does
        :param str|None measuring_unit: the unit the sensor measures its relevant quantity in
        :param dict|None metadata: any useful metadata about the sensor e.g. sensitivities
        :raise ValueError: if the addition fails
        :return None:
        """
        sensor_type_already_exists = self._get_field_if_exists(
            table_name=self.table_names["sensor_type"],
            field_name="reference",
            comparison_field_name="reference",
            value=reference,
        )

        if sensor_type_already_exists:
            raise SensorTypeWithSameReferenceAlreadyExists(
                f"A sensor type with the reference {reference!r} already exists."
            )

        if not isinstance(metadata, str):
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

    def add_installation(self, reference, turbine_id, receiver_firmware_version, location=None):
        """Add a new installation to the BigQuery dataset.

        :param str reference: the name to give to the installation
        :param str turbine_id:
        :param str receiver_firmware_version: the version of the receiver firmware in this installation
        :param str|None location: the geographical location of the installation in WKT format if relevant (it may not be if it's a wind tunnel which could be set up anywhere)
        :raise cloud_functions.exceptions.InstallationWithSameNameAlreadyExists: if an installation with the given name already exists
        :raise ValueError: if the addition fails
        :return None:
        """
        installation_already_exists = self._get_field_if_exists(
            table_name=self.table_names["installation"],
            field_name="reference",
            comparison_field_name="reference",
            value=reference,
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
                    "receiver_firmware_version": receiver_firmware_version,
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
        gateway_configuration = configuration.pop("gateway")

        nodes_configuration_json = json.dumps(configuration["nodes"])
        nodes_configuration_hash = blake3(nodes_configuration_json.encode()).hexdigest()

        configuration_id = self._get_field_if_exists(
            table_name=self.table_names["configuration"],
            field_name="id",
            comparison_field_name="nodes_configuration_hash",
            value=nodes_configuration_hash,
        )

        if configuration_id:
            raise ConfigurationAlreadyExists(
                f"An identical configuration already exists in the database with UUID {configuration_id}.",
                configuration_id,
            )

        configuration_id = str(uuid.uuid4())
        gateway_configuration_json = json.dumps(gateway_configuration)
        gateway_configuration_hash = blake3(gateway_configuration_json.encode()).hexdigest()

        errors = self.client.insert_rows(
            table=self.client.get_table(self.table_names["configuration"]),
            rows=[
                {
                    "id": configuration_id,
                    "nodes_configuration": nodes_configuration_json,
                    "nodes_configuration_hash": nodes_configuration_hash,
                    "gateway_configuration": gateway_configuration_json,
                    "gateway_configuration_hash": gateway_configuration_hash,
                }
            ],
        )

        if errors:
            raise ValueError(errors)

        logger.info("Added configuration %r to BigQuery dataset %r.", configuration_id, self.dataset_id)
        return configuration_id

    def add_or_update_measurement_campaign(
        self,
        reference,
        start_time,
        end_time,
        installation_reference,
        nodes,
        label=None,
        description=None,
    ):
        """Add a measurement campaign to the BigQuery dataset or, if it already exists, update its end time.

        :param str reference: the reference of the measurement campaign
        :param datetime.datetime start_time: the measurement campaign's start time
        :param datetime.datetime end_time: the measurement campaign's end time
        :param str installation_reference: the reference of the installation the measurement campaign is carried out from
        :param dict nodes: a mapping of each node's ID to a list of its sensors' names
        :param str|None label: an optional label to give the measurement campaign
        :param str|None description: an optional description to give the measurement campaign

        :return None:
        """
        measurement_campaign_exists = self._get_field_if_exists(
            table_name=self.table_names["measurement_campaign"],
            field_name="reference",
            comparison_field_name="reference",
            value=reference,
        )

        if measurement_campaign_exists:
            logger.info("Measurement campaign %r already exists - updating measurement campaign end time.", reference)

            query_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("end_time", "DATETIME", end_time),
                    bigquery.ScalarQueryParameter("measurement_campaign_reference", "STRING", reference),
                ]
            )

            self.client.query(
                f"""UPDATE {self.table_names["measurement_campaign"]}
                SET end_time = @end_time
                WHERE reference = @measurement_campaign_reference;
                """,
                job_config=query_config,
            )
            return

        errors = self.client.insert_rows(
            table=self.client.get_table(self.table_names["measurement_campaign"]),
            rows=[
                {
                    "reference": reference,
                    "start_time": start_time,
                    "end_time": end_time,
                    "installation_reference": installation_reference,
                    "nodes": json.dumps(nodes),
                    "label": label,
                    "description": description,
                }
            ],
        )

        if errors:
            raise ValueError(errors)

        logger.info("Added measurement campaign %r to BigQuery dataset %r.", reference, self.dataset_id)

    def _get_field_if_exists(self, table_name, field_name, comparison_field_name, value):
        """Get the value of the given field for the row of the given table for which the comparison field has the
        given value.

        :param str table_name:
        :param str field_name:
        :param str comparison_field_name:
        :param any value:
        :return str|None:
        """
        result = list(
            self.client.query(
                f"SELECT {field_name} FROM `{table_name}` WHERE `{comparison_field_name}`='{value}' LIMIT 1"
            ).result()
        )

        if result:
            return getattr(result[0], field_name)
