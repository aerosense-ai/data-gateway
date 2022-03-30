import datetime
import os
import sys
import types
from unittest.mock import Mock, patch

from tests.base import BaseTestCase
from tests.test_cloud_functions import REPOSITORY_ROOT
from tests.test_cloud_functions.mocks import MockBigQueryClient


# Manually add the cloud_functions package to the path (its imports have to be done in a certain way for Google Cloud
# Functions to accept them that doesn't work when running/testing the package locally).
sys.path.insert(0, os.path.abspath(os.path.join(REPOSITORY_ROOT, "cloud_functions")))
from cloud_functions.big_query import (  # noqa
    BigQueryDataset,
    ConfigurationAlreadyExists,
    InstallationWithSameNameAlreadyExists,
    SensorTypeWithSameReferenceAlreadyExists,
)


class TestBigQueryDataset(BaseTestCase):
    def test_insert_sensor_data(self):
        """Test that sensor data can be sent to BigQuery for insertion."""
        data = {
            "Mics": [[1636559720.639327, 1, 2, 3, 4]],
            "Baros_P": [[1636559720.639327, 6, 7, 8, 9]],
            "Baros_T": [[1636559720.639327, 11, 12, 13, 14]],
            "Acc": [[1636559720.639327, 16, 17, 18, 19]],
            "Gyro": [[1636559720.639327, 21, 22, 23, 24]],
            "Mag": [[1636559720.639327, 26, 27, 28, 29]],
            "Analog Vbat": [[1636559720.639327, 31, 32, 33, 34]],
            "Constat": [[1636559720.639327, 36, 37, 38, 39]],
        }

        mock_big_query_client = MockBigQueryClient()

        with patch("big_query.bigquery.Client", return_value=mock_big_query_client):
            BigQueryDataset(project_name="my-project", dataset_name="my-dataset").add_sensor_data(
                data=data,
                configuration_id="dbfed555-1b70-4191-96cb-c22071464b90",
                installation_reference="turbine-1",
                label="my-test",
            )

        self.assertEqual(len(mock_big_query_client.rows[0]), 8)

        expected_rows = [
            {
                "datetime": datetime.datetime(2021, 11, 10, 15, 55, 20, 639327),
                "sensor_type_reference": "microphone",
                "sensor_value": [1, 2, 3, 4],
                "configuration_id": "dbfed555-1b70-4191-96cb-c22071464b90",
                "installation_reference": "turbine-1",
                "label": "my-test",
            },
            {
                "datetime": datetime.datetime(2021, 11, 10, 15, 55, 20, 639327),
                "sensor_type_reference": "barometer",
                "sensor_value": [6, 7, 8, 9],
                "configuration_id": "dbfed555-1b70-4191-96cb-c22071464b90",
                "installation_reference": "turbine-1",
                "label": "my-test",
            },
            {
                "datetime": datetime.datetime(2021, 11, 10, 15, 55, 20, 639327),
                "sensor_type_reference": "barometer_thermometer",
                "sensor_value": [11, 12, 13, 14],
                "configuration_id": "dbfed555-1b70-4191-96cb-c22071464b90",
                "installation_reference": "turbine-1",
                "label": "my-test",
            },
            {
                "datetime": datetime.datetime(2021, 11, 10, 15, 55, 20, 639327),
                "sensor_type_reference": "accelerometer",
                "sensor_value": [16, 17, 18, 19],
                "configuration_id": "dbfed555-1b70-4191-96cb-c22071464b90",
                "installation_reference": "turbine-1",
                "label": "my-test",
            },
            {
                "datetime": datetime.datetime(2021, 11, 10, 15, 55, 20, 639327),
                "sensor_type_reference": "gyroscope",
                "sensor_value": [21, 22, 23, 24],
                "configuration_id": "dbfed555-1b70-4191-96cb-c22071464b90",
                "installation_reference": "turbine-1",
                "label": "my-test",
            },
            {
                "datetime": datetime.datetime(2021, 11, 10, 15, 55, 20, 639327),
                "sensor_type_reference": "magnetometer",
                "sensor_value": [26, 27, 28, 29],
                "configuration_id": "dbfed555-1b70-4191-96cb-c22071464b90",
                "installation_reference": "turbine-1",
                "label": "my-test",
            },
            {
                "datetime": datetime.datetime(2021, 11, 10, 15, 55, 20, 639327),
                "sensor_type_reference": "battery_voltmeter",
                "sensor_value": [31, 32, 33, 34],
                "configuration_id": "dbfed555-1b70-4191-96cb-c22071464b90",
                "installation_reference": "turbine-1",
                "label": "my-test",
            },
            {
                "datetime": datetime.datetime(2021, 11, 10, 15, 55, 20, 639327),
                "sensor_type_reference": "connection_statistics",
                "sensor_value": [36, 37, 38, 39],
                "configuration_id": "dbfed555-1b70-4191-96cb-c22071464b90",
                "installation_reference": "turbine-1",
                "label": "my-test",
            },
        ]

        self.assertEqual(mock_big_query_client.rows[0], expected_rows)

    def test_add_new_sensor_type(self):
        """Test that new sensor types can be added."""
        mock_big_query_client = MockBigQueryClient(expected_query_result=[])

        with patch("big_query.bigquery.Client", return_value=mock_big_query_client):
            BigQueryDataset(project_name="my-project", dataset_name="my-dataset").add_sensor_type(
                name="My sensor_Name",
                reference="my-sensor-name",
            )

        self.assertEqual(
            mock_big_query_client.rows[0][0],
            {
                "reference": "my-sensor-name",
                "name": "My sensor_Name",
                "description": None,
                "unit": None,
                "metadata": "{}",
            },
        )

    def test_add_new_sensor_type_raises_error_if_sensor_type_already_exists(self):
        """Test that an error is raised if attempting to add a new sensor type that already exists."""
        mock_big_query_client = MockBigQueryClient(
            expected_query_result=[types.SimpleNamespace(reference="my-sensor-type")]
        )

        with patch("big_query.bigquery.Client", return_value=mock_big_query_client):
            dataset = BigQueryDataset(project_name="my-project", dataset_name="my-dataset")

            with self.assertRaises(SensorTypeWithSameReferenceAlreadyExists):
                dataset.add_sensor_type(name="My sensor_Type", reference="my-sensor-type")

    def test_add_installation(self):
        """Test that installations can be added."""
        mock_big_query_client = MockBigQueryClient(expected_query_result=[])

        with patch("big_query.bigquery.Client", return_value=mock_big_query_client):
            BigQueryDataset(project_name="my-project", dataset_name="my-dataset").add_installation(
                reference="my-installation",
                turbine_id="my-turbine",
                blade_id="my-blade",
                hardware_version="1.0.0",
                sensor_coordinates={"my-sensor": [[0, 1, 2], [3, 8, 7]]},
            )

        self.assertEqual(
            mock_big_query_client.rows[0][0],
            {
                "reference": "my-installation",
                "turbine_id": "my-turbine",
                "blade_id": "my-blade",
                "hardware_version": "1.0.0",
                "sensor_coordinates": '{"my-sensor": [[0, 1, 2], [3, 8, 7]]}',
                "location": None,
            },
        )

    def test_add_installation_raises_error_if_installation_already_exists(self):
        """Test that an error is raised if attempting to add an installation that already exists."""
        mock_big_query_client = MockBigQueryClient(
            expected_query_result=[types.SimpleNamespace(reference="my-installation")]
        )

        with patch("big_query.bigquery.Client", return_value=mock_big_query_client):
            dataset = BigQueryDataset(project_name="my-project", dataset_name="my-dataset")

            with self.assertRaises(InstallationWithSameNameAlreadyExists):
                dataset.add_installation(
                    reference="my-installation",
                    turbine_id="my-turbine",
                    blade_id="my-blade",
                    hardware_version="1.0.0",
                    sensor_coordinates={"my-sensor": [[0, 1, 2], [3, 8, 7]]},
                )

    def test_add_configuration(self):
        """Test that a configuration can be added."""
        mock_big_query_client = MockBigQueryClient(expected_query_result=[])

        with patch("big_query.bigquery.Client", return_value=mock_big_query_client):
            BigQueryDataset(project_name="my-project", dataset_name="my-dataset").add_configuration(
                configuration={"blah": "blah", "installation_data": {"stuff": "data"}}
            )

        del mock_big_query_client.rows[0][0]["id"]

        self.assertEqual(
            mock_big_query_client.rows[0][0],
            {
                "software_configuration": '{"blah": "blah"}',
                "software_configuration_hash": "a9a553b17102e3f08a1ca32486086cdb8699f8f50c358b0fed8071b1d4c11bb2",
                "installation_data": '{"stuff": "data"}',
                "installation_data_hash": "9fe86ad8340043710ee7c69ac555ad2e6981dc7a2c6d18f1757b899de5de1240",
            },
        )

    def test_add_configuration_raises_error_if_installation_already_exists(self):
        """Test that an error is raised if attempting to add a configuration that already exists and that the ID of the
        existing configuration is returned.
        """
        existing_configuration_id = "0846401a-89fb-424e-89e6-039063e0ee6d"
        mock_big_query_client = MockBigQueryClient(expected_query_result=[Mock(id=existing_configuration_id)])

        with patch("big_query.bigquery.Client", return_value=mock_big_query_client):
            dataset = BigQueryDataset(project_name="my-project", dataset_name="my-dataset")

            with self.assertRaises(ConfigurationAlreadyExists):
                configuration_id = dataset.add_configuration(
                    configuration={"blah": "blah", "installation_data": {"stuff": "data"}}
                )

                self.assertEqual(configuration_id, existing_configuration_id)
