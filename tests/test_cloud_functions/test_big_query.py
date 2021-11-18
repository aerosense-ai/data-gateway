import datetime
import os
import sys
from unittest.mock import Mock, patch

from tests.base import BaseTestCase
from tests.test_cloud_functions import REPOSITORY_ROOT


# Manually add the cloud_functions package to the path (its imports have to be done in a certain way for Google Cloud
# Functions to accept them that doesn't work when running/testing the package locally).
sys.path.insert(0, os.path.abspath(os.path.join(REPOSITORY_ROOT, "cloud_functions")))
from cloud_functions.big_query import (  # noqa
    BigQueryDataset,
    ConfigurationAlreadyExists,
    InstallationWithSameNameAlreadyExists,
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

        with patch("big_query.bigquery.Client.get_table"):
            with patch("big_query.bigquery.Client.insert_rows", return_value=None) as mock_insert_rows:

                BigQueryDataset(project_name="my-project", dataset_name="my-dataset").add_sensor_data(
                    data=data,
                    configuration_id="dbfed555-1b70-4191-96cb-c22071464b90",
                    installation_reference="turbine-1",
                    label="my-test",
                )

        new_rows = mock_insert_rows.call_args.kwargs["rows"]
        self.assertEqual(len(new_rows), 8)

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

        self.assertEqual(new_rows, expected_rows)

    def test_add_new_sensor_type(self):
        """Test that new sensor types can be added and that their references are their names slugified."""
        with patch("big_query.bigquery.Client.get_table"):
            with patch("big_query.bigquery.Client.insert_rows", return_value=None) as mock_insert_rows:

                BigQueryDataset(project_name="my-project", dataset_name="my-dataset").add_sensor_type(
                    name="My sensor_Name"
                )

        self.assertEqual(
            mock_insert_rows.call_args.kwargs["rows"][0],
            {
                "reference": "my-sensor-name",
                "name": "My sensor_Name",
                "description": None,
                "unit": None,
                "metadata": "{}",
            },
        )

    def test_add_installation(self):
        """Test that installations can be added."""
        with patch("big_query.bigquery.Client.get_table"):
            with patch("big_query.bigquery.Client.insert_rows", return_value=None) as mock_insert_rows:
                with patch("big_query.bigquery.Client.query", return_value=Mock(result=lambda: [])):

                    BigQueryDataset(project_name="my-project", dataset_name="my-dataset").add_installation(
                        reference="my-installation",
                        hardware_version="1.0.0",
                    )

        self.assertEqual(
            mock_insert_rows.call_args.kwargs["rows"][0],
            {
                "reference": "my-installation",
                "hardware_version": "1.0.0",
                "location": None,
            },
        )

    def test_add_installation_raises_error_if_installation_already_exists(self):
        """Test that an error is raised if attempting to add an installation that already exists."""
        dataset = BigQueryDataset(project_name="my-project", dataset_name="my-dataset")

        with patch("big_query.bigquery.Client.query", return_value=Mock(result=lambda: [1])):
            with self.assertRaises(InstallationWithSameNameAlreadyExists):
                dataset.add_installation(reference="my-installation", hardware_version="1.0.0")

    def test_add_configuration(self):
        """Test that a configuration can be added."""
        with patch("big_query.bigquery.Client.get_table"):
            with patch("big_query.bigquery.Client.insert_rows", return_value=None) as mock_insert_rows:
                with patch("big_query.bigquery.Client.query", return_value=Mock(result=lambda: [])):

                    BigQueryDataset(project_name="my-project", dataset_name="my-dataset").add_configuration(
                        configuration={"blah": "blah"}
                    )

        del mock_insert_rows.call_args.kwargs["rows"][0]["id"]

        self.assertEqual(
            mock_insert_rows.call_args.kwargs["rows"][0],
            {
                "configuration": '{"blah": "blah"}',
                "hash": "a9a553b17102e3f08a1ca32486086cdb8699f8f50c358b0fed8071b1d4c11bb2",
            },
        )

    def test_add_configuration_raises_error_if_installation_already_exists(self):
        """Test that an error is raised if attempting to add a configuration that already exists and that the ID of the
        existing configuration is returned.
        """
        existing_configuration_id = "0846401a-89fb-424e-89e6-039063e0ee6d"
        dataset = BigQueryDataset(project_name="my-project", dataset_name="my-dataset")

        with patch(
            "big_query.bigquery.Client.query",
            return_value=Mock(result=lambda: [Mock(id=existing_configuration_id)]),
        ):
            with self.assertRaises(ConfigurationAlreadyExists):
                configuration_id = dataset.add_configuration(configuration={"blah": "blah"})
                self.assertEqual(configuration_id, existing_configuration_id)
