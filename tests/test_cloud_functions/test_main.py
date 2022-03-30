import copy
import json
import os
import sys
import types
from unittest.mock import MagicMock, patch

import numpy as np
from flask import Flask, request
from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.resources import Datafile
from octue.utils.encoders import OctueJSONEncoder

from tests import TEST_BUCKET_NAME  # noqa
from tests.base import BaseTestCase  # noqa
from tests.test_cloud_functions import REPOSITORY_ROOT
from tests.test_cloud_functions.mocks import MockBigQueryClient


# Manually add the cloud_functions package to the path (its imports have to be done in a certain way for Google Cloud
# Functions to accept them that doesn't work when running/testing the package locally).
sys.path.insert(0, os.path.abspath(os.path.join(REPOSITORY_ROOT, "cloud_functions")))

from cloud_functions import main  # noqa
from cloud_functions.main import (  # noqa
    InstallationWithSameNameAlreadyExists,
    SensorTypeWithSameReferenceAlreadyExists,
    add_sensor_type,
    create_installation,
)
from cloud_functions.window_handler import MICROPHONE_SENSOR_NAME, ConfigurationAlreadyExists  # noqa


class TestUploadWindow(BaseTestCase):
    SOURCE_BUCKET_NAME = TEST_BUCKET_NAME

    MOCK_EVENT = {
        "bucket": SOURCE_BUCKET_NAME,
        "name": "window-0.json",
        "metageneration": "some-metageneration",
        "timeCreated": "0",
        "updated": "0",
    }

    @classmethod
    def setUpClass(cls):
        """Create the destination bucket for the tests.

        :return None:
        """
        GoogleCloudStorageClient().create_bucket("destination-bucket")

    def test_upload_window(self):
        """Test that a window file is uploaded to its destination bucket following the relevant Google Cloud
        storage trigger.
        """
        window = BaseTestCase().random_window(sensors=["Constat"], window_duration=1)

        GoogleCloudStorageClient().upload_from_string(
            string=json.dumps(window, cls=OctueJSONEncoder),
            cloud_path=storage.path.generate_gs_path(self.SOURCE_BUCKET_NAME, "window-0.json"),
            metadata={"data_gateway__configuration": self.VALID_CONFIGURATION},
        )

        with patch.dict(
            os.environ,
            {
                "DESTINATION_PROJECT_NAME": "destination-project",
                "DESTINATION_BUCKET_NAME": "destination-bucket",
                "BIG_QUERY_DATASET_NAME": "blah",
            },
        ):
            with patch("big_query.bigquery.Client"):
                with patch("window_handler.BigQueryDataset") as mock_dataset:
                    main.upload_window(event=self.MOCK_EVENT, context=self._make_mock_context())

        # Check configuration without user data was added.
        expected_configuration = copy.deepcopy(self.VALID_CONFIGURATION)
        del expected_configuration["session_data"]
        self.assertIn("add_configuration", mock_dataset.mock_calls[1][0])
        self.assertEqual(mock_dataset.mock_calls[1].args[0], expected_configuration)

        # Check data was persisted.
        self.assertIn("add_sensor_data", mock_dataset.mock_calls[2][0])
        self.assertEqual(mock_dataset.mock_calls[2].kwargs["data"].keys(), {"Constat"})
        self.assertEqual(mock_dataset.mock_calls[2].kwargs["installation_reference"], "aventa_turbine")
        self.assertEqual(mock_dataset.mock_calls[2].kwargs["label"], "my-test-1")

    def test_upload_window_with_microphone_data(self):
        """Test that, if present, microphone data is uploaded to cloud storage and its location is recorded in BigQuery."""
        window = BaseTestCase().random_window(sensors=["Constat", MICROPHONE_SENSOR_NAME], window_duration=1)

        storage_client = GoogleCloudStorageClient()

        storage_client.upload_from_string(
            string=json.dumps(window, cls=OctueJSONEncoder),
            cloud_path=storage.path.generate_gs_path(self.SOURCE_BUCKET_NAME, "window-0.json"),
            metadata={"data_gateway__configuration": self.VALID_CONFIGURATION},
        )

        configuration_id = "0ee0f88e-166f-4b9b-9bf1-43f6ff84063a"
        mock_big_query_client = MockBigQueryClient(expected_query_result=[types.SimpleNamespace(id=configuration_id)])

        with patch.dict(
            os.environ,
            {
                "DESTINATION_PROJECT_NAME": "destination-project",
                "DESTINATION_BUCKET_NAME": "destination-bucket",
                "BIG_QUERY_DATASET_NAME": "blah",
            },
        ):
            with patch("big_query.bigquery.Client"):
                with patch("cloud_functions.big_query.bigquery.Client", return_value=mock_big_query_client):
                    main.upload_window(event=self.MOCK_EVENT, context=self._make_mock_context())

        expected_microphone_cloud_path = "gs://destination-bucket/microphone/window-0.hdf5"

        # Check location of microphone data has been recorded.
        self.assertEqual(
            mock_big_query_client.rows[0][0],
            {
                "path": expected_microphone_cloud_path,
                "project_name": "destination-project",
                "configuration_id": configuration_id,
                "installation_reference": "aventa_turbine",
                "label": "my-test-1",
            },
        )

        # Check the microphone data has been uploaded to cloud storage.
        with Datafile(expected_microphone_cloud_path) as (datafile, f):

            # Check from column 1 of the data onwards as the timestamps are adjusted by the cloud function.
            self.assertTrue(
                np.equal(
                    np.array(f["dataset"])[:, 1:],
                    window["sensor_data"][MICROPHONE_SENSOR_NAME][:, 1:],
                ).all()
            )

        # Check non-microphone sensor data was added to BigQuery.
        self.assertEqual(mock_big_query_client.rows[1][0]["sensor_type_reference"], "connection_statistics")
        self.assertEqual(mock_big_query_client.rows[1][0]["configuration_id"], configuration_id)
        self.assertEqual(len(mock_big_query_client.rows[1]), len(window["sensor_data"]["Constat"]))

    def test_upload_window_for_existing_configuration(self):
        """Test that uploading a window with a configuration that already exists in BigQuery does not fail."""
        window = BaseTestCase().random_window(sensors=["Constat"], window_duration=1)

        GoogleCloudStorageClient().upload_from_string(
            string=json.dumps(window, cls=OctueJSONEncoder),
            cloud_path=storage.path.generate_gs_path(self.SOURCE_BUCKET_NAME, "window-0.json"),
            metadata={"data_gateway__configuration": self.VALID_CONFIGURATION},
        )

        with patch.dict(
            os.environ,
            {
                "DESTINATION_PROJECT_NAME": "destination-project",
                "DESTINATION_BUCKET_NAME": "destination-bucket",
                "BIG_QUERY_DATASET_NAME": "blah",
            },
        ):
            with patch(
                "window_handler.BigQueryDataset.add_configuration",
                side_effect=ConfigurationAlreadyExists("blah", "8b9337d8-40b1-4872-b2f5-b1bfe82b241e"),
            ):
                with patch("window_handler.BigQueryDataset.add_sensor_data", return_value=None):
                    with patch("big_query.bigquery.Client"):
                        main.upload_window(event=self.MOCK_EVENT, context=self._make_mock_context())

    @staticmethod
    def _make_mock_context():
        """Make a mock Google Cloud Functions event context object.

        :return unittest.mock.MagicMock:
        """
        context = MagicMock()
        context.event_id = "some-id"
        context.event_type = "google.storage.object.finalize"


class TestAddSensorType(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        """Create a test Flask app.

        :return None:
        """
        app = Flask(__name__)
        cls.app = app

        @app.route("/", methods=["POST"])
        def test_add_sensor_type():
            return add_sensor_type(request)

        os.environ = {**os.environ, "DESTINATION_PROJECT_NAME": "blah", "BIG_QUERY_DATASET_NAME": "blah"}

    @classmethod
    def tearDownClass(cls):
        """Remove the test environment variables from the environment.

        :return None:
        """
        del os.environ["DESTINATION_PROJECT_NAME"]
        del os.environ["BIG_QUERY_DATASET_NAME"]

    def test_error_raised_if_non_post_method_used(self):
        """Test that a 405 error is raised if a method other than `POST` is used on the endpoint."""
        with self.app.test_client() as client:
            response = client.get("/")

        self.assertEqual(response.status_code, 405)

    def test_add_sensor_type_with_invalid_data(self):
        """Test that invalid data sent to the creation endpoint results in a 400 status code and a relevant error."""
        with patch("cloud_functions.main.BigQueryDataset"):
            with self.app.test_client() as client:
                for expected_error_field, data in (
                    ("reference", {"reference": "not slugified", "name": "not slugified"}),
                    ("reference", {"reference": None, "name": "no name"}),
                    ("name", {"reference": "my-sensor-type", "name": None}),
                ):
                    with self.subTest(expected_error_field=expected_error_field, data=data):
                        response = client.post(json=data)
                        self.assertEqual(response.status_code, 400)
                        self.assertIn(expected_error_field, response.json["fieldErrors"])

    def test_error_raised_if_sensor_type_already_exists(self):
        """Test that a 409 error is returned if the sensor type reference sent to the endpoint already exists in the
        BigQuery dataset.
        """
        mock_big_query_client = MockBigQueryClient(
            expected_query_result=[types.SimpleNamespace(reference="my-sensor_type")]
        )

        with patch("cloud_functions.big_query.bigquery.Client", return_value=mock_big_query_client):
            with self.app.test_client() as client:
                response = client.post(
                    json={
                        "reference": "my-sensor-type",
                        "name": "My sensor type",
                    }
                )

        self.assertEqual(response.status_code, 409)

    def test_error_raised_if_internal_server_error_occurs(self):
        """Test that a 500 error is returned if an unspecified error occurs in the endpoint."""
        with patch(
            "cloud_functions.main.BigQueryDataset.add_sensor_type",
            side_effect=Exception("Deliberately raised for test"),
        ):
            with self.app.test_client() as client:
                response = client.post(
                    json={
                        "reference": "my-sensor-type",
                        "name": "My sensor type",
                    }
                )

        self.assertEqual(response.status_code, 500)

    def test_add_sensor_type_with_valid_data_for_all_fields(self):
        """Test sending valid data for all fields to the sensor type creation endpoint works and returns a 200 status
        code.
        """
        data = {
            "reference": "my-sensor-type",
            "name": "My sensor type",
            "description": "This is a sensor of a type.",
            "measuring_unit": "m/s",
            "metadata": {"something": ["blah", "blah"]},
        }

        with patch("cloud_functions.main.BigQueryDataset"):
            with self.app.test_client() as client:
                response = client.post(json=data)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, data)

    def test_add_sensor_type_with_only_required_inputs(self):
        """Test sending valid data for only required fields to the sensor type creation endpoint works and returns a
        200 status code.
        """
        with patch("cloud_functions.main.BigQueryDataset"):
            with self.app.test_client() as client:
                response = client.post(
                    json={
                        "reference": "my-sensor-type",
                        "name": "My sensor type",
                    }
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["description"], None)
        self.assertEqual(response.json["measuring_unit"], None)
        self.assertEqual(response.json["metadata"], None)


class TestCreateInstallation(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        """Create a test Flask app.

        :return None:
        """
        app = Flask(__name__)
        cls.app = app

        @app.route("/", methods=["POST"])
        def test_create_installation():
            return create_installation(request)

        os.environ = {**os.environ, "DESTINATION_PROJECT_NAME": "blah", "BIG_QUERY_DATASET_NAME": "blah"}

    @classmethod
    def tearDownClass(cls):
        """Remove the test environment variables from the environment.

        :return None:
        """
        del os.environ["DESTINATION_PROJECT_NAME"]
        del os.environ["BIG_QUERY_DATASET_NAME"]

    def test_error_raised_if_non_post_method_used(self):
        """Test that a 405 error is raised if a method other than `POST` is used on the endpoint."""
        with self.app.test_client() as client:
            response = client.get("/")

        self.assertEqual(response.status_code, 405)

    def test_create_installation_with_invalid_data(self):
        """Test that invalid data sent to the installation creation endpoint results in a 400 status code and a relevant
        error.
        """
        with patch("cloud_functions.main.BigQueryDataset"):
            with self.app.test_client() as client:

                for expected_error_field, data in (
                    ("reference", {"reference": "not slugified", "hardware_version": "0.0.1"}),
                    ("reference", {"reference": None, "hardware_version": "0.0.1"}),
                    ("hardware_version", {"reference": "is-slugified", "hardware_version": None}),
                    (
                        "longitude",
                        {"reference": "is-slugified", "hardware_version": "0.0.1", "longitude": "not-a-number"},
                    ),
                    (
                        "latitude",
                        {"reference": "is-slugified", "hardware_version": "0.0.1", "latitude": "not-a-number"},
                    ),
                ):
                    with self.subTest(expected_error_field=expected_error_field, data=data):
                        response = client.post(json=data)
                        self.assertEqual(response.status_code, 400)
                        self.assertIn(expected_error_field, response.json["fieldErrors"])

    def test_error_raised_if_installation_reference_already_exists(self):
        """Test that a 409 error is returned if the installation reference sent to the endpoint already exists in the
        BigQuery dataset.
        """
        with patch("cloud_functions.big_query.bigquery.Client"):
            with patch(
                "cloud_functions.main.BigQueryDataset.add_installation",
                side_effect=InstallationWithSameNameAlreadyExists(),
            ):
                with self.app.test_client() as client:
                    response = client.post(
                        json={
                            "reference": "hello",
                            "hardware_version": "0.0.1",
                            "turbine_id": "0",
                            "blade_id": "0",
                            "sensor_coordinates": {"blah_sensor": [[0, 0, 0]]},
                        }
                    )

        self.assertEqual(response.status_code, 409)

    def test_error_raised_if_internal_server_error_occurs(self):
        """Test that a 500 error is returned if an unspecified error occurs in the endpoint."""
        with patch(
            "cloud_functions.main.BigQueryDataset.add_installation",
            side_effect=Exception("Deliberately raised for test"),
        ):
            with self.app.test_client() as client:
                response = client.post(
                    json={
                        "reference": "hello",
                        "hardware_version": "0.0.1",
                        "turbine_id": "0",
                        "blade_id": "0",
                        "sensor_coordinates": {"blah_sensor": [[0, 0, 0]]},
                    }
                )

        self.assertEqual(response.status_code, 500)

    def test_create_installation_with_valid_data_for_all_fields(self):
        """Test sending valid data for all fields to the installation creation endpoint works and returns a 200 status
        code.
        """
        data = {
            "reference": "hello",
            "hardware_version": "0.0.1",
            "turbine_id": "0",
            "blade_id": "0",
            "sensor_coordinates": {"blah_sensor": [[0, 0, 0]]},
            "latitude": 0,
            "longitude": 1,
        }

        with patch("cloud_functions.main.BigQueryDataset"):
            with self.app.test_client() as client:
                response = client.post(json=data)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, data)

    def test_create_installation_with_only_required_inputs(self):
        """Test sending valid data for only required fields to the installation creation endpoint works and returns a
        200 status code.
        """
        with patch("cloud_functions.main.BigQueryDataset"):
            with self.app.test_client() as client:
                response = client.post(
                    json={
                        "reference": "hello",
                        "hardware_version": "0.0.1",
                        "turbine_id": "0",
                        "blade_id": "0",
                        "sensor_coordinates": {"blah_sensor": [[0, 0, 0]]},
                    }
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["latitude"], None)
        self.assertEqual(response.json["longitude"], None)
