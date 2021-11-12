import json
import os
from unittest.mock import MagicMock, patch

from flask import Flask, request
from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.utils.encoders import OctueJSONEncoder

import sys
from tests import TEST_BUCKET_NAME  # noqa
from tests.base import BaseTestCase  # noqa
from tests.test_cloud_functions import REPOSITORY_ROOT


# Manually add the cloud_functions package to the path (its imports have to be done in a certain way for Google Cloud
# Functions to accept them that doesn't work when running/testing the package locally).
sys.path.insert(0, os.path.abspath(os.path.join(REPOSITORY_ROOT, "cloud_functions")))

from cloud_functions import main  # noqa
from cloud_functions.main import InstallationWithSameNameAlreadyExists, create_installation  # noqa


class TestCleanAndUploadWindow(BaseTestCase):
    SOURCE_PROJECT_NAME = "source-project"
    SOURCE_BUCKET_NAME = TEST_BUCKET_NAME

    def test_clean_and_upload_window(self):
        """Test that a window file is cleaned and uploaded to its destination bucket following the relevant Google Cloud
        storage trigger. The same source and destination bucket are used in this test although different ones will most
        likely be used in production.
        """
        window = self.random_window(10, 10)

        GoogleCloudStorageClient(self.SOURCE_PROJECT_NAME).upload_from_string(
            string=json.dumps(window, cls=OctueJSONEncoder),
            bucket_name=self.SOURCE_BUCKET_NAME,
            path_in_bucket="window-0.json",
            metadata={"data_gateway__configuration": self.VALID_CONFIGURATION},
        )

        event = {
            "bucket": self.SOURCE_BUCKET_NAME,
            "name": "window-0.json",
            "metageneration": "some-metageneration",
            "timeCreated": "0",
            "updated": "0",
        }

        with patch.dict(
            os.environ,
            {
                "SOURCE_PROJECT_NAME": self.SOURCE_PROJECT_NAME,
                "DESTINATION_PROJECT_NAME": "destination-project",
                "BIG_QUERY_DATASET_NAME": "blah",
            },
        ):
            with patch("file_handler.BigQueryDataset") as mock_dataset:
                main.clean_and_upload_window(event=event, context=self._make_mock_context())

        # Check configuration without user data was added.
        del self.VALID_CONFIGURATION["user_data"]
        self.assertIn("add_configuration", mock_dataset.mock_calls[1][0])
        self.assertEqual(mock_dataset.mock_calls[1].args[0], self.VALID_CONFIGURATION)

        # Check data was persisted.
        self.assertIn("insert_sensor_data", mock_dataset.mock_calls[2][0])
        self.assertEqual(mock_dataset.mock_calls[2].kwargs["data"].keys(), {"Mics", "cleaned"})
        self.assertEqual(mock_dataset.mock_calls[2].kwargs["installation_reference"], "aventa_turbine")
        self.assertEqual(mock_dataset.mock_calls[2].kwargs["label"], "my_test_1")

    @staticmethod
    def _make_mock_context():
        """Make a mock Google Cloud Functions event context object.

        :return unittest.mock.MagicMock:
        """
        context = MagicMock()
        context.event_id = "some-id"
        context.event_type = "google.storage.object.finalize"


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

    def test_error_raised_if_non_post_method_used(self):
        with self.app.test_client() as client:
            response = client.get("/")

        self.assertEqual(response.status_code, 405)

    def test_create_installation_with_invalid_data(self):
        """Test that invalid data sent to the installation creation endpoint return a 400 status code and a relevant
        error.
        """
        with patch.dict(os.environ, values={"DESTINATION_PROJECT_NAME": "blah", "BIG_QUERY_DATASET_NAME": "blah"}):
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
                        response = client.post(json=data)

                        self.assertEqual(response.status_code, 400)
                        self.assertIn(expected_error_field, response.json["fieldErrors"])

    def test_error_raised_if_installation_reference_already_exists(self):
        with patch.dict(os.environ, values={"DESTINATION_PROJECT_NAME": "blah", "BIG_QUERY_DATASET_NAME": "blah"}):
            with patch("cloud_functions.main.BigQueryDataset") as mock_big_query_dataset:
                mock_big_query_dataset.side_effect = InstallationWithSameNameAlreadyExists()

                with self.app.test_client() as client:
                    response = client.post(json={"reference": "hello", "hardware_version": "0.0.1"})

        self.assertEqual(response.status_code, 409)

    def test_error_raised_if_internal_server_error_occurs(self):
        with patch.dict(os.environ, values={"DESTINATION_PROJECT_NAME": "blah", "BIG_QUERY_DATASET_NAME": "blah"}):
            with patch("cloud_functions.main.BigQueryDataset") as mock_big_query_dataset:
                mock_big_query_dataset.side_effect = Exception()

                with self.app.test_client() as client:
                    response = client.post(json={"reference": "hello", "hardware_version": "0.0.1"})

        self.assertEqual(response.status_code, 500)

    def test_create_installation_with_valid_data(self):
        """Test sending valid data for all fields to the installation creation endpoint works and returns a 200 status
        code.
        """
        with patch.dict(os.environ, values={"DESTINATION_PROJECT_NAME": "blah", "BIG_QUERY_DATASET_NAME": "blah"}):
            with patch("cloud_functions.main.BigQueryDataset"):
                with self.app.test_client() as client:
                    response = client.post(
                        json={"reference": "hello", "hardware_version": "0.0.1", "latitude": "0", "longitude": "1"}
                    )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json, {"reference": "hello", "hardware_version": "0.0.1", "latitude": 0, "longitude": 1}
        )

    def test_create_installation_with_only_required_inputs(self):
        """Test sending valid data for only required fields to the installation creation endpoint works and returns a
        200 status code.
        """
        with patch.dict(os.environ, values={"DESTINATION_PROJECT_NAME": "blah", "BIG_QUERY_DATASET_NAME": "blah"}):
            with patch("cloud_functions.main.BigQueryDataset"):
                with self.app.test_client() as client:
                    response = client.post(json={"reference": "hello", "hardware_version": "0.0.1"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["latitude"], None)
        self.assertEqual(response.json["longitude"], None)
