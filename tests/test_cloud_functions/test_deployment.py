import copy
import json
import os
import time
import unittest
import uuid

from google.cloud import bigquery
from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.utils.encoders import OctueJSONEncoder

from data_gateway.configuration import DEFAULT_SENSOR_NAMES
from data_gateway.persistence import METADATA_CONFIGURATION_KEY
from tests.base import DatasetMixin


@unittest.skipUnless(
    condition=os.getenv("RUN_DEPLOYMENT_TESTS", "0") == "1",
    reason="'RUN_DEPLOYMENT_TESTS' environment variable is False or not present.",
)
class TestDeployment(unittest.TestCase, DatasetMixin):
    if os.getenv("RUN_DEPLOYMENT_TESTS", "0") == "1":
        # The client must be instantiated here to avoid the storage emulator.
        storage_client = GoogleCloudStorageClient()

    def test_upload_window(self):
        """Test that a window can be uploaded to a cloud bucket, its data processed by the test cloud function, and the
        results uploaded to a test BigQuery instance.
        """
        window = self.random_window(sensors=["Constat", DEFAULT_SENSOR_NAMES[0]], window_duration=1)
        upload_path = storage.path.generate_gs_path(os.environ["TEST_BUCKET_NAME"], "window-0.json")

        test_label = f"test-{uuid.uuid4()}"
        configuration = copy.deepcopy(self.VALID_CONFIGURATION)
        configuration["session"]["label"] = test_label

        self.storage_client.upload_from_string(
            string=json.dumps(window, cls=OctueJSONEncoder),
            cloud_path=upload_path,
            metadata={METADATA_CONFIGURATION_KEY: configuration},
        )

        bigquery_client = bigquery.Client()

        start_time = time.time()

        # Poll for the new data in the test BigQuery dataset.
        while time.time() - start_time < 30:
            results = list(
                bigquery_client.query(
                    f"SELECT label FROM `test_greta.sensor_data` WHERE label = '{test_label}'"
                ).result()
            )

            if len(results) > 0:
                break

            time.sleep(1)

        self.assertTrue(len(results) > 20)
