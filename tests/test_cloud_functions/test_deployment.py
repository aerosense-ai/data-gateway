import json
import os
import time
import unittest
import uuid

from google.cloud import bigquery
from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.utils.encoders import OctueJSONEncoder

from tests.base import DatasetMixin


@unittest.skipUnless(
    condition=os.getenv("RUN_DEPLOYMENT_TESTS", "0") == "1",
    reason="'RUN_DEPLOYMENT_TESTS' environment variable is False or not present.",
)
class TestDeployment(unittest.TestCase, DatasetMixin):
    if os.getenv("RUN_DEPLOYMENT_TESTS", "0") == "1":
        # The client must be instantiated here to avoid the storage emulator.
        storage_client = GoogleCloudStorageClient(os.environ["TEST_PROJECT_NAME"])

    def test_clean_and_upload_window(self):
        """Test that a window can be uploaded to a cloud bucket, its data processed by the test cloud function, and the
        results uploaded to a test BigQuery instance.
        """
        window = self.random_window(sensors=["Constat"], window_duration=1)
        upload_path = storage.path.join(os.environ["TEST_BUCKET_NAME"], "window-0.json")

        test_label = f"test-{uuid.uuid4()}"
        self.VALID_CONFIGURATION["session_data"]["label"] = test_label

        self.storage_client.upload_from_string(
            string=json.dumps(window, cls=OctueJSONEncoder),
            cloud_path=upload_path,
            metadata={"data_gateway__configuration": self.VALID_CONFIGURATION},
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
