import copy
import json
import os
import time
import unittest

import coolname
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

    bigquery_client = bigquery.Client()

    def test_upload_window(self):
        """Test that a window can be uploaded to a cloud bucket, its data processed by the test cloud function, and the
        results uploaded to a test BigQuery instance.
        """
        window = self.random_window(sensors=["Constat", DEFAULT_SENSOR_NAMES[0]], window_duration=1)
        upload_path = storage.path.generate_gs_path(os.environ["TEST_BUCKET_NAME"], "window-0.json")

        configuration = copy.deepcopy(self.VALID_CONFIGURATION)

        session_reference = coolname.generate_slug(4)
        configuration["session"]["reference"] = session_reference

        self.storage_client.upload_from_string(
            string=json.dumps(window, cls=OctueJSONEncoder),
            cloud_path=upload_path,
            metadata={METADATA_CONFIGURATION_KEY: configuration},
        )

        # Poll for the new data in the test BigQuery dataset.
        session_data = self._poll_for_data(
            query=f"SELECT * FROM `test_greta.session` WHERE reference = '{session_reference}'"
        )

        self.assertEqual(session_data, configuration["session"])

        sensor_data = self._poll_for_data(
            query=f"SELECT label FROM `test_greta.sensor_data` WHERE session_reference = '{session_reference}'"
        )

        self.assertTrue(len(sensor_data) > 20)

    def _poll_for_data(self, query, timeout=30):
        """Poll the BigQuery dataset for a result to the given query.

        :param str query:
        :param float|int timeout:
        :return list:
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            results = list(self.bigquery_client.query(query).result())

            if len(results) > 0:
                return results

            time.sleep(1)
