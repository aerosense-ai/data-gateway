import json
import os
import time
import unittest
import warnings

import google.api_core.exceptions
from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.utils.encoders import OctueJSONEncoder

from tests.base import DatasetMixin


SOURCE_PROJECT_NAME = "aerosense-twined"
SOURCE_BUCKET_NAME = "test-aerosense-ingress-eu"
DESTINATION_PROJECT_NAME = SOURCE_PROJECT_NAME
DESTINATION_BUCKET_NAME = "test-data-gateway-processed-data"


@unittest.skipUnless(
    condition=os.getenv("RUN_DEPLOYMENT_TESTS", "").lower() == "true",
    reason="'RUN_DEPLOYMENT_TESTS' environment variable is False or not present.",
)
class TestDeployment(unittest.TestCase, DatasetMixin):
    storage_client = GoogleCloudStorageClient(SOURCE_PROJECT_NAME)

    def setUp(self):
        warnings.simplefilter("ignore", category=ResourceWarning)

    def test_configuration_upload(self):
        """Test that uploading a configuration file to the source bucket results in it being uploaded to the destination
        bucket.
        """
        upload_path = f"gs://{SOURCE_BUCKET_NAME}/configuration.json"
        destination_path = f"gs://{DESTINATION_BUCKET_NAME}/configuration.json"

        try:
            with open(os.path.join(os.path.dirname(os.path.dirname(__file__)), "valid_configuration.json")) as f:
                configuration = json.load(f)

            self.storage_client.upload_from_string(string=json.dumps(configuration), cloud_path=upload_path)
            uploaded_configuration = self._poll_for_uploaded_file(destination_path, timeout=30)
            self.assertEqual(uploaded_configuration, configuration)

        finally:
            self.storage_client.delete(cloud_path=upload_path)
            self.storage_client.delete(cloud_path=destination_path)

    def test_upload_batch(self):
        """Test that uploading a batch to the source bucket results in it being cleaned and uploaded to the destination
        bucket.
        """
        upload_path = f"gs://{SOURCE_BUCKET_NAME}/window-0.json"
        destination_path = f"gs://{DESTINATION_BUCKET_NAME}/window-0.json"

        try:
            batch = self.random_batch(10, 10)

            self.storage_client.upload_from_string(
                string=json.dumps(batch, cls=OctueJSONEncoder),
                cloud_path=upload_path,
                metadata={"data_gateway__configuration": self.VALID_CONFIGURATION},
            )

            cleaned_batch = self._poll_for_uploaded_file(destination_path, timeout=30)
            self.assertEqual(cleaned_batch["cleaned"], True)
            self.assertIn("Mics", cleaned_batch)

        finally:
            self.storage_client.delete(cloud_path=upload_path)
            self.storage_client.delete(cloud_path=destination_path)

    def _poll_for_uploaded_file(self, cloud_path, timeout):
        """Poll Google Cloud storage for the file at the given path until it is available or the timeout is reached.

        :param str cloud_path:
        :param float timeout:
        :return dict|None:
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                return json.loads(self.storage_client.download_as_string(cloud_path=cloud_path))
            except google.api_core.exceptions.NotFound:
                time.sleep(5)

        return None
