import json
import os
import time
import unittest

from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.utils.encoders import OctueJSONEncoder

from tests.base import DatasetMixin


SOURCE_PROJECT_NAME = "aerosense-twined"
SOURCE_BUCKET_NAME = "test-aerosense-ingress-eu"
DESTINATION_PROJECT_NAME = SOURCE_PROJECT_NAME
DESTINATION_BUCKET_NAME = "test-data-gateway-processed-data"


class TestDeployment(unittest.TestCase, DatasetMixin):
    storage_client = GoogleCloudStorageClient(SOURCE_PROJECT_NAME)

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

            time.sleep(10)

            # Check configuration has been persisted in the destination bucket.
            self.assertEqual(
                json.loads(self.storage_client.download_as_string(cloud_path=destination_path)),
                configuration,
            )

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
            batch = self.random_batch()

            self.storage_client.upload_from_string(
                string=json.dumps(batch, cls=OctueJSONEncoder),
                cloud_path=upload_path,
                metadata={"data_gateway__configuration": self.VALID_CONFIGURATION},
            )

            time.sleep(10)

            # Check that cleaned batch has been created and is in the right place.
            cleaned_batch = json.loads(self.storage_client.download_as_string(cloud_path=destination_path))
            self.assertEqual(cleaned_batch["cleaned"], True)
            self.assertIn("Mics", cleaned_batch)

        finally:
            self.storage_client.delete(cloud_path=upload_path)
            self.storage_client.delete(cloud_path=destination_path)
