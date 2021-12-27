import json
import os
import unittest

from octue.cloud import storage
from octue.cloud.storage.client import GoogleCloudStorageClient
from octue.utils.encoders import OctueJSONEncoder

from tests.base import DatasetMixin


@unittest.skipUnless(
    condition=os.getenv("RUN_DEPLOYMENT_TESTS", "").lower() == "true",
    reason="'RUN_DEPLOYMENT_TESTS' environment variable is False or not present.",
)
class TestDeployment(unittest.TestCase, DatasetMixin):
    storage_client = GoogleCloudStorageClient(os.environ["TEST_PROJECT_NAME"])

    def test_clean_and_upload_window(self):
        window = self.random_window(sensors=["Constat"], window_duration=1)
        upload_path = storage.path.join(os.environ["TEST_BUCKET_NAME"], "window-0.json")

        try:
            self.storage_client.upload_from_string(
                string=json.dumps(window, cls=OctueJSONEncoder),
                cloud_path=upload_path,
                metadata={"data_gateway__configuration": self.VALID_CONFIGURATION},
            )

        finally:
            self.storage_client.delete(cloud_path=upload_path)
