import unittest
from octue.utils.cloud.emulators import GoogleCloudStorageEmulatorTestResultModifier


TEST_PROJECT_NAME = "a-project-name"
TEST_BUCKET_NAME = "a-bucket-name"


test_result_modifier = GoogleCloudStorageEmulatorTestResultModifier(default_bucket_name=TEST_BUCKET_NAME)
setattr(unittest.TestResult, "startTestRun", test_result_modifier.startTestRun)
setattr(unittest.TestResult, "stopTestRun", test_result_modifier.stopTestRun)
