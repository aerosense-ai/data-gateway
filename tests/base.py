import unittest
from octue.utils.cloud.emulators import GoogleCloudStorageEmulatorTestResultModifier

from tests import TEST_BUCKET_NAME


class BaseTestCase(unittest.TestCase):
    test_result_modifier = GoogleCloudStorageEmulatorTestResultModifier(default_bucket_name=TEST_BUCKET_NAME)
    setattr(unittest.TestResult, "startTestRun", test_result_modifier.startTestRun)
    setattr(unittest.TestResult, "stopTestRun", test_result_modifier.stopTestRun)
