import datetime
import json
import os
import unittest

import numpy as np
from octue.cloud.emulators import GoogleCloudStorageEmulatorTestResultModifier

from tests import TEST_BUCKET_NAME


class BaseTestCase(unittest.TestCase):
    test_result_modifier = GoogleCloudStorageEmulatorTestResultModifier(default_bucket_name=TEST_BUCKET_NAME)
    setattr(unittest.TestResult, "startTestRun", test_result_modifier.startTestRun)
    setattr(unittest.TestResult, "stopTestRun", test_result_modifier.stopTestRun)

    configuration_path = os.path.join(os.path.dirname(__file__), "valid_configuration.json")

    with open(configuration_path) as f:
        VALID_CONFIGURATION = json.load(f)

    def random_dataset(self, rows, cols):
        data = np.random.rand(rows, cols)
        time = np.linspace(0, 10, rows)
        random_data = np.append(np.transpose([time]), data, axis=1)
        return random_data

    def random_batch(self):
        sensors = {"Mics"}

        test_batch = {"sensor_time_offset": datetime.datetime.now().timestamp(), "sensor_data": {}}

        for sensor in sensors:
            test_batch["sensor_data"][sensor] = self.random_dataset(
                int(10 / self.VALID_CONFIGURATION["period"][sensor]),  # 10 seconds long
                self.VALID_CONFIGURATION["n_meas_qty"][sensor],
            )

        return test_batch
