import datetime
import json
import os
import unittest
import warnings

import numpy as np
from octue.cloud.emulators import GoogleCloudStorageEmulatorTestResultModifier

from tests import TEST_BUCKET_NAME


class DatasetMixin:
    configuration_path = os.path.join(os.path.dirname(__file__), "valid_configuration.json")

    with open(configuration_path) as f:
        VALID_CONFIGURATION = json.load(f)

    def random_dataset(self, rows, cols):
        data = np.random.rand(rows, cols)
        time = np.linspace(0, 10, rows)
        random_data = np.append(np.transpose([time]), data, axis=1)
        return random_data

    def random_window(self, rows=None, cols=None):
        sensors = {"Mics"}
        window = {"sensor_time_offset": datetime.datetime.now().timestamp(), "sensor_data": {}}

        for sensor in sensors:
            rows = rows or int(10 / self.VALID_CONFIGURATION["period"][sensor])  # 10 seconds long
            cols = cols or self.VALID_CONFIGURATION["n_meas_qty"][sensor]
            window["sensor_data"][sensor] = self.random_dataset(rows, cols)

        return window


class BaseTestCase(unittest.TestCase, DatasetMixin):
    test_result_modifier = GoogleCloudStorageEmulatorTestResultModifier(default_bucket_name=TEST_BUCKET_NAME)
    setattr(unittest.TestResult, "startTestRun", test_result_modifier.startTestRun)
    setattr(unittest.TestResult, "stopTestRun", test_result_modifier.stopTestRun)

    def setUp(self):
        warnings.simplefilter("ignore", category=ResourceWarning)
