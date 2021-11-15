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

    def random_dataset(self, rows, cols, window_duration):
        data = np.random.rand(rows, cols)
        time = np.linspace(0, window_duration, rows)
        random_data = np.append(np.transpose([time]), data, axis=1)
        return random_data

    def random_window(self, sensors=None, window_duration=None):
        """
        Generates a window with random data
        :param sensors: List with sensor names
        :param window_duration: Unit: s
        """
        sensors = sensors or ["Constat"]
        window_duration = window_duration or 1
        window = {"sensor_time_offset": datetime.datetime.now().timestamp(), "sensor_data": {}}

        for sensor in sensors:
            rows = int(window_duration // self.VALID_CONFIGURATION["period"][sensor]) + 1
            cols = self.VALID_CONFIGURATION["n_meas_qty"][sensor]
            # Update window duration to stop at last sample
            window_duration = (rows - 1) * self.VALID_CONFIGURATION["period"][sensor]
            window["sensor_data"][sensor] = self.random_dataset(rows, cols, window_duration)

        return window


class BaseTestCase(unittest.TestCase, DatasetMixin):
    test_result_modifier = GoogleCloudStorageEmulatorTestResultModifier(default_bucket_name=TEST_BUCKET_NAME)
    setattr(unittest.TestResult, "startTestRun", test_result_modifier.startTestRun)
    setattr(unittest.TestResult, "stopTestRun", test_result_modifier.stopTestRun)

    def setUp(self):
        warnings.simplefilter("ignore", category=ResourceWarning)
