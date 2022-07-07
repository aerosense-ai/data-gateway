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

    def random_sensor_data(self, rows, cols, first_sample_time, last_sample_time):
        """Generate a numpy array with time column from first_sample_time to last_sample_time and rows x cols random
        data.

        :param int rows: Number of sensor samples
        :param int cols: Number of sensors of the type
        :param float first_sample_time: First timestamp in seconds
        :param float last_sample_time: Last timestamp in seconds
        :return array random_data:
        """
        data = np.random.rand(rows, cols)
        time = np.linspace(first_sample_time, last_sample_time, rows)
        random_data = np.append(np.transpose([time]), data, axis=1)
        return random_data

    def random_window(self, sensors=None, window_duration=None):
        """Generate a window dict with random data for given sensors and duration of window_duration [sec.]

        :param list sensors: List with sensor names
        :param float window_duration: Unit: s

        :return dict window:
        """
        sensors = sensors or ["Constat"]
        window_duration = window_duration or 1
        window = {"0": {}}

        node_configuration = self.VALID_CONFIGURATION["nodes"]["0"]

        for sensor in sensors:
            rows = int(window_duration // node_configuration["periods"][sensor]) + 1
            cols = node_configuration["number_of_sensors"][sensor]
            # Compute last sample time within the window duration
            last_sample_time = (rows - 1) * node_configuration["periods"][sensor]
            window["0"][sensor] = self.random_sensor_data(rows, cols, 0, last_sample_time)

        return window


class BaseTestCase(unittest.TestCase, DatasetMixin):
    test_result_modifier = GoogleCloudStorageEmulatorTestResultModifier(default_bucket_name=TEST_BUCKET_NAME)
    setattr(unittest.TestResult, "startTestRun", test_result_modifier.startTestRun)
    setattr(unittest.TestResult, "stopTestRun", test_result_modifier.stopTestRun)

    def setUp(self):
        """Set up the test case to ignore extraneous `ResourceWarning`s from the Google Cloud Storage client to clean
        up logs generated during tests.

        :return None:
        """
        warnings.simplefilter("ignore", category=ResourceWarning)
