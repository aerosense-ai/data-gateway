import json
import os

import numpy as np

from cloud_functions.preprocessing import preprocess
from tests.base import BaseTestCase


class TestPreprocess(BaseTestCase):

    path_to_sample_data = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "tests",
        ".test_data",
        "AVENTA",
        "21.06.2021",
        "424536",
        "window-0.json",
    )

    def sample_dataset(self):
        with open(self.path_to_sample_data, "r") as in_file:
            sample_data = json.load(in_file)
        return sample_data

    def test_missing_data(self):
        sensor = "Constat"
        test_window = self.random_window(
            sensors=[sensor], window_duration=self.VALID_CONFIGURATION["period"][sensor] * 20
        )
        test_metadata = self.VALID_CONFIGURATION
        # add some noise to timestamps
        test_window["sensor_data"]["Constat"][:, 0] += np.random.rand(test_window["sensor_data"][sensor][:, 0].size) / (
            100 / self.VALID_CONFIGURATION["period"][sensor]
        )
        # remove some rows
        test_window["sensor_data"][sensor] = np.delete(test_window["sensor_data"][sensor], slice(4, 8), 0)

        processed_window = preprocess.run(test_window, test_metadata)

        # Check if the data got padded with NaN
        self.assertTrue(np.isnan(processed_window["Constat"][6][1]) and not np.isnan(processed_window["Constat"][1][1]))

    """
    def test_sample_dataset(self):
        test_window = self.sample_dataset()
        test_metadata = self.VALID_CONFIGURATION

        processed_window = preprocess.run(test_window, test_metadata)

        with open('../../../apps/visualisers/time-series/data/out.json', 'w') as out_file:
            json.dump(processed_window, out_file)
    """
