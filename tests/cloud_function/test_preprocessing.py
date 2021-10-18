import datetime
import json
import os
import unittest
import numpy as np

from cloud_function.preprocessing import preprocess


class TestPreprocess(unittest.TestCase):

    path_to_sample_data = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "tests",
        ".test_data",
        "AVENTA",
        "21.06.2021",
        "424536",
        "window-0.json",
    )
    configuration_path = os.path.join(os.path.dirname(__file__), "valid_configuration.json")

    with open(configuration_path) as f:
        VALID_CONFIGURATION = json.load(f)

    def sample_dataset(self):
        with open(self.path_to_sample_data, "r") as in_file:
            sample_data = json.load(in_file)
        return sample_data

    def random_dataset(self, rows, cols):
        data = np.random.rand(rows, cols)
        time = np.linspace(0, 10, rows)
        random_data = np.append(np.transpose([time]), data, axis=1)
        return random_data

    def random_batch(self):
        sensors = {"Mics"}
        # sensors = {"Mics", "Baros_P", "Baros_T", "Acc", "Gyro", "Mag"}

        test_batch = {"sensor_time_offset": datetime.datetime.now().timestamp(), "sensor_data": {}}

        for sensor in sensors:
            test_batch["sensor_data"][sensor] = self.random_dataset(
                int(10 / self.VALID_CONFIGURATION["period"][sensor]),  # 10 seconds long
                self.VALID_CONFIGURATION["n_meas_qty"][sensor],
            )

        return test_batch

    def test_missing_data(self):
        test_batch = self.random_batch()
        test_metadata = self.VALID_CONFIGURATION
        # add some noise to timestamps
        test_batch["sensor_data"]["Mics"][:, 0] += np.random.rand(test_batch["sensor_data"]["Mics"][:, 0].size) / 1e8
        # remove some rows
        test_batch["sensor_data"]["Mics"] = np.delete(test_batch["sensor_data"]["Mics"], slice(40, 61), 0)

        processed_batch = preprocess.run(test_batch, test_metadata)

        # Check if the data got padded with NaN
        self.assertTrue(np.isnan(processed_batch["Mics"][50][1]) and not np.isnan(processed_batch["Mics"][1][1]))

    """
    def test_sample_dataset(self):
        test_batch = self.sample_dataset()
        test_metadata = self.VALID_CONFIGURATION

        processed_batch = preprocess.run(test_batch, test_metadata)

        with open('../../../apps/visualisers/time-series/data/out.json', 'w') as out_file:
            json.dump(processed_batch, out_file)
    """
