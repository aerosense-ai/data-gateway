import unittest
import numpy as np
import pandas as pd
from scipy.stats import uniform

from data_preprocess import MeasurementData, preprocess


class TestDataPreProcess(unittest.TestCase):
    def sample_dataframe(self, rows, cols):

        data = uniform.rvs(size=rows * cols)
        time = pd.date_range("1/1/2000", periods=rows, freq="0.01S")

        test_df = pd.DataFrame(data.reshape(-1, cols), index=time)
        return test_df

    def sample_batch(self):
        test_batch = {
            "Mics": self.sample_dataframe(100, 10).to_csv(header=False),
            "Baros_P": self.sample_dataframe(10, 40).to_csv(header=False),
            "Baros_T": self.sample_dataframe(10, 40).to_csv(header=False),
            "Acc": self.sample_dataframe(10, 3).to_csv(header=False),
            "Gyro": self.sample_dataframe(10, 3).to_csv(header=False),
            "Mag": self.sample_dataframe(10, 3).to_csv(header=False),
            "Analog Vbat": self.sample_dataframe(10, 1).to_csv(header=False),
            "Constat": self.sample_dataframe(10, 4).to_csv(header=False),
        }

        return test_batch

    def test_outlier_detection(self):
        """Tests parameters of outlier detection function definition"""
        df = self.sample_dataframe(100, 1)
        df.iloc[50, 0] = 0.5 + np.sqrt(1 / 12) * 1000  # Variance of uniform dist is 1/12*(a-b)^2
        cleaned = MeasurementData.remove_outliers(df, 10, 3)
        self.assertTrue(np.isnan(cleaned.iloc[50, 0]))

    def test_missing_data(self):
        test_batch = self.sample_batch()
        test_metadata = {}

        preprocess.run(test_batch, test_metadata)

    def test_resample(self):
        """Tests resampling function"""
