import unittest
import numpy as np
import pandas as pd
from scipy.stats import uniform

from data_preprocess import functions


class TestDataPreProcess(unittest.TestCase):
    def sample_dataframe(self):

        data = uniform.rvs(size=100)
        time = pd.date_range("1/1/2000", periods=100, freq="0.01S")

        test_df = pd.DataFrame(data, index=time)
        return test_df

    def test_outlier_detection(self):
        """Tests parameters of outlier detection function definition"""
        df = self.sample_dataframe()
        df.iloc[50, 0] = 0.5 + np.sqrt(1 / 12) * 1000  # Variance of uniform dist is 1/12*(a-b)^2
        cleaned = functions.remove_outliers(df, 10, 3)
        self.assertTrue(np.isnan(cleaned.iloc[50, 0]))

    def test_resample(self):
        """Tests resampling function"""
