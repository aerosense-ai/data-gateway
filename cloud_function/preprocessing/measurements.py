import numpy as np
import pandas as pd
from scipy.interpolate import interp1d


class RawSignal:
    def __init__(self, dataframe, sensor):
        self.dataframe = dataframe
        self.sensor = sensor

    def find_missing_data(self, threshold):
        """
        Check for missing data
        """
        self.dataframe[self.dataframe[0].diff() > threshold] = np.NaN

    def to_constant_timestep(self, time_step):
        """
        Resample dataframe to the given time step. Linearly interpolates between samples.

        :param dataframe: dataframe to resample
        :param float time_step: timestep in seconds
        :return: resampled and interpolated data
        """

        old_time_vector = self.dataframe[0] * 1e9
        new_time_vector = pd.date_range(
            start=self.dataframe.index[0], end=self.dataframe.index[-1], freq="{:.12f}S".format(time_step)
        )

        new_dataframe = pd.DataFrame(new_time_vector.values.astype(np.int64) / 1e9, index=new_time_vector)

        for column in self.dataframe.columns[1:]:
            signal = interp1d(old_time_vector, self.dataframe[column], assume_sorted=True)
            new_dataframe[column] = signal(new_time_vector.values.astype(np.int64))

        self.dataframe = new_dataframe

    def remove_outliers(self, window, std_multiplier):
        """
        Removes outliers outside of the confidence interval using a rolling median and standard deviation.

        :param dataframe: dataframe to be cleaned
        :param int window: window for rolling average
        :param float std_multiplier: multiplier to the rolling standard deviation
        :return: dataframe cleaned from outliers
        """
        rolling_median = self.dataframe.rolling(window).median()
        rolling_std = self.sensor.rolling(window).std()
        # TODO define filtering rule using rolling df here
        self.dataframe = self.dataframe[
            (self.dataframe <= rolling_median + std_multiplier * rolling_std)
            & (self.dataframe >= rolling_median - std_multiplier * rolling_std)
        ]

    def fixed_point_to_measurement_variable(self):
        """
        Transform fixed point values to a physical variable.
        """
        if self.sensor == "Baros_P":
            # Pascal
            self.dataframe.iloc[:, 1:] /= 40.96
        if self.sensor == "Baros_T":
            self.dataframe.iloc[:, 1:] /= 100
