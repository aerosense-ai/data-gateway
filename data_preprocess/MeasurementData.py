import pandas as pd


class RawSignal:
    def __init__(self, dataframe=pd.DataFrame(), sensor=None):
        self.sensor = sensor
        self.dataframe = dataframe

    def find_missing_data(self, threshold):
        """
        Check for missing
        """
        self.dataframe.asfreq(threshold)

    def resample(self, time_step):
        """
        Resample dataframe to the given time step. Linearly interpolates between samples.

        :param dataframe: dataframe to resample
        :param float time_step: timestep in seconds
        :return: resampled and interpolated data
        """
        # Pandas can handle resampling and interpolation easily
        resampled = self.dataframe.resample("{}S".format(time_step)).mean().interpolate(method="linear")
        return resampled

    def raw_to_variable(self):
        """
        Transform raw bytes to a physical variable.
        """
        pass


def remove_outliers(dataframe, window, std_multiplier):
    """
    Removes outliers outside of the confidence interval using a rolling median and standard deviation.

    :param dataframe: dataframe to be cleaned
    :param int window: window for rolling average
    :param float std_multiplier: multiplier to the rolling standard deviation
    :return: dataframe cleaned from outliers
    """
    rolling_median = dataframe.rolling(window).median()
    rolling_std = dataframe.rolling(window).std()
    # TODO define filtering rule using rolling df here
    cleaned = dataframe[
        (dataframe <= rolling_median + std_multiplier * rolling_std)
        & (dataframe >= rolling_median - std_multiplier * rolling_std)
    ]

    return cleaned
