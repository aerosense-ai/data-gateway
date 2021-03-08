def correct_for_temperature():
    """
    Correct pressure sensor reading using temperature and humidity
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


def resample(dataframe, time_step):
    """
    Resample dataframe to the given time step. Linearly interpolates between samples.

    :param dataframe: dataframe to resample
    :param float time_step: timestep in seconds
    :return: resampled and interpolated data
    """
    # Pandas can handle resampling and interpolation easily
    resampled = dataframe.resample("{}S".format(time_step)).mean().interpolate(method="linear")
    return resampled
