import pandas as pd

from .measurements import RawSignal


def run(raw_window, window_metadata):
    """Pre-process a raw window of data.
    TODO Note: This code wraps pandas and is not optimised for computational efficiency, some things that can be done:
        0. Time chunks of code and see most penalising parts
        1. Index columns instead of .iloc or [column number]
        2. Check the efficiency of appending new columns to a dataframe
        3. Ditch pandas altogether

    :param dict raw_window: raw data window to process
    :param dict window_metadata: dict with window configuration
    :return: cleaned data json
    """
    processed_window = {}

    for sensor in raw_window["sensor_data"].keys():

        # Convert nested lists to Pandas
        df = pd.DataFrame(raw_window["sensor_data"][sensor])
        # Init RawData
        raw_data = RawSignal(df, sensor)

        # Convert to absolute time
        # TODO introduce absolute timestamps on sensors
        raw_data.dataframe[0] += raw_window["sensor_time_offset"]

        # Index dataframe with timestamp as date series
        raw_data.dataframe.index = pd.to_datetime(raw_data.dataframe[0], unit="s")

        # Check for missing data and pad with NaN
        raw_data.find_missing_data(3 * window_metadata["period"][sensor])

        # Transform to constant dt, pad missing gaps with NaN
        # TODO make a log of pre-process ... compare data availbility before and after cleaning...
        raw_data.to_constant_timestep(window_metadata["period"][sensor])

        raw_data.fixed_point_to_measurement_variable()

        processed_window[sensor] = raw_data.dataframe.values.tolist()

    return processed_window
