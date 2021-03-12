from io import StringIO
import pandas as pd

from data_preprocess import functions


def run(raw_batch, batch_metadata):
    """
    Run pre-process operations on the raw data batch.

    :param dict raw_batch: raw batch to process
    :param dict batch_metadata: dict with batch metadata
    :return: cleaned data
    """

    # TODO think on how to deal with missing data..

    processed_batch = {}

    # Pressure pre-process
    raw_pressure_bytes = pd.read_csv(
        StringIO(raw_batch["Baros"]), delimiter=",", index_col=0, header=None, parse_dates=[0]
    )

    cleaned_pressure = functions.remove_outliers(raw_pressure_bytes, 10, 10)
    resampled_pressure = functions.resample(cleaned_pressure, 0.005)
    processed_batch["Baros"] = resampled_pressure.to_csv(header=False, index=False)
    # TODO check dt on raw bytes,
    # TODO make a log of pre-process ... compare data availbility before and after claibration...

    return processed_batch
