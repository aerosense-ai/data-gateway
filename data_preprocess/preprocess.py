from io import StringIO
import pandas as pd

from data_preprocess.MeasurementData import RawSignal


def run(raw_batch, batch_metadata):
    """
    Run pre-process operations on the raw data batch.

    :param dict raw_batch: raw batch to process
    :param dict batch_metadata: dict with batch metadata
    :return: cleaned data
    """

    # TODO think on how to deal with missing data..

    processed_batch = {}

    for key in raw_batch.keys():
        # Convert CSV to Pandas
        df = pd.read_csv(StringIO(raw_batch[key]), delimiter=",", index_col=0, header=None, parse_dates=[0])
        # Init RawData
        raw_data = RawSignal(df)
        # Check for missing data and pad with NaN
        raw_data.find_missing_data()

    # TODO check dt on raw bytes,
    # TODO make a log of pre-process ... compare data availbility before and after claibration...

    return processed_batch
