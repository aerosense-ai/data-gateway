from io import StringIO
import functions
import pandas as pd


def run(raw_batch, batch_metadata):
    """
    Run pre-process operations on the raw data batch.

    :param dict raw_batch: raw batch to process
    :param dict batch_metadata: dict with batch metadata
    :return: cleaned data
    """

    processed_batch = {}
    # Pressure pre-process
    raw_pressure = pd.read_csv(StringIO(raw_batch["Baros"]), delimiter=",", index_col=0, header=None, parse_dates=[0])

    cleaned_pressure = functions.remove_outliers(raw_pressure, 10)
    resampled_pressure = functions.resample(cleaned_pressure, 0.005)
    processed_batch["Baros"] = resampled_pressure.to_csv(header=False, index=False)

    return processed_batch
