"""
A quick script used to transform old batches saved during interactive mode (legacy_json) with
csv format. A new_json file stores data as in a nested list format.
"""

import json
from io import StringIO

import pandas as pd


path_to_legacy_json = "tests/.test_data/AVENTA/21.06.2021/677196/window-0.json"
path_to_new_json = "window-0.json"

with open(path_to_legacy_json, "r") as json_dump:
    data = json.load(json_dump)

sensor_dict = {}
processed_batch = {"sensor_time_offset": "POSIX TIMESTAMP HERE"}

for key in data.keys():
    # Convert CSV to Pandas
    df = pd.read_csv(StringIO(data[key]), delimiter=",", header=None)
    sensor_dict[key] = df.iloc[:, :-1].values.tolist()

processed_batch["sensor_data"] = sensor_dict

with open(path_to_new_json, "w") as out_file:
    # Note: float('nan') will be saved as NaN, which is not strictly supported by json, but json.load will parse it
    json.dump(processed_batch, out_file)
