import os

import pandas as pd
from scipy import interpolate
import numpy as np
import ahrs
from ahrs import Quaternion, QuaternionArray
from ahrs.filters import Madgwick
import matplotlib.pyplot as plt



def load_sen(name):
    acc_name = os.path.join(raw_path, name + suffix)

    df = pd.read_csv(acc_name)
    df = df.iloc[1:-1, 0:4]  # keep first 4 columns and drop first and last line

    headers = ["time", "x", "y", "z"]
    df.columns = headers

    return df


def interp_sen(sensor, ref, type):
    sensor_interp = pd.DataFrame(columns = ['time', 'x', 'y', 'z'])
    sensor_interp = sensor_interp.assign(time = ref)
    for j in range(1, 4):
        fs = interpolate.interp1d(sensor["time"], sensor.iloc[:, j].astype('float64'), kind = type)
        sensor_interp.iloc[:, j] = fs(ref)
    return sensor_interp


## load data
raw_path = '.\\rawdata'
suffix = '.csv.'

acc_data = load_sen('acc')
gyr_data = load_sen('gyro')
mag_data = load_sen('mag')

# have common timestamp for all 3 sensors
# it seems we have same timestamp for acc and gyr, lets use them as sensor and adapt magnetometer
mins_sensors = [acc_data['time'].min(), gyr_data['time'].min(), mag_data['time'].min()]
maxs_sensors = [acc_data['time'].max(), gyr_data['time'].max(), mag_data['time'].max()]

# crop data acc. to laziest sensor
acc_data_cropped = acc_data[acc_data["time"].ge(max(mins_sensors)) & acc_data["time"].le(min(maxs_sensors))]
gyr_data_cropped = gyr_data[gyr_data["time"].ge(max(mins_sensors)) & gyr_data["time"].le(min(maxs_sensors))]
mag_data_cropped = mag_data[mag_data["time"].ge(max(mins_sensors)) & mag_data["time"].le(min(maxs_sensors))]

# --------- Get unique time vector -----------
# while using pandas, but it's really complex for something simple...
# acc_data_cropped.index = acc_data_cropped['time']
# mag_data_cropped.index = mag_data_cropped['time']
# mag_data_res = mag_data_cropped.reindex(mag_data_cropped.index.union(acc_data_cropped.index)).interpolate(method='index')

time_ref = acc_data_cropped["time"].array

# with scipy it's easier! (still quite complicated for what I want to do...) Maybe it exists a simpler solution?
acc_data_res = interp_sen(acc_data_cropped, time_ref, 'linear')
gyr_data_res = interp_sen(gyr_data_cropped, time_ref, 'linear')
mag_data_res = interp_sen(mag_data_cropped, time_ref, 'linear')

# Dimensionalise sensors correctly according to Raphael's work
# acceleration in m/s^2
acc_data_norm = acc_data_res[["x", "y", "z"]] / (2**15) * 16  # unit: g +- 16g
acc_data_norm = acc_data_norm * 9.81  # unit m/s^2
# gyr_data_norm in rad/s
gyr_data_norm = gyr_data_res[["x", "y", "z"]] / (2**15) * 2000  # Unit: deg / s
gyr_data_norm = gyr_data_norm * np.pi / 180  # Unit: rad / s
# mag_data_norm in mT
mag_data_norm = mag_data_res[["x", "y", "z"]] # Unit: mT should already be correct?

# Lets calculate the position of the sensors
madgwick = Madgwick()  # initialisation function
# It seems interesting to reduce the gain for rotating motion,
# as the gain is highly related to the mean zero gyroscope measurements errors (used for quaternion derivatives)
madgwick.gain = 0.01 # 0.041 for MARG
time_ref = acc_data_res["time"].array
num_samples = len(time_ref)
Q = np.zeros((num_samples, 4))  # Allocation of quaternions
Q[0] = [1.0, 0.0, 0.0, 0.0]  # Initial attitude as a quaternion
for t in range(1, num_samples):
    madgwick.Dt = time_ref[t] - time_ref[t - 1]
    Q[t] = madgwick.updateMARG(Q[t - 1], gyr = np.array(gyr_data_norm.iloc[t]), acc = np.array(acc_data_norm.iloc[t]),
                              mag = np.array(mag_data_norm.iloc[t]) ) #
q = QuaternionArray(Q)
qa = np.degrees(q.to_angles())
q.

#


plt.plot(np.array(time_ref), qa[:, 0])
# plt.plot(np.array(time_ref), qmada[:, 0])
plt.xlim(time_ref[0],time_ref[-1])
plt.show()
# acc_data_cropped.interpolate('time',)
# gyr_data["time"].diff().mean()
# mag_data["time"].diff().mean()
# acc_data.resample()

# df.dtypes
# df.info()
# df.describe(include = "all")

# madgwick = Madgwick(gyr=gyro_data, acc=acc_data)     # Using IMU
