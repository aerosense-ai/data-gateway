.. _cloud_preprocessing:

====================
Cloud pre-processing
====================

.. ATTENTION::
   This part of the documentation is still in progress. Please check back soon.

Structured raw data (bytes) windows are pre-processed by the ``cloud_functions.preprocessing`` package. See
:ref:`here <installation>` for example output data from ``data-gateway`` that is the input to the preprocessor.

The configuration used to read the window is included as metadata on the cloud objects. The following is used currently:

- Node ID

  - WT parent
  - Spanwise location

- Sensor ID for each column

  - Position on the blade (underformed section): x/c, y/c, normal vector,
  - Sampling frequency
  - Calibration matrix (For Baros/possibly IMUs) - coefficients for the calibration function.


Data Loss
=========
All the the sensor data should be checked for data loss. Check if ``dt > 4`` or ``5 * freq``, then overwrite the
last/first reading with ``NaN`` to avoid meaningless interpolations


Sensors
=======

Temperature Data
________________

**1. Outlier detection**

Replace with the average

WIP options:
    Low pass filter
    Peak detection with Rolling average (Check matlab code)

cleaned temperature  = f("Baros_T", outlier detection parameters)


Pressure Data
_____________

**1. Temperature calibration**

p_calibrated = f("Baros_P", cleaned temperature(interpolated to pressure times) , calibration coefficients)

Returns pressure in [Pa]

**2. Outlier detection**
Only for physically non-sensical readings,
Replace peaks with NaN
p_cleaned=f(p_calibrated)

**3. Resampling**
Just for const delta_t

p_resampled =f(p_cleaned, pressure sampling freq)

Out: p_cleaned, t_start, t_end, dt



IMU Data
________
**1. Accelerometer**

**2. Gyrometer**

**3. Magnetometer**

Plan: Hook up some library like AHRS from pip

Audio Data
__________
