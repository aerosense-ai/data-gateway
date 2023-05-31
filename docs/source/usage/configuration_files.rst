.. _configuration_files:

===================
Configuration Files
===================

Configuration options for the gateway are supplied via a configuration file,
which was set up in :ref:`configuration`.

Once the ``gateway start`` command is invoked, the configuration is saved with
the output data (if saving data locally). The configuration is also added to the
metadata on the output files uploaded to the cloud, where it is used by the cloud
ingress to populate the database.

Specifying other configuration files
====================================

The easiest way of specifying the file is to set the ``GATEWAY_CONFIG_FILE`` 
environment variable.

But, there are other ways. If the environment variable is not set, ``data-gateway``
looks for a file named ``config.json`` in the working directory, or the file path
can be overridden in the CLI options (also see ``gateway start --help``):

.. code-block:: shell

    gateway start --config-file=</path/to/config.json>


Useful customisations
=====================

The most useful customisation is to add a ``measurement_campaign_reference`` field:

.. code-block:: javascript

    {
      "gateway": {
        // ...
      },
      "nodes": {
        // ...
      }
      "measurement_campaign": {
        // If you leave out this reference, a new one is created every
        // time you start the gateway. That allows you to then filter
        // down results in the dashboard to the exact run you're doing
        // right now.
        // But, if you're doing a series of related runs and want all
        // the results to be able to be merged, the set the reference
        // value here for continuity across runs. Don't forget to change
        // or remove this if you reuse the file for something else, though.
        "measurement_campaign_reference": "my-measurement-campaign",
        // You can enable further sorting of data by adding
        // campaign-specific labels
        "label": "run-1",
        // And add notes as aide-memoires.
        "description": "It's windy right now and the battery charged up overnight so we're taking the opportunity to run with mic and diff baros turned on."
      }
    }

Further customisation
=====================

Any of the options in the ``data-gateway`` `configuration module <https://github.com/aerosense-ai/data-gateway/blob/main/data_gateway/configuration.py>`_
can be customised by updating entries in the configuration file.

.. warning::
  Moving off the beaten track, especially customising things like handles and packet keys, you really have to know what you're doing!

Here is an example of a more extensive configuration file.

.. code-block:: javascript

    {
      "gateway": {
        "baudrate": 2300000,
        "endian": "little",
        "installation_reference": "my_installation_reference",
        "latitude": 0,
        "longitude": 0,
        "packet_key": 254,
        "packet_key_offset": 245,
        "receiver_firmware_version": "1.2.3",
        "serial_buffer_rx_size": 100000,
        "serial_buffer_tx_size": 1280,
        "turbine_id": "unknown"
      },
      "nodes": {
        "0": {
          "acc_freq": 100,
          "acc_range": 16,
          "analog_freq": 16384,
          "baros_bm": 1023,
          "baros_freq": 100,
          "blade_id": "0",
          "constat_period": 45,
          "battery_info_period": 3600,
          "decline_reason": {
            "0": "Bad block detection ongoing",
            "1": "Task already registered, cannot register again",
            "2": "Task is not registered, cannot de-register",
            "3": "Connection Parameter update unfinished"
          },
          "diff_baros_freq": 1000,
          "initial_node_handles": {
            "34": "Abs. baros",
            "36": "Diff. baros",
            "38": "Mic 0",
            "40": "Mic 1",
            "42": "IMU Accel",
            "44": "IMU Gyro",
            "46": "IMU Magnetometer",
            "48": "Analog1",
            "50": "Analog2",
            "52": "Constat",
            "54": "Cmd Decline",
            "56": "Sleep State",
            "58": "Info Message"
          },
          "gyro_freq": 100,
          "gyro_range": 2000,
          "remote_info_type": {
            "0": "Battery info"
          },
          "mag_freq": 12.5,
          "mics_freq": 15625,
          "mics_bm": 1023,
          "max_timestamp_slack": 0.005,
          "max_period_drift": 0.02,
          "node_firmware_version": "unknown",
          "number_of_sensors": {
            "Mics": 10,
            "Baros_P": 40,
            "Baros_T": 40,
            "Diff_Baros": 5,
            "Acc": 3,
            "Gyro": 3,
            "Mag": 3,
            "Analog Vbat": 1,
            "Constat": 4,
            "battery_info": 3
          },
          "periods": {
            "Mics": 6.4e-5,
            "Baros_P": 0.01,
            "Baros_T": 0.01,
            "Diff_Baros": 0.001,
            "Acc": 0.01,
            "Gyro": 0.01,
            "Mag": 0.08,
            "Analog Vbat": 6.103515625e-5,
            "Constat": 0.045,
            "battery_info": 3600
          },
          "samples_per_packet": {
            "Mics": 8,
            "Diff_Baros": 24,
            "Baros_P": 1,
            "Baros_T": 1,
            "Acc": 40,
            "Gyro": 40,
            "Mag": 40,
            "Analog Vbat": 60,
            "Constat": 24,
            "battery_info": 1
          },
          "sensor_conversion_constants": {
            "Mics": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            "Diff_Baros": [1, 1, 1, 1, 1],
            "Acc": [1, 1, 1],
            "Gyro": [1, 1, 1],
            "Mag": [1, 1, 1],
            "Analog Vbat": [1],
            "Constat": [1, 1, 1, 1],
            "battery_info": [1e6, 100, 256]
          },
          "sensor_coordinates": {
            "Mics": "mics_coordinate_reference",
            "Baros_P": "baros_coordinate_reference",
            "Baros_T": "baros_coordinate_reference",
            "Diff_Baros": "baros_coordinate_reference",
            "Acc": "accelerometers_coordinate_reference",
            "Gyro": "gyroscopes_coordinate_reference",
            "Mag": "magnetometers_coordinate_reference"
          },
          "sensor_names": [
            "Mics",
            "Baros_P",
            "Baros_T",
            "Diff_Baros",
            "Acc",
            "Gyro",
            "Mag",
            "Analog Vbat",
            "Constat",
            "battery_info"
          ],
          "sleep_state": {
            "0": "Exiting sleep",
            "1": "Entering sleep"
          }
        }
      },
      "measurement_campaign": {
        "label": "my-test-1",
        "description": null
      }
    }
