.. _using_the_gateway:

=================
Using the Gateway
=================

.. _gateway_cli:

Gateway CLI
===========

The gateway has a CLI which means you can call it just like any other unix command.

It is called simply ``gateway`` and, once :ref:`installed <installation>`, you can see the options and
help by typing:

.. code-block:: shell

   gateway --help

The main command to start the gateway running is:

.. code-block:: shell

   gateway start

You can see help about this command by executing:

.. code-block:: shell

   gateway start --help


Automatic mode
--------------------------------
Running the gateway in automatic mode doesn't allow further commands to be passed to the serial port. Instead, a
:doc:`routine JSON file <routines>` can be provided via the ``--routine-file`` option. Data from the serial port is
processed, batched into time windows, and uploaded to an ingress Google Cloud storage bucket where it is cleaned and
forwarded to another bucket for storage. This is the mode you'll want to deploy in production.

Before starting this mode, Google application credentials must be provided. To start this mode, type:

.. code-block:: shell

    export GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service/account/file.json

    gateway start --gcp-bucket-name=my-bucket --output-dir=path/to/output-directory-in-cloud-bucket

If the connection to Google Cloud fails, windows will be written to the hidden directory
``./<output_directory>/.backup`` where they will stay until the connection resumes. Backup files are deleted upon
successful cloud upload.

You can stop the gateway by pressing ``Ctrl + C``.


Interactive mode
----------------
Running the gateway in interactive mode allows commands to be sent to the serial port while the gateway is
running. A routine file can't be provided if using this mode. Any commands entered interactively are logged to a
``commands.txt`` file in the output directory.

To start this mode, type:

.. code-block:: shell

    export GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service/account/file.json

    gateway start --interactive --output-dir=<path/to/output-directory>

Typing ``stop`` or pressing ``Ctrl + C`` will stop the session.


Other options
-------------
* The window size (default 600 seconds) can be set by using ``--window-size=<number_of_seconds>`` after the `start` command
* You can store data locally instead of or at the same time as it is sent to the cloud by using the ``--save-locally`` option
* To avoid sending any data to the cloud, provide the ``--no-upload-to-cloud`` option


.. _configuring:


Configuring the Gateway
=======================

Configuration options for the gateway can be supplied via a configuration file. By default, **data-gateway** looks for
a file named ``config.json`` in the working directory, although the CLI allows this to be overridden to use a specific
configuration file. Here is the contents of an example configuration file:

.. code-block:: json

    {
      "mics_freq": 15625,
      "mics_bm": 1023,
      "baros_freq": 100,
      "diff_baros_freq": 1000,
      "baros_bm": 1023,
      "acc_freq": 100,
      "acc_range": 16,
      "gyro_freq": 100,
      "gyro_range": 2000,
      "mag_freq": 12.5,
      "analog_freq": 16384,
      "constat_period": 45,
      "serial_buffer_rx_size": 100000,
      "serial_buffer_tx_size": 1280,
      "baudrate": 2300000,
      "endian": "little",
      "max_timestamp_slack": 0.005,
      "max_period_drift": 0.02,
      "packet_key": 254,
      "type_handle_def": 255,
      "mics_samples_per_packet": 8,
      "imu_samples_per_packet": 40,
      "analog_samples_per_packet": 60,
      "baros_samples_per_packet": 1,
      "diff_baros_samples_per_packet": 24,
      "constat_samples_per_packet": 24,
      "sensor_names": [
        "Mics",
        "Baros_P",
        "Baros_T",
        "Diff_Baros",
        "Acc",
        "Gyro",
        "Mag",
        "Analog Vbat",
        "Constat"
      ],
      "default_handles": {
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
      "decline_reason": {
        "0": "Bad block detection ongoing",
        "1": "Task already registered, cannot register again",
        "2": "Task is not registered, cannot de-register",
        "3": "Connection Parameter update unfinished"
      },
      "sleep_state": {
        "0": "Exiting sleep",
        "1": "Entering sleep"
      },
      "info_type": {
        "0": "Battery info"
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
        "Constat": 24
      },
      "number_of_sensors": {
        "Mics": 10,
        "Baros_P": 40,
        "Baros_T": 40,
        "Diff_Baros": 5,
        "Acc": 3,
        "Gyro": 3,
        "Mag": 3,
        "Analog Vbat": 1,
        "Constat": 4
      },
      "sensor_conversion_constants":{
        "Mics": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        "Diff_Baros": [1, 1, 1, 1, 1],
        "Baros_P": [40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96, 40.96],
        "Baros_T": [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100],
        "Acc": [1, 1, 1],
        "Gyro": [1, 1, 1],
        "Mag": [1, 1, 1],
        "Analog Vbat": [1],
        "Constat": [1, 1, 1, 1]
      },
      "period": {
        "Mics": 6.4e-05,
        "Baros_P": 0.01,
        "Baros_T": 0.01,
        "Diff_Baros": 0.001,
        "Acc": 0.01,
        "Gyro": 0.01,
        "Mag": 0.08,
        "Analog Vbat": 6.103515625e-05,
        "Constat": 0.045
      },
      "sensor_commands": {
        "start": ["startBaros", "startDiffBaros", "startIMU", "startMics"],
        "stop": ["stopBaros", "stopDiffBaros", "stopIMU", "stopMics"],
        "configuration": ["configBaros", "configAccel", "configGyro", "configMics"],
        "utilities": [
          "getBattery",
          "setConnInterval",
          "tpcBoostIncrease",
          "tpcBoostDecrease",
          "tpcBoostHeapMemThr1",
          "tpcBoostHeapMemThr2",
          "tpcBoostHeapMemThr4"
        ]
      },
      "installation_data": {
        "installation_reference": "aventa_turbine",
        "turbine_id": "0",
        "blade_id": "0",
        "hardware_version": "1.2.3",
        "sensor_coordinates": {
          "Mics": [[0, 0, 0], [0, 0, 1]],
          "Baros_p": [[1, 0, 0], [1, 0, 1], [1, 2, 0]]
        }
      },
      "session_data": {
        "label": "my-test-1"
      }
    }

A default configuration is used if a ``config.json`` file is not specified and one is not found in the working
directory. If a configuration file is specified, all of the fields seen above must be present for it to be valid. Any
extra metadata you'd like to include can be specified in the ``session_data`` field as a JSON object.

One configuration is used per run of the ``start`` command. A copy is saved with the output data if saving data
locally. The configuration is saved as metadata on the output files uploaded to the cloud. To supply the configuration
file and start the gateway, type the following, supplying any other options you need:

.. code-block:: shell

    gateway start --config-file=</path/to/config.json>


.. _daemonising_the_installation:

Daemonising the installation
============================

If you are setting up a deployment of aerosense (on a turbine nacelle, rather than on prototype equipment or a
test rig) you should *daemonise* the gateway.

This basically means set the system up to:

 - start the gateway along with the rest of the OS on boot
 - restart the gateway program if it crashes

There are lots of ways of doing this but we **strongly** recommend using `supervisord <http://supervisord.org/>`_,
which, as the name suggests, is a supervisor for daemonised processes.

Install supervisord on your system:

.. code-block:: shell

   # Ensure you've got the latest version of supervisord installed
   sudo apt-get install --update supervisord

Configure supervisord to  (`more info here <http://supervisord.org/installing.html#creating-a-configuration-file>`_) run
the gateway as a daemonised service:

.. code-block:: shell

   sudo gateway supervisord-conf >> /etc/supervisord.conf
   # Or, if you want to set up the daemon with a specific configuration file
   sudo gateway supervisord-conf --config-file = /path/to/my/config.json >> /etc/supervisord.conf

Restarting your system, at this point, should start the gateway process at boot time.

You can use `supervisorctl <http://supervisord.org/running.html#running-supervisorctl>`_ to check gateway status:

.. code-block:: shell

   supervisorctl status AerosenseGateway

Similarly, you can stop and start the daemon with:

.. code-block:: shell

   supervisorctl stop AerosenseGateway
   supervisorctl start AerosenseGateway
