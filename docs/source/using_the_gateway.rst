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

.. code-block::

   gateway --help

The main command to start the gateway running is:

.. code-block::

   gateway start

You can see help about this command by executing:

.. code-block::

   gateway start --help


Automatic (non-interactive) mode
--------------------------------
Running the gateway in automatic (non-interactive) mode doesn't allow further commands to be passed to the serial port.
Data from the serial port is processed, batched, and uploaded to an ingress Google Cloud storage bucket where it is
cleaned and forwarded to another bucket for storage. This is the mode you'll want to deploy in production.

Before starting this mode, this environment variable must be defined to allow a cloud connection:
``GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service/account/file.json``

To start this mode, type:

.. code-block::

    gateway start \
        --gcp-project-name=<name-of-google-cloud-project> \
        --gcp-bucket-name=<name-of-google-cloud-bucket> \
        --output-dir=<path/to/output-directory-in-cloud-bucket> \

If the connection to Google Cloud fails, batches will be written to the hidden directory
``./<output_directory>/.backup`` where they will stay until the connection resumes. Backup files will be deleted upon
successful cloud upload.


Interactive (manual) mode
-------------------------
Running the gateway in interactive (manual) mode allows commands to be sent to the serial port while the gateway is
running. Typing ``stop`` will stop the session. Commands are logged to a ``commands.txt`` file in the output directory.
Data from the serial port is not uploaded to Google Cloud but instead written to the given output directory
(``./data_gateway`` by default) in the same format as in automatic mode.

To start this mode, type:

.. code-block::

    gateway start --interactive --output-dir=<path/to/output-directory>


Other options
-------------
* The batch interval (default 600 seconds) can be set by using ``--batch-interval=<number_of_seconds>`` after the `start` command


.. _configuring:

Configuring the Gateway
=======================

Configuration options for the gateway can be supplied via a configuration file. By default, **data-gateway** looks for
a file named ``config.json`` in the working directory, although the CLI allows this to be overridden to use a specific
configuration file. Here is the contents of an example configuration file:

.. code-block::

    {
        "acc_freq": 100,
        "acc_range": 16,
        "analog_freq": 16384,
        "analog_samples_per_packet": 60,
        "baros_bm": 1023,
        "baros_freq": 100,
        "baros_group_size": 4,
        "baros_packet_size": 60,
        "baros_samples_per_packet": 15,
        "baudrate": 2300000,
        "default_handles": {
            "34": "Baro group 0",
            "36": "Baro group 1",
            "38": "Baro group 2",
            "40": "Baro group 3",
            "42": "Baro group 4",
            "44": "Baro group 5",
            "46": "Baro group 6",
            "48": "Baro group 7",
            "50": "Baro group 8",
            "52": "Baro group 9",
            "54": "Mic 0",
            "56": "Mic 1",
            "58": "Mic 2",
            "60": "Mic 3",
            "62": "Mic 4",
            "64": "Mic 5",
            "66": "Mic 6",
            "68": "Mic 7",
            "70": "Mic 8",
            "72": "Mic 9",
            "74": "IMU Accel",
            "76": "IMU Gyro",
            "78": "IMU Magnetometer",
            "80": "Analog Kinetron",
            "82": "Analog Vbat"
        },
        "endian": "little",
        "gyro_freq": 100,
        "gyro_range": 2000,
        "imu_samples_per_packet": 40,
        "max_period_drift": 0.02,
        "max_timestamp_slack": 0.005,
        "mics_bm": 1023,
        "mics_freq": 5000,
        "mics_samples_per_packet": 120,
        "packet_key": 254,
        "serial_buffer_rx_size": 100000,
        "serial_buffer_tx_size": 1280,
        "serial_port": "COM9",
        "type_handle_def": 255,
        "n_meas_qty": {"Mics": 10, "Baros": 40, "Acc": 3, "Gyro": 3, "Mag": 3, "Analog Vbat": 2},
        "period": {"Mics": 0.0002, "Baros": 0.01, "Acc": 0.01, "Gyro": 0.01, "Mag": 0.08, "Analog Vbat": 6.103515625e-05},
        "samples_per_packet": {"Mics": 120, "Baros": 15, "Acc": 40, "Gyro": 40, "Mag": 40, "Analog Vbat": 60},
        "user_data": {}
    }

A default configuration (see ``data_gateway.reader.configuration`` is used if a ``config.json`` file is not specified
and one is not found in the working directory. If a configuration file is specified, all of the fields seen above must
be present for it to be valid. Any extra metadata you'd like to include can be specified in the ``user_data`` field as
a JSON object. See the :ref:`Configuration API <configuration_api>` for more information.

One configuration is used per run of the ``start`` command and is a copy is saved with the output data. The
configuration is also saved as metadata on the output files uploaded to the cloud. To supply the
configuration file and start the gateway, type the following, supplying any other options you need:

.. code-block::

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

.. code-block::

   # Ensure you've got the latest version of supervisord installed
   sudo apt-get install --update supervisord

Configure supervisord to  (`more info here <http://supervisord.org/installing.html#creating-a-configuration-file>`_) run
the gateway as a daemonised service:

.. code-block::

   sudo gateway supervisord-conf >> /etc/supervisord.conf
   # Or, if you want to set up the daemon with a specific configuration file
   sudo gateway supervisord-conf --config-file = /path/to/my/config.json >> /etc/supervisord.conf

Restarting your system, at this point, should start the gateway process at boot time.

You can use `supervisorctl <http://supervisord.org/running.html#running-supervisorctl>`_ to check gateway status:

.. code-block::

   supervisorctl status AerosenseGateway

Similarly, you can stop and start the daemon with:

.. code-block::

   supervisorctl stop AerosenseGateway
   supervisorctl start AerosenseGateway
