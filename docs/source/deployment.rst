.. _deployment:

==========
Deployment
==========

To deploy the AeroSense system, you must:

- Register the deployment
- Create a deployment Service Account
- Configure and run the gateway (at the on-nacelle processor)


.. _register_the_deployment:

Register the deployment
=======================

A "deployment" is the act of commissioning the AeroSense system on a group of turbines (typically the turbines in a
particular wind farm).

For experimental and test purposes, there will typically be only one turbine (and therefore one gateway) per deployment.

TODO Registration process documentation - See Tom and Yuriy


.. _create_a_service_account:

Create a Service Account
========================

The gateway that you install on a turbine needs to upload data to AeroSense Cloud. However, we don't want "just anybody"
to be able to write data to the store - that leaves us vulnerable to a wide range of attacks. So the gateway must
authenticate itself with the store prior to upload.

To enable the gateway to authenticate itself, we use a **service account**, which is a bit like a user account (it has
an email address and can be given certain permissions) but for a non-human.

Here, we will create a service account for a deployment - this will result in a single credentials file that we can
reuse across the gateways (turbines) in the deployment to save administrative overhead maintaining all the credentials.

Log in to the ``aerosense-twined`` project on Google Cloud Platform (GCP) and work through the following steps:


**1. Go to IAM > Service Accounts > Create**


.. figure:: images/creating-a-service-account/1-go-to-iam-service-accounts.png
    :width: 600px
    :align: center
    :figclass: align-center

    Go to the service accounts view, and click "Create Service Account"


**2. Create the service account**

.. figure:: images/creating-a-service-account/2-create-service-account.png
    :width: 600px
    :align: center
    :figclass: align-center

    The service account name should contain your deployment id (from above) in the pattern
    ``as-deployment-<deploymentId>``. In this case, ``deploymentId = gatewaytest``

**3. Skip assignation of optional roles and users (for now)**

.. figure:: images/creating-a-service-account/3-no-grants-or-users.png
    :width: 600px
    :align: center
    :figclass: align-center

    Do not assign roles or users for now. We'll assign the permissions for the specific resource(s) in step 6.

**4. Create and download a private JSON key for this Service Account**

.. figure:: images/creating-a-service-account/4a-create-key.png
    :width: 600px
    :align: center
    :figclass: align-center

    Find your newly created service account in the list (you may have to search) and click 'Create Key'.

.. figure:: images/creating-a-service-account/4b-key-should-be-json.png
    :width: 600px
    :align: center
    :figclass: align-center

    Choose the default JSON key type.

.. figure:: images/creating-a-service-account/4c-key-will-be-saved.png
    :width: 600px
    :align: center
    :figclass: align-center

    Google will create a key file and it will be downloaded to your desktop.

**5. Locate the ingress bucket in the storage browser, and click on "Add Member"**

.. figure:: images/creating-a-service-account/5-locate-aerosense-ingress-bucket.png
    :width: 600px
    :align: center
    :figclass: align-center

    From the left hand navigation menu, change to the Storage Browser view and locate the ``aerosense-ingress-eu``
    bucket. Select it, and click "Add Member" in the right hand control pane.

**6. Assign ``Storage Object Creator`` permission**

.. figure:: images/creating-a-service-account/5-locate-aerosense-ingress-bucket.png
    :width: 600px
    :align: center
    :figclass: align-center

    We wish to add the service account created above to this bucket's permissions member list. Use the email address
    that was generated in step 2 to find your new service account and add it. We want the service
    account to have *minimal permissions* which in this case means assigning the role of ``Storage Object Creator``.

And you're done! Keep that downloaded permission file for later.

.. ATTENTION::

   Do not add to a docker image, email, skype/meet/zoom, dropbox, whatsapp, commit to git, post in an issue, or
   whatever, this private credentials file.

   Doing so will earn you the penance of flushing and rotating all the system credentials.


.. _configure_and_run_the_gateway:

Configure and run the Gateway
=============================
The gateway can be configured using a `configuration.json` file. Here is an example:

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
            "80": "Analog"
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
        "n_meas_qty": {"Mics": 10, "Baros": 40, "Acc": 3, "Gyro": 3, "Mag": 3, "Analog": 2},
        "period": {"Mics": 0.0002, "Baros": 0.01, "Acc": 0.01, "Gyro": 0.01, "Mag": 0.08, "Analog": 6.103515625e-05},
        "samples_per_packet": {"Mics": 120, "Baros": 15, "Acc": 40, "Gyro": 40, "Mag": 40, "Analog": 60},
        "user_data": {}
    }

A default configuration (see `data_gateway.reader.configuration` is used if a `config.json` file is not specified. If a
configuration file is specified, all of the fields seen above must be present for it to be valid. Any extra metadata
you'd like to include can be specified in the `user_data` field as a JSON object. If there is a `config.json` file
present in the directory where `gateway` is run, it will be used.

One configuration is used per run of the `start` command. To supply the configuration file and start the gateway, type
the following, supplying any other options you need:

.. code-block::

    gateway start --config-file=</path/to/config.json>
