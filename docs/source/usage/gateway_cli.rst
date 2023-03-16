.. _gateway_cli:

===========
Gateway CLI
===========

The gateway has a CLI which means you can call it just like any other unix command.

It is called simply ``gateway``. Once the code is :ref:`deployed/installed <deployment>`, you can see the options and
help by typing:

.. code-block:: shell

   gateway --help

Or see more detailed help on a subcommand (eg ``start``) with:

.. code-block:: shell

   gateway start --help


Start
=====

The ``start`` subcommand is overwhelmingly the most common you'll use.

Once started, data is read continuously from the serial port, parsed, 
processed, batched into time windows, and either:

- :ref:`uploaded <uploads>` to an ingress Google Cloud storage bucket where it is cleaned and
   forwarded to another bucket for storage, or

- saved locally as JSON files, or

- both.

The start command also allows you to send commands to the base station (which will
broadcast them to the nodes). The sequence of commands you send is called a "routine" and the
commands can be sent automatically (for long term acquisition) or interactivel (for debug/test).

Automatic mode
--------------

Running the gateway in automatic mode doesn't allow further commands to be passed
to the serial port. Instead, a :ref:`routine file <routine_files>` must be specified,
and the commands in it are issues automatically on your behalf, looping indefinitely.

Assuming you have your configuration and routine files set up per :ref:`the instructions here <configuration>`,
to start this mode, type:

.. code-block:: shell

    gateway start

You can stop the gateway by pressing ``Ctrl + C``.

Interactive mode
----------------

Running the gateway in interactive mode allows commands to be sent to the serial port while the gateway is
running. A routine file can't be provided if using this mode. Any commands entered interactively are logged to a
``commands.txt`` file in the output directory.

To start this mode, type:

.. code-block:: shell

    gateway start --interactive

Typing ``stop`` or pressing ``Ctrl + C`` will stop the session.


Other options
-------------

* The window size (default 600 seconds) can be set by using ``--window-size=<number_of_seconds>`` after the `start` command
* You can store data locally instead of or at the same time as it is sent to the cloud by using the ``--save-locally`` option
* To avoid sending any data to the cloud, provide the ``--no-upload-to-cloud`` option


