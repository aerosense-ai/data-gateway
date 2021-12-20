.. _sensor_command_routines:

=======================
Sensor command routines
=======================

Commands can be sent to the sensors in two ways:

- The user manually typing them in in interactive mode
- Creating a routine JSON file and providing it to the CLI


Creating and providing a routine file
-------------------------------------

A routine file looks like this:

.. code-block::

    {
        "commands": [["startIMU", 0.1], ["startBaros", 0.2], ["getBattery", 3]]
        "period": 5  # (period is optional)
    }

and can be provided to the CLI's ``start`` command by using:

.. code-block::

    --routine-file=<path/to/routine_file.json>

If this option isn't provided, the CLI looks for a file called ``routine.json`` in the current working directory. If this file doesn't 
exist and the ``--routine-file`` option isn't provided, the command assumes there is no routine file to run.

.. warning::
    If a routine file is provided in interactive mode, the routine is ignored. Only the commands entered interactively are sent to the
    sensors.


Routine file schema
-------------------

- The ``commands`` key in the file should be a list of two-element lists. Each two-element list should comprise a valid string command to 
send to the sensors and a delay in seconds from the gateway starting to run the command.
- An optional ``period`` in seconds can be provided to repeat the routine. If none is provided, the routine is run once only. 
  The period must be greater than each of the commands' delays.
