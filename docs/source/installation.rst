.. _installation:

============
Installation
============

.. _installing_on_a_raspberry_pi:

Installing on a Raspberry Pi 4
==============================

Although **data-gateway** can be run on a wide range of hardware, it's generally aimed at being run on a Raspberry Pi
on board a turbine nacelle.

It's anticipated that you're using:
    - Raspberry Pi 4
    - With at least 2GB ram
    - Python >= 3.7.1

You'll need to install Raspberry Pi OS (formerly "Raspbian", which was a much better name) onto your **pi**. Use
`the current instructions from raspberrypi.org <https://www.raspberrypi.org/software/>`_, and follow their setup guides.

When booted into your **pi**, use the following commands to install...

.. code-block:: shell

   export GATEWAY_VERSION="0.11.8" # Or whatever release number you aim to use, check the latest available on GitHub
   pip install git+https://github.com/aerosense-ai/data-gateway.git@${GATEWAY_VERSION}

This installs the CLI :ref:`gateway_cli`, which enables you to start the gateway.


.. _installing_on_other_hardware:

Installing on Other Hardware
============================

There's no reason **data-gateway** can't be run on a wide range of hardware, although we've only tested it for the
Raspberry Pi 4, which has a quad-core processor.

The main consideration when choosing other hardware is that a dual-core CPU is probably a conservative choice:
**data-gateway** uses three processes and multiple threads. Additional vCPUs will always reduce the likelihood of the
packet reader being blocked, improving stability of the system. In reality these processes are both sufficiently
lightweight that they'd **probably** be just fine on a single core, but we haven't tested that, so please run extensive
tests prior to field deployment if you go down this route!


.. _installation_for_developers:

Installation for developers
===========================

If you're developing **data-gateway** you'll need to follow the instructions for developers in the
`repo's README.md file <https://github.com/aerosense-ai/data-gateway/blob/main/README.md>`_.
