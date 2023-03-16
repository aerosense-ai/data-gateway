.. _manual_installation:

===================
Manual Installation
===================

.. note::
    Once installed, you'll need to configure with a service account; follow the :ref:`setup steps <configuration>` using your own terminal and system environment variables.

.. warning::
    
   It's possible to manually install gateway code to any machine (eg your own laptop)
   but **by far the easiest way**, even for development purposes in the lab, is to use balena to deploy the
   code straight to a raspberry pi.
   

.. _installing_on_a_raspberry_pi:

Installing manually (on a Raspberry Pi 4)
=========================================

Although **data-gateway** can be run on a wide range of hardware, it's generally aimed at being run on a Raspberry Pi
on board a turbine (in the base station box on the tower).

It's anticipated that you're using:
    - Raspberry Pi 4
    - With at least 2GB ram
    - Python >= 3.8

You'll need to install Raspberry Pi OS (formerly "Raspbian", which was a much better name) onto your **pi**. Use
`the current instructions from raspberrypi.org <https://www.raspberrypi.org/software/>`_, and follow their setup guides.

When booted into your **pi**, use the following commands to install...

.. code-block:: shell
    
   sudo apt-get update
   sudo apt-get install libhdf5-dev libhdf5-serial-dev

   git clone https://github.com/aerosense-ai/data-gateway.git
   cd data-gateway
   pip install -r requirements-pi.txt

This installs the CLI :ref:`gateway_cli`, which enables you to start the gateway.


.. _installing_on_other_hardware:

Installing on Other Hardware
============================

There's no reason **data-gateway** can't be run on a wide range of hardware, or your own development laptop in the lab.

However, we've only tested it for the Raspberry Pi 4, which has a quad-core processor and is unix-based.

The main consideration when choosing other hardware is that a dual-core CPU is probably a conservative choice:
**data-gateway** uses three processes and multiple threads. Additional vCPUs will always reduce the likelihood of the
packet reader being blocked, improving stability of the system. In reality these processes are both sufficiently
lightweight that they'd **probably** be just fine on a single core, but we haven't tested that, so please run extensive
tests prior to field deployment if you go down this route!


.. _installation_for_developers:

Installation for developers
===========================

If you're developing **data-gateway** code itself you'll need to follow the instructions for developers in the
`repo's README.md file <https://github.com/aerosense-ai/data-gateway/blob/main/README.md>`_.
