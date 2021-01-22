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
    - With at least 2Gb ram

You'll need to install Raspberry Pi OS (formerly "Raspbian", which was a much better name) onto your **pi**. Use
`the current instructions from raspberrypi.org <https://www.raspberrypi.org/software/>`_, and follow their setup guides.

.. ATTENTION::
    You need to have python >= 3.7 since data-gateway uses the new async/await features in >=3.7.
    That means that you need the latest ``buster`` version, I recommend ``buster-lite``.

When booted into your **pi**, use the following commands to install...

.. code-block::

   export GATEWAY_VERSION="0.1.0" # Or whatever release number you aim to use, check the latest available on GitLab
   export PERSONAL_ACCESS_TOKEN_GITLAB=abcdef123456 # Generate a PAT in gitlab, noting that it's for this purpose (so
you can revoke it in future if necessary)
   git clone --depth 1 https://${PERSONAL_ACCESS_TOKEN_GITLAB}@gitlab.com/windenergie-hsr/aerosense/digital-twin/data-gateway.git@${GATEWAY_VERSION}
   pip install -e ./data-gateway

.. _installing_on_other_hardware:

Installing on Other Hardware
============================

There's no reason **data-gateway** can't be run on a wide range of hardware, although we've only tested it for the
Raspberry Pi 4, which has a quad-core processor.

The main consideration when choosing other hardware is that a dual-core CPU is probably a conservative choice:
**data-gateway** uses an async event loop to save/recover/POST data, and another thread to read and buffer the stream
of data from the serial port. Additional vCPUs will always reduce the likelihood of the event loop (or other system
processes) from blocking the reader, improving stability of the system.

In reality these processes are both sufficiently lightweight that they'd **probably** be just fine on a single core, but
we haven't tested that, so please run extensive tests prior to field deployment if you go down this route!


.. _installation_for_developers:

Installation for developers
===========================

If you're developing **data-gateway** you'll need to follow the instructions for developers in the
`repo's README.md file <https://gitlab.com/windenergie-hsr/aerosense/digital-twin/data-gateway/-/blob/main/README.md>`_.

