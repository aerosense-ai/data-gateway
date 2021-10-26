.. _installation:

============
Installation
============

.. _creating_a_personal_access_token:

Creating a Personal Access Token:
=================================

If setting up installation for many devices, you won't want to save your personal account credentials on the devices.

To avoid this, create a "deployment account" - a "robot user" on GitLab which has read-only access only to the
**data-gateway** repository.

Log in to GitLab as the robot user, and create a "Personal Access Token", with only ``read_repository`` scope. Ensure
you note in its description what it'll be used for.

This allows download of the software more securely than using your own personal credentials (which will
have varying levels of admin access, as well as read-write privileges). It's best to keep credentials like
this as tightly scoped as possible.

.. figure:: images/creating-a-personal-access-token.png
    :width: 600px
    :align: center
    :figclass: align-center
    :alt: Creating a personal access token in GitLab

    Creating a Personal Access Token in GitLab.


.. _installing_on_a_raspberry_pi:

Installing on a Raspberry Pi 4
==============================

Although **data-gateway** can be run on a wide range of hardware, it's generally aimed at being run on a Raspberry Pi
on board a turbine nacelle.

It's anticipated that you're using:
    - Raspberry Pi 4
    - With at least 2GB ram
    - Python >= 3.7

You'll need to install Raspberry Pi OS (formerly "Raspbian", which was a much better name) onto your **pi**. Use
`the current instructions from raspberrypi.org <https://www.raspberrypi.org/software/>`_, and follow their setup guides.

When booted into your **pi**, use the following commands to install...

.. code-block::

   export GATEWAY_VERSION="0.1.0" # Or whatever release number you aim to use, check the latest available on GitLab
   export PERSONAL_ACCESS_TOKEN=abcdef123456 # See above for how to generate the PAT
   pip install git+https://${PERSONAL_ACCESS_TOKEN}@gitlab.com/windenergie-hsr/aerosense/digital-twin/data-gateway@${GATEWAY_VERSION}

This installs the CLI :ref:`gateway_cli`, which enables you to start the gateway.

.. ATTENTION::
   For prototyping and early set-ups, you needn't bother creating a PAT. By omitting the
   ``${PERSONAL_ACCESS_TOKEN_GITLAB}`` string from the above ``git clone`` command, you'll be prompted for
   your own username and password, which will be used to fetch the code but won't be stored.


.. _installing_on_other_hardware:

Installing on Other Hardware
============================

There's no reason **data-gateway** can't be run on a wide range of hardware, although we've only tested it for the
Raspberry Pi 4, which has a quad-core processor.

The main consideration when choosing other hardware is that a dual-core CPU is probably a conservative choice:
**data-gateway** uses two threads. Additional vCPUs will always reduce the likelihood of the packet reader being blocked,
improving stability of the system. In reality these processes are both sufficiently lightweight that they'd **probably**
be just fine on a single core, but we haven't tested that, so please run extensive tests prior to field deployment if
you go down this route!


.. _installation_for_developers:

Installation for developers
===========================

If you're developing **data-gateway** you'll need to follow the instructions for developers in the
`repo's README.md file <https://gitlab.com/windenergie-hsr/aerosense/digital-twin/data-gateway/-/blob/main/README.md>`_.
