.. _using_balena:

============
Using Balena
============

A fleet of devices (under the ``aerosense`` organisation on BalenaCloud) is managed by Balena,
and this is by far the preferred way of doing things.

Balena manages device installation, health and continuous deployment of code
(whenever the main repository is updated, a fleet-wide update is triggered).
It also works as a portal to the device, allowing you to log in and view device status as well
as opening a terminal shell to individual devices for test and diagnostic purposes.

You can :ref:`join the balena organisation with your github account <balena_organization_admins>`.

.. _installation_with_balena:

Installation with balena
========================

With a fresh new raspberry pi (or an old one with a wiped SSD card!) you'll want to install
the gateway code. You do this by Adding a Device.

.. tip::
    Technically, the "device" is tied to the SSD card, not the raspberry pi itself.
    SSDs are notorious for failing after a short cycle time, so it's well worth buying high quality SanDisk
    SSD cards rather than the cheap equivalents, or you may find yourself dealing with a failure in the field. 


Add a device
------------

Follow the balenaCloud instructions to install balena on the SSD card and add it to the ``gateways`` fleet (in the ``aerosense`` organization):

.. figure:: /images/balena/adding-a-device.png
    :width: 600px
    :align: center
    :figclass: align-center

    Hit 'add a device' and balena will take you through the process of installing and deploying.

Once added, the device will appear in your balena dashboard with a coolname, like ``fried-firefly`` or
``holy-lake``.

Labelmaking is the way
----------------------

Steal the labelmaker from PBL, and label your raspberry pi so you know which one is which.

Once added, you can follow the instructions to :ref:`configure <configuration>` your device.


.. _balena_organization_admins:

Balena Organization Admins
==========================

Use your github account to register with balena. Existing admins for the aerosense organisation
can log into BalenaCloud then invite you.

.. figure:: /images/balena/current-admins.png
    :width: 600px
    :align: center
    :figclass: align-center

    The current list of admins in balena.
