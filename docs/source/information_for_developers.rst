.. _information_for_developers:

==========================
Information for Developers
==========================

.. _editing_files:

Editing configuration and routine files
=======================================

Often, whilst testing or getting set up, you'll want to edit files on a device itself,
particularly a configuration or routine file that you're just trying out.

For this, the ``nano`` editor has been installed in the built containers so you can do:

.. code-block:: shell
    $ nano $GATEWAY_ROUTINE_FILE
    
or

.. code-block:: shell
    $ nano $GATEWAY_CONFIG_FILE
    
to edit either the routine or the configuration files.


.. _scp_of_files:

SCP of files to/from BalenaOS
=============================

For 99% of the time, using nano to edit (or paste from your own preferred IDE) will be fine.
Occasionally though, you'll want to get files on/off a device. In particular, for debugging, 
you might run the gateway with the ``-l`` and/or ``--save-local-logs`` options, then want to
directly inspect the data files that result.

However, the ``balena`` cli doesn't support ``scp`` that well out of the box (although there are workarounds using tunneling).

To copy files between ``/data`` directory (on a container deployed by Balena) and your own machine:

#. `Install the balena CLI <https://github.com/balena-io/balena-cli/blob/master/INSTALL.md>`_ and do ``balena login``

#. Add your public ssh key to BalenaCloud and make sure you can use ``balena ssh` correctly, `following these instructions <https://www.balena.io/docs/learn/manage/ssh-access/#using-balena-ssh-from-the-cli>`_

#. Check `this GitHub issue <https://github.com/balena-io/balena-cli/issues/885>`_. If closed with a new balena CLI feature, then follow those instructions instead. Otherwise use the following workaround.

#. Install `the ssh-uuid utility <https://github.com/pdcastro/ssh-uuid#file-transfer-with-scp>`_.

#. Get the full UUID of the device:

.. code-block:: shell
    $ balena devices
    ID      UUID    DEVICE NAME   DEVICE TYPE     FLEET              STATUS IS ONLINE SUPERVISOR VERSION OS VERSION       DASHBOARD URL
    7294376 4bfe19d fried-firefly raspberrypi4-64 aerosense/gateways Idle   true      13.1.11            balenaOS 2.98.33 https://dashboard.balena-cloud.com/devices/4bfe19d3651d27dc89d4b1a8c95061fa/summary

    $ balena device 4bfe19d
    == FRIED FIREFLY
    ID:                    7294376
    ...
    UUID:                  4bfe19d3651d27dc89d4b1a8c95061fa
    ...

#. Get the App ID (which is actually the Fleet ID), in this case ``1945598``:

.. code-block:: shell
    balena fleets
    ID      NAME                SLUG                          DEVICE TYPE         ONLINE DEVICES DEVICE COUNT
    1945598 gateways            aerosense/gateways            raspberrypi4-64     1              3

#. We'll be copying from balena's Host OS, not from the container. The ``/data`` directory `isn't mounted in the same place as when you're inside the container <https://github.com/balena-io/docs/blob/master/shared/general/persistent-storage.md>`_. So the root of the data folder is:

.. code-block:: shell
    /var/lib/docker/volumes/<APP ID>_resin-data/_data/

#. So for example, to copy a file from within the ``/data`` folder from remote to local, we do:

.. code-block:: shell
    scp-uuid 4bfe19d3651d27dc89d4b1a8c95061fa.balena:/var/lib/docker/volumes/1945598_resin-data/_data/gateway/20221122T100229/window-2.json .

#. The scp command should work recursively with folders, but take care because they can be large if a long session has taken place.

