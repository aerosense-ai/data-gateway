.. _using_the_gateway:

=================
Using the Gateway
=================

.. _configuring:

Configuring the Gateway
=======================

Configuration options for the gateway can be supplied via a configuration file. By default, **data-gateway** looks for
a file named ``config.json`` in the working directory, although the CLI allows this to be overriden, to use a specific
configuration file.

.. _gateway_cli:

Gateway CLI
===========

The gateway has a CLI which means you can call it just like any other unix command.

It is called simply ``gateway`` and, once :ref:`installed <installation>`, you can see the options and
help docs by typing:

.. code-block::

   gateway --help

The main command to start the gateway running is:

.. code-block::

   gateway start


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
