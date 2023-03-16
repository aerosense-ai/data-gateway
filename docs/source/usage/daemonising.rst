.. _daemonising:

===========
Daemonising
===========

.. warning::

   Daemonisation cannot happen reliably until :ref:<https://github.com/aerosense-ai/data-gateway/issues/119>`_ is solved.

During the aerosense project, Balena has made it so convenient
to shell in and manage sessions that it's the only thing we've actually done.

However, this sometimes means babysitting the gateway and takes up time - that's fine
in the very early days, but if you are setting up a longer-term deployment of aerosense
test rig) you should *daemonise* the ``gateway start`` command.

This basically means set the system up to:

 - start the gateway along with the rest of the OS on boot
 - restart the gateway program if it crashes

There are lots of ways of doing this but we **strongly** recommend using `supervisord <http://supervisord.org/>`_,
which, as the name suggests, is a supervisor for daemonised processes.

Install supervisord on your system:

.. code-block:: shell

   # Ensure you've got the latest version of supervisord installed
   sudo apt-get install --update supervisord

Configure supervisord to  (`more info here <http://supervisord.org/installing.html#creating-a-configuration-file>`_) run
the gateway as a daemonised service:

.. warning::

   We've not actualy done this (see the warning above) but it should look very similar to this:

.. code-block:: shell

   sudo gateway supervisord-conf >> /etc/supervisord.conf
   
Restarting your system, at this point, should start the gateway process at boot time.

You can use `supervisorctl <http://supervisord.org/running.html#running-supervisorctl>`_ to check gateway status:

.. code-block:: shell

   supervisorctl status AerosenseGateway

Similarly, you can stop and start the daemon with:

.. code-block:: shell

   supervisorctl stop AerosenseGateway
   supervisorctl start AerosenseGateway
