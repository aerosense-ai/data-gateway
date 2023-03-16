.. _registration:

Registration
============

When installing the physical Aerosense system onto one or more turbines (typically the turbines in a
particular wind farm), you need to register the installation in the aerosense database.

For experimental and test purposes, there will generally be only one turbine (and therefore one gateway) per deployment.

Either way, once you've :ref:`installed <installation_with_balena>` and :ref:`configured <configuration>` the gateway,
you need to register the installation.

.. tip::
   
   Make sure you've chosen a sensible value for the ``installation_reference`` value in your
   configuration file. This should be unique to your installation (an error will occur in registration 
   if it's been used before), and this will be what you use to refer to later when you want to filter results
   in ``dashboard`` and ``aerosense-tools``.

To register, do:

.. code-block:: shell

   gateway create-installation --config-file $GATEWAY_CONFIG_FILE

And follow the instructions. After this, the gateway should be ready for use.




