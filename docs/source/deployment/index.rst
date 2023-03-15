.. _deployment:

==========
Deployment
==========

To deploy the Aerosense system, you must:

- Install gateway code to the raspberry pi in the base station
- Install and connect the physical base station 
- Configure the gateway
- Register this installation to the aerosense database

Go through the following steps:

1. :ref:`Install using balena <using_balena>` (strongly recommended) or :ref:`manually <manual_installation>`.

2. :ref:`Configure <configuration>` the gateway for the first time.

3. :ref:`Register <registration>` your installation.

4. :ref:`Check <check>` the gateway.

Once complete, move on to :ref:`using the gateway <using_the_gateway>`.

.. toctree::
   :maxdepth: 1
   :hidden:

   using_balena
   manual_installation
   configuration
   registration
   check


.. _check:

Check
=====

Once the above steps are complete, in the balena (or your own, for a manual installation) terminal, check the installation by typing:

.. code-block:: shell
   
   gateway --help

.. figure:: /images/balena/gateway-help.png
    :width: 600px
    :align: center
    :figclass: align-center

    If the gateway is correctly installed, you should see this.


To get started, see :ref:`using the gateway <using_the_gateway>`. 


