.. ATTENTION::
    This library is in experimental stages! Please pin deployments to a specific release, and consider every release as breaking.

============
Data Gateway
============

.. epigraph::
   *"Data Gateway" ~ reads data from an aerosense receiver and shoves it into the cloud.*


Data Flow
=========

The data flow from the aerosense sensor modules looks like this:

.. code-block::

   Node (edge processor on-blade)
     >  Receiver (bluetooth equipment in-nacelle)
       >  Gateway (data manager and uploader on-nacelle)
         >  Ingress (server to receive data on-cloud)
           >  Digital Twin (data analysis and storage system)

A ``Node`` streams data to the ``Receiver`` via bluetooth. The ``Receiver`` writes the bytestream directly to a serial
port. The ``Gateway`` (this library) reads the bytestream from the serial port, decodes it and buffers it in local
storage. The ``Gateway`` then is responsible for:

   - establishing a connection (websocket) to ``Ingress`` and writing the buffered data, or
   - packaging the data into events and files which are ``POST``ed to ``Ingress``.

The ``Gateway`` is also responsible for managing the buffer and local store to minimise data loss in the event of internet
outages.


.. toctree::
   :maxdepth: 2

   self
   installation
   using_the_gateway
   output_data
   deployment
   cloud_functions
   cloud_preprocessing
   hardware_and_firmware_versions
   api
   version_history
