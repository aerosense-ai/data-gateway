.. ATTENTION::
    This library is in experimental stages! Please pin deployments to a specific release, and consider every release as breaking.

============
Data Gateway
============

.. epigraph::
   *"Data Gateway" ~ reads data from an Aerosense receiver and shoves it into the cloud.*


Data Flow
=========

The data flow from the aerosense sensor modules looks like this:

.. code-block::

    Node (edge processor on-blade)
      >  Receiver (bluetooth equipment in-nacelle)
        >  Gateway (data manager and uploader on-nacelle)
          >  Ingress (Cloud Function to receive data on-cloud)
            >  Digital Twin (data analysis and storage system)

A ``Node`` streams data to the ``Receiver`` via bluetooth. The ``Receiver`` writes the bytestream directly to a serial
port. The ``Gateway`` (this library) reads the bytestream from the serial port, decodes it and buffers it in local
storage. The ``Gateway`` then is responsible for:

- establishing a connection (websocket) to ``Ingress`` and writing the buffered data, or
- packaging the data into events and files which are posted to ``Ingress``.

The ``Gateway`` is also responsible for managing the buffer and local storage to minimise data loss in the event of
internet outages.

The code for the Cloud Function ``Ingress`` is also included in this repository.


.. toctree::
   :maxdepth: 2
   :hidden:

   self
   installation
   using_the_gateway
   output_data
   routines
   deployment
   cloud_functions
   hardware_and_firmware_versions
   version_history
