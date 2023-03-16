============
Data Gateway
============

.. epigraph::
   *"Data Gateway" ~ reads data from an Aerosense receiver and shoves it into the cloud.*

Get Started Quick
=================

- :ref:`Deploy a gateway <deployment>`. 

- :ref:`Run an already-deployed gateway <usage>`. 

Data Flow
=========

The data flow from the aerosense sensor modules looks like this:

.. code-block::

    Node (edge processor on-blade)
    ->  Base Station (bluetooth equipment on-tower)
    --->  Gateway (data manager and uploader on-tower)
    ----->  Ingress (Cloud Function to receive data on-cloud)
    ------->  Google Cloud BigQuery + Google Cloud Store (database / object storage system)
        |---->  Digital Twin (data analysis system)
        |---->  Jupyter Notebooks (data analysis/introspection for researchers)
        |---->  Dashboard (data visualisation for researchers and system installers)
    

A ``Node`` streams data to the ``Base Station`` via bluetooth. The ``Base Station`` writes the bytestream directly to a serial
port. The ``Gateway`` (this library) reads the bytestream from the serial port, decodes it and buffers it in local
storage. The ``Gateway`` then is responsible for:

- establishing a connection (websocket) to ``Ingress`` and writing the buffered data, or
- packaging the data into events and files which are posted to ``Ingress``.

The ``Gateway`` is also responsible for managing the buffer and local storage to minimise data loss in the event of
internet outages.

The code for the Cloud Function ``Ingress`` is also included in this repository.

.. toctree::
   :caption: Table of Contents
   :maxdepth: 2

   deployment/index
   usage/index
   cloud/index
   hardware_and_firmware_versions
   version_history
