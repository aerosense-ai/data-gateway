.. _cloud_function:

==============
Cloud function
==============
We've written a Google Cloud Function (a serverless deployed app) that, when a window is uploaded to the storage ingress
bucket, pre-processes/cleans it before moving it to a more permanent home in a different bucket. The ingress bucket is
currently set to ``aerosense-ingress-eu`` and the output bucket is set to ``data-gateway-processed-data``. Both are
part of the ``aerosense-twined`` Google Cloud project. You can view the deployed Cloud Function
`here <https://console.cloud.google.com/functions/details/europe-west6/ingress-eu>`_ - it's called ``ingress-eu``.

There is no need to read further about this if you are only working on data collection from the serial port.


=============================
Developing the cloud function
=============================
The entrypoint for the cloud function is ``cloud_functions.main.clean_and_upload_window`` and it must accept ``event`` and
``context`` arguments in that order. Apart from that, it can do anything upon receiving an event (the event is an upload
of a file to the ingress bucket). It currently uses the ``file_handler`` module and ``preprocessing`` subpackage.

Dependencies
============
Dependencies for the cloud function must be included in the ``requirements.txt`` file in the ``cloud_functions`` package.


More information
================
More information can be found at https://cloud.google.com/functions/docs/writing


Manual redeployment
===================
The cloud function package is included in this (``data-gateway``) repository in ``cloud_functions``, which is where it
should be edited and version controlled. When a new version is ready, it must be manually deployed to the cloud for it
to be used for new window uploads (there is no automatic deployment enabled currently):

.. code-block::

    cd cloud_functions

    gcloud functions deploy ingress-eu \
        --runtime python38 \
        --trigger-resource <name_of_ingress_bucket> \
        --trigger-event google.storage.object.finalize \
        --memory 1GB \
        --region <name_of_region> \
        --set-env-vars SOURCE_PROJECT_NAME=<source_project_name>,DESTINATION_PROJECT_NAME=<destination_project_name>,DESTINATION_BUCKET_NAME=<destination_bucket_name>
