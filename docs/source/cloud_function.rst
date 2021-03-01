.. _cloud_function:

==============
Cloud function
==============

When a batch is uploaded to the Google Cloud storage ingress bucket, a cloud function (a serverless deployed app) runs
on it to clean it and move it to a more permanent home in a different bucket.

The function used as the cloud function is ``cloud_function.main.clean_and_upload_batch`` and it must accept ``event``
and ``context`` arguments in that order.


Dependencies
============

Dependencies for the cloud function must be detailed in the ``requirements.txt`` file in the ``cloud_function`` package.


More information
================

More information can be found at https://cloud.google.com/functions/docs/writing


Developer notes
===============

The cloud function package is included in this (``data-gateway``) repository in ``cloud_function``, which is where it
should be edited and version controlled. When a new version is ready, it can be deployed to the cloud as follows:

.. code-block::

    gcloud functions deploy clean_and_upload_batch \
        --runtime python38 \
        --trigger-resource <name_of_ingress_bucket> \
        --trigger-event google.storage.object.finalize

At some point, we will write something to trigger this deployment on changes to the cloud function file or similar.
