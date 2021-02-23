.. _cloud_function:

==============
Cloud function
==============

When a batch is uploaded to the Google Cloud storage ingress bucket, a cloud function (a serverless deployed app) runs
on it to clean it and move it to a more permanent home in a different bucket. The cloud function package is included in
the ``data-gateway`` repository in ``cloud_function``, which is where it should be edited and version controlled. When
a new version is ready, it can be deployed to the cloud as follows:

.. code-block::

    gcloud functions deploy clean_and_upload_batch \
        --runtime python38 \
        --trigger-resource <name_of_ingress_bucket> \
        --trigger-event google.storage.object.finalize

The function used as the cloud function is ``cloud_function.main.clean_and_upload_batch`` and it must accept ``event``
and ``context`` arguments in that order.
