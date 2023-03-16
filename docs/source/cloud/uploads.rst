.. _uploads:

=======
Uploads
=======

Once running ``gateway start``, windows are uploaded to the cloud ingress bucket (unless the ``--no-upload-to-cloud`` option is used).

If the connection to Google Cloud fails, windows will be written to the hidden directory
``./<output_directory>/.backup`` where they will stay until the connection resumes.
Backup files are deleted upon successful cloud upload.
