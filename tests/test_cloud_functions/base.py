import json
import os
import tempfile

from octue.cloud.credentials import GCPCredentialsManager


class CredentialsEnvironmentVariableAsFile:
    """Temporarily store JSON credentials from the `GOOGLE_APPLICATION_CREDENTIALS` environment variable in a file for
    use during the test class's test run. This is useful on GitHub where a file cannot be created for a secret but
    tests that require credentials to be present as a file are run.
    """

    credentials_file = None
    current_google_application_credentials_variable_value = None

    @classmethod
    def setUpClass(cls):
        """Temporarily write the credentials to a file so that the tests can run on GitHub where the credentials are
        only provided as JSON in an environment variable. Set the credentials environment variable to point to this
        file instead of the credentials JSON.

        :return None:
        """
        cls.credentials_file = tempfile.NamedTemporaryFile(delete=False)
        cls.current_google_application_credentials_variable_value = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

        credentials = GCPCredentialsManager().get_credentials(as_dict=True)

        with open(cls.credentials_file.name, "w") as f:
            json.dump(credentials, f)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cls.credentials_file.name

    @classmethod
    def tearDownClass(cls):
        """Remove the temporary credentials file and restore the credentials environment variable to its original value.

        :return None:
        """
        os.remove(cls.credentials_file.name)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cls.current_google_application_credentials_variable_value
