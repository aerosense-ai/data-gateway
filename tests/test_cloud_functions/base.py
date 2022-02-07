import json
import os

from octue.cloud.credentials import GCPCredentialsManager


class CredentialsEnvironmentVariableAsFile:
    """Temporarily store JSON credentials from the `GOOGLE_APPLICATION_CREDENTIALS` environment variable in a file for
    use during the test class's test run. This is useful on GitHub where a file cannot be created for a secret but
    tests that require credentials to be present as a file are run.
    """

    def __init__(self):
        self.credentials_path = "temporary_file.json"
        self.current_google_application_credentials_variable_value = None

    def __enter__(self):
        """Temporarily write the credentials to a file so that the tests can run on GitHub where the credentials are
        only provided as JSON in an environment variable. Set the credentials environment variable to point to this
        file instead of the credentials JSON.

        :return None:
        """
        self.current_google_application_credentials_variable_value = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

        credentials = GCPCredentialsManager().get_credentials(as_dict=True)

        with open(self.credentials_path, "w") as f:
            json.dump(credentials, f)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.credentials_path

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Remove the temporary credentials file and restore the credentials environment variable to its original value.

        :return None:
        """
        os.remove(self.credentials_path)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.current_google_application_credentials_variable_value
