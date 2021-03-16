import os
import tempfile
from unittest import TestCase, mock
from click.testing import CliRunner

from data_gateway.cli import gateway_cli
from dummy_serial.dummy_serial import DummySerial
from tests import TEST_BUCKET_NAME, TEST_PROJECT_NAME


class EnvironmentVariableRemover:
    """Remove the given environment variables in the context, restoring them once outside it.

    :param iter(str) environment_variable_names:
    :return None:
    """

    def __init__(self, *environment_variable_names):
        self.environment_variables = tuple((name, os.environ.get(name)) for name in environment_variable_names)

    def __enter__(self):
        for variable_name, _ in self.environment_variables:
            del os.environ[variable_name]

    def __exit__(self, exc_type, exc_val, exc_tb):
        for variable_name, value in self.environment_variables:
            os.environ[variable_name] = value


class TestCLI(TestCase):
    def test_version(self):
        """Ensure the version command works in the CLI."""
        result = CliRunner().invoke(gateway_cli, ["--version"])
        assert "version" in result.output

    def test_help(self):
        """Ensure the help commands works in the CLI."""
        help_result = CliRunner().invoke(gateway_cli, ["--help"])
        assert "Usage" in help_result.output

        h_result = CliRunner().invoke(gateway_cli, ["-h"])
        assert help_result.output == h_result.output

    def test_start(self):
        """Ensure the gateway can be started via the CLI. The "stop-when-no-more-data" option is enabled so the test
        doesn't run forever.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            with mock.patch("serial.Serial", new=DummySerial):
                result = CliRunner().invoke(
                    gateway_cli,
                    f"start "
                    f"--gcp-project-name={TEST_PROJECT_NAME} "
                    f"--gcp-bucket-name={TEST_BUCKET_NAME} "
                    f"--output-dir={temporary_directory} "
                    f"--stop-when-no-more-data",
                )

        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)

    def test_start_with_default_output_directory(self):
        """Ensure the gateway can be started via the CLI with a default output directory. The "stop-when-no-more-data"
        option is enabled so the test doesn't run forever.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            os.chdir(temporary_directory)

            with mock.patch("serial.Serial", new=DummySerial):
                result = CliRunner().invoke(
                    gateway_cli,
                    f"start "
                    f"--gcp-project-name={TEST_PROJECT_NAME} "
                    f"--gcp-bucket-name={TEST_BUCKET_NAME} "
                    f"--stop-when-no-more-data",
                )

        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)

    def test_commands_are_recorded_in_interactive_mode(self):
        """Ensure commands given in interactive mode are recorded. Interactive mode should work without the
        GOOGLE_APPLICATION_CREDENTIALS environment variable.
        """
        with EnvironmentVariableRemover("GOOGLE_APPLICATION_CREDENTIALS"):
            commands = "here\nit\nis\nstop\n"

            with tempfile.TemporaryDirectory() as temporary_directory:
                with mock.patch("serial.Serial", new=DummySerial):
                    result = CliRunner().invoke(
                        gateway_cli, f"start --interactive --output-dir={temporary_directory}", input=commands
                    )

                self.assertIsNone(result.exception)
                self.assertEqual(result.exit_code, 0)

                with open(os.path.join(temporary_directory, "commands.txt")) as f:
                    self.assertEqual(f.read(), commands)

    def test_start_and_stop_in_interactive_mode(self):
        """Ensure the gateway can be started and stopped via the CLI in interactive mode. Interactive mode should work
        without the GOOGLE_APPLICATION_CREDENTIALS environment variable.
        """
        with EnvironmentVariableRemover("GOOGLE_APPLICATION_CREDENTIALS"):
            with tempfile.TemporaryDirectory() as temporary_directory:
                with mock.patch("serial.Serial", new=DummySerial):
                    result = CliRunner().invoke(
                        gateway_cli, f"start --interactive --output-dir={temporary_directory}", input="stop\n"
                    )

            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)
            self.assertTrue("Stopping gateway." in result.output)

    def test_start_with_config_file(self):
        """Ensure a configuration file can be provided via the CLI. Interactive mode should work without the
        GOOGLE_APPLICATION_CREDENTIALS environment variable.
        """
        with EnvironmentVariableRemover("GOOGLE_APPLICATION_CREDENTIALS"):
            config_path = os.path.join(os.path.dirname(__file__), "valid_configuration.json")

            with tempfile.TemporaryDirectory() as temporary_directory:
                with mock.patch("serial.Serial", new=DummySerial):
                    result = CliRunner().invoke(
                        gateway_cli,
                        f"start --interactive --config-file={config_path} --output-dir={temporary_directory}",
                        input="stop\n",
                    )

            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)
            self.assertTrue("Loaded configuration file" in result.output)
