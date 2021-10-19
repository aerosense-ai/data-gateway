import json
import os
import tempfile
from unittest import mock
from click.testing import CliRunner

from data_gateway.cli import gateway_cli
from dummy_serial.dummy_serial import DummySerial
from tests import LENGTH, PACKET_KEY, RANDOM_BYTES, TEST_BUCKET_NAME, TEST_PROJECT_NAME
from tests.base import BaseTestCase


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


class TestCLI(BaseTestCase):
    def test_version(self):
        """Ensure the version command works in the CLI."""
        result = CliRunner().invoke(gateway_cli, ["--version"])
        assert "version" in result.output

    def test_help(self):
        """Ensure the help commands works in the CLI."""
        help_result = CliRunner().invoke(gateway_cli, ["--help"])
        assert "Usage" in help_result.output

        h_result = CliRunner().invoke(gateway_cli, ["-h"])
        assert "Usage" in h_result.output

    def test_start(self):
        """Ensure the gateway can be started via the CLI. The "stop-when-no-more-data" option is enabled so the test
        doesn't run forever.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            with mock.patch("serial.Serial", new=DummySerial):
                result = CliRunner().invoke(
                    gateway_cli,
                    [
                        "start",
                        f"--gcp-project-name={TEST_PROJECT_NAME}",
                        f"--gcp-bucket-name={TEST_BUCKET_NAME}",
                        f"--output-dir={temporary_directory}",
                        "--stop-when-no-more-data",
                    ],
                )

        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)

    def test_start_with_default_output_directory(self):
        """Ensure the gateway can be started via the CLI with a default output directory. The "stop-when-no-more-data"
        option is enabled so the test doesn't run forever.
        """
        initial_directory = os.getcwd()

        with tempfile.TemporaryDirectory() as temporary_directory:
            os.chdir(temporary_directory)

            with mock.patch("serial.Serial", new=DummySerial):
                result = CliRunner().invoke(
                    gateway_cli,
                    [
                        "start",
                        f"--gcp-project-name={TEST_PROJECT_NAME}",
                        f"--gcp-bucket-name={TEST_BUCKET_NAME}",
                        "--stop-when-no-more-data",
                    ],
                )

            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)
            os.chdir(initial_directory)

    def test_commands_are_recorded_in_interactive_mode(self):
        """Ensure commands given in interactive mode are recorded. Interactive mode should work without the
        GOOGLE_APPLICATION_CREDENTIALS environment variable.
        """
        with EnvironmentVariableRemover("GOOGLE_APPLICATION_CREDENTIALS"):
            commands = "here\nit\nis\nstop\n"

            with tempfile.TemporaryDirectory() as temporary_directory:
                with mock.patch("serial.Serial", new=DummySerial):
                    result = CliRunner().invoke(
                        gateway_cli, ["start", "--interactive", f"--output-dir={temporary_directory}"], input=commands
                    )

                self.assertIsNone(result.exception)
                self.assertEqual(result.exit_code, 0)

                with open(os.path.join(temporary_directory, "commands.txt")) as f:
                    self.assertEqual(f.read(), commands)

    def test_start_and_stop_in_interactive_mode(self):
        """Ensure the gateway can be started and stopped via the CLI in interactive mode. Interactive mode should work
        without the GOOGLE_APPLICATION_CREDENTIALS environment variable.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            with EnvironmentVariableRemover("GOOGLE_APPLICATION_CREDENTIALS"):
                with mock.patch("logging.StreamHandler.emit") as mock_local_logger_emit:
                    with mock.patch("serial.Serial", new=DummySerial):
                        result = CliRunner().invoke(
                            gateway_cli,
                            ["start", "--interactive", f"--output-dir={temporary_directory}"],
                            input="stop\n",
                        )

            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)

            self.assertTrue(
                any(call_arg[0][0].msg == "Stopping gateway." for call_arg in mock_local_logger_emit.call_args_list)
            )

    def test_interactive_mode_writes_to_disk(self):
        """Ensure interactive mode writes data to disk. It should work without the GOOGLE_APPLICATION_CREDENTIALS
        environment variable.
        """
        with EnvironmentVariableRemover("GOOGLE_APPLICATION_CREDENTIALS"):
            serial_port = DummySerial(port="test")
            sensor_type = bytes([34])
            serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[0])))
            serial_port.write(data=b"".join((PACKET_KEY, sensor_type, LENGTH, RANDOM_BYTES[1])))

            with tempfile.TemporaryDirectory() as temporary_directory:
                with mock.patch("serial.Serial", return_value=serial_port):
                    result = CliRunner().invoke(
                        gateway_cli,
                        ["start", "--interactive", f"--output-dir={temporary_directory}"],
                        input="sleep 2\nstop\n",
                    )

                session_subdirectory = [item for item in os.scandir(temporary_directory) if item.is_dir()][0].name

                with open(os.path.join(temporary_directory, session_subdirectory, "window-0.json")) as f:
                    data = json.loads(f.read())

                self.assertEqual(len(data), 2)
                self.assertTrue(len(data["sensor_data"]["Baros_P"][0]) > 1)
                self.assertTrue(len(data["sensor_data"]["Baros_T"][0]) > 1)

            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)

    def test_start_with_config_file(self):
        """Ensure a configuration file can be provided via the CLI. Interactive mode should work without the
        GOOGLE_APPLICATION_CREDENTIALS environment variable.
        """
        with EnvironmentVariableRemover("GOOGLE_APPLICATION_CREDENTIALS"):
            config_path = os.path.join(os.path.dirname(__file__), "valid_configuration.json")

            with mock.patch("logging.StreamHandler.emit") as mock_local_logger_emit:
                with tempfile.TemporaryDirectory() as temporary_directory:
                    with mock.patch("serial.Serial", new=DummySerial):
                        result = CliRunner().invoke(
                            gateway_cli,
                            [
                                "start",
                                "--interactive",
                                f"--config-file={config_path}",
                                f"--output-dir={temporary_directory}",
                            ],
                            input="stop\n",
                        )

            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)
            self.assertIn("Loaded configuration file", mock_local_logger_emit.call_args_list[0][0][0].msg)
