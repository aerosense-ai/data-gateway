import json
import os
import tempfile
from unittest import mock

import requests
from click.testing import CliRunner

from data_gateway.cli import CREATE_INSTALLATION_CLOUD_FUNCTION_URL, gateway_cli
from data_gateway.dummy_serial import DummySerial
from tests import LENGTH, PACKET_KEY, RANDOM_BYTES, TEST_BUCKET_NAME, TEST_PROJECT_NAME
from tests.base import BaseTestCase


CONFIGURATION_PATH = os.path.join(os.path.dirname(__file__), "valid_configuration.json")


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


class TestStart(BaseTestCase):
    def test_start(self):
        """Ensure the gateway can be started via the CLI."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            with mock.patch("serial.Serial", new=DummySerial):
                result = CliRunner().invoke(
                    gateway_cli,
                    [
                        "start",
                        f"--gcp-project-name={TEST_PROJECT_NAME}",
                        f"--gcp-bucket-name={TEST_BUCKET_NAME}",
                        f"--output-dir={temporary_directory}",
                    ],
                    input="stop\n",
                )

        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)

    def test_start_with_default_output_directory(self):
        """Ensure the gateway can be started via the CLI with a default output directory."""
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
                    ],
                    input="sleep 2\nstop\n",
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
                        gateway_cli,
                        ["start", "--no-upload-to-cloud", f"--output-dir={temporary_directory}"],
                        input=commands,
                    )

                self.assertIsNone(result.exception)
                self.assertEqual(result.exit_code, 0)

                with open(os.path.join(temporary_directory, os.listdir(temporary_directory)[0], "commands.txt")) as f:
                    self.assertEqual(f.read(), commands)

    def test_log_level_can_be_set(self):
        """Test that the log level can be set."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            with mock.patch("serial.Serial", new=DummySerial):
                with self.assertLogs(level="DEBUG") as mock_logger:
                    result = CliRunner().invoke(
                        gateway_cli,
                        [
                            "--log-level=debug",
                            "start",
                            "--no-upload-to-cloud",
                            f"--output-dir={temporary_directory}",
                        ],
                        input="stop\n",
                    )

                    self.assertIsNone(result.exception)
                    self.assertEqual(result.exit_code, 0)

                    debug_message_found = False

                    for message in mock_logger.output:
                        if "DEBUG" in message:
                            debug_message_found = True
                            break

                    self.assertTrue(debug_message_found)

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
                            ["start", "--no-upload-to-cloud", f"--output-dir={temporary_directory}"],
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
                        ["start", "--no-upload-to-cloud", f"--output-dir={temporary_directory}"],
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
            with mock.patch("logging.StreamHandler.emit") as mock_local_logger_emit:
                with tempfile.TemporaryDirectory() as temporary_directory:
                    with mock.patch("serial.Serial", new=DummySerial):
                        result = CliRunner().invoke(
                            gateway_cli,
                            [
                                "start",
                                "--no-upload-to-cloud",
                                f"--config-file={CONFIGURATION_PATH}",
                                f"--output-dir={temporary_directory}",
                            ],
                            input="stop\n",
                        )

            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)
            self.assertIn("Loaded configuration file", mock_local_logger_emit.call_args_list[0][0][0].msg)


class TestCreateInstallation(BaseTestCase):
    def test_create_installation_slugifies_and_lowercases_names(self):
        """Test that names given to the create-installation command are lower-cased and slugified."""
        with EnvironmentVariableRemover("GOOGLE_APPLICATION_CREDENTIALS"):
            with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
                with open(temporary_file.name, "w") as f:
                    json.dump(
                        {
                            "installation_data": {
                                "installation_reference": "My Installation_1",
                                "turbine_id": 0,
                                "blade_id": 0,
                                "hardware_version": "1.7.19",
                                "sensor_coordinates": {},
                            }
                        },
                        f,
                    )

                with mock.patch("requests.post", return_value=mock.Mock(status_code=200)) as mock_post:
                    result = CliRunner().invoke(
                        gateway_cli,
                        ["create-installation", f"--config-file={temporary_file.name}"],
                        input="Y",
                    )

        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)

        mock_post.assert_called_with(
            url=CREATE_INSTALLATION_CLOUD_FUNCTION_URL,
            json={
                "reference": "my-installation-1",
                "turbine_id": 0,
                "blade_id": 0,
                "hardware_version": "1.7.19",
                "sensor_coordinates": "{}",
            },
        )

    def test_create_installation_with_longitude_and_latitude(self):
        """Test creating an installation with longitude and latitude works."""
        with EnvironmentVariableRemover("GOOGLE_APPLICATION_CREDENTIALS"):
            with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
                with open(temporary_file.name, "w") as f:
                    json.dump(
                        {
                            "installation_data": {
                                "installation_reference": "My Installation_1",
                                "turbine_id": 0,
                                "blade_id": 0,
                                "hardware_version": "1.7.19",
                                "sensor_coordinates": {},
                                "longitude": 3.25604,
                                "latitude": 178.24833,
                            }
                        },
                        f,
                    )

            with mock.patch("requests.post", return_value=mock.Mock(status_code=200)) as mock_post:
                result = CliRunner().invoke(
                    gateway_cli, ["create-installation", f"--config-file={temporary_file.name}"], input="Y"
                )

        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)

        mock_post.assert_called_with(
            url=CREATE_INSTALLATION_CLOUD_FUNCTION_URL,
            json={
                "reference": "my-installation-1",
                "turbine_id": 0,
                "blade_id": 0,
                "hardware_version": "1.7.19",
                "sensor_coordinates": "{}",
                "longitude": 3.25604,
                "latitude": 178.24833,
            },
        )

    def test_create_installation_raises_error_if_status_code_is_not_200(self):
        """Test that an `HTTPError` is raised if the status code of the response received by the create-installation
        command is not 200.
        """
        with EnvironmentVariableRemover("GOOGLE_APPLICATION_CREDENTIALS"):
            with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
                with open(temporary_file.name, "w") as f:
                    json.dump(
                        {
                            "installation_data": {
                                "installation_reference": "My Installation_1",
                                "turbine_id": 0,
                                "blade_id": 0,
                                "hardware_version": "1.7.19",
                                "sensor_coordinates": {},
                            }
                        },
                        f,
                    )

            with mock.patch("requests.post", return_value=mock.Mock(status_code=403)):
                result = CliRunner().invoke(
                    gateway_cli, ["create-installation", f"--config-file={temporary_file.name}"], input="Y"
                )

        self.assertEqual(result.exit_code, 1)
        self.assertIsInstance(result.exception, requests.HTTPError)
