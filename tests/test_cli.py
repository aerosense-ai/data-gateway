import os
import tempfile
from unittest import TestCase, mock
from click.testing import CliRunner

from data_gateway.cli import gateway_cli
from dummy_serial.dummy_serial import DummySerial
from tests import TEST_BUCKET_NAME


class TestCLI(TestCase):
    TEST_PROJECT_NAME = "a-project-name"

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
                    f"--gcp-project-name={self.TEST_PROJECT_NAME} "
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
                    f"--gcp-project-name={self.TEST_PROJECT_NAME} "
                    f"--gcp-bucket-name={TEST_BUCKET_NAME} "
                    f"--stop-when-no-more-data",
                )

        self.assertIsNone(result.exception)
        self.assertEqual(result.exit_code, 0)

    def test_commands_are_recorded_in_interactive_mode(self):
        """Ensure commands given in interactive mode are recorded."""
        commands = "here\nit\nis\nstop\n"

        with tempfile.TemporaryDirectory() as temporary_directory:
            with mock.patch("serial.Serial", new=DummySerial):
                CliRunner().invoke(
                    gateway_cli, f"start --interactive --output-dir={temporary_directory}", input=commands
                )

            with open(os.path.join(temporary_directory, "commands.txt")) as f:
                self.assertEqual(f.read(), commands)

    def test_start_and_stop_in_interactive_mode(self):
        """Ensure the gateway can be started and stopped via the CLI in interactive mode."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            with mock.patch("serial.Serial", new=DummySerial):
                result = CliRunner().invoke(
                    gateway_cli, f"start --interactive --output-dir={temporary_directory}", input="stop\n"
                )

            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)
            self.assertTrue("Stopping gateway." in result.output)

    def test_start_with_config_file(self):
        """Ensure a configuration file can be provided via the CLI."""
        config_path = os.path.join(os.path.dirname(__file__), "valid_configuration.json")

        with tempfile.TemporaryDirectory() as temporary_directory:
            with mock.patch("serial.Serial", new=DummySerial):
                result = CliRunner().invoke(
                    gateway_cli,
                    f"start --interactive --config-file={config_path} --output-dir={temporary_directory}",
                    input="stop\n",
                )

        self.assertTrue("Loaded configuration file" in result.output)
