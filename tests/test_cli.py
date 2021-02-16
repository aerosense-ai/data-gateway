import os
from unittest import TestCase, mock
from click.testing import CliRunner

from data_gateway.cli import gateway_cli
from dummy_serial.dummy_serial import DummySerial


class TestCLI(TestCase):
    def test_version(self):
        """Ensure the version command works in the CLI."""
        result = CliRunner().invoke(gateway_cli, ["--version"])
        assert "version" in result.output

    def test_help(self):
        """Ensure the help commands works in the CLI."""
        help_result = CliRunner().invoke(gateway_cli, ["--help"])
        assert help_result.output.startswith("Usage")

        h_result = CliRunner().invoke(gateway_cli, ["-h"])
        assert help_result.output == h_result.output

    def test_start_and_stop_in_interactive_mode(self):
        """Ensure the gateway can be started and stopped via the CLI in interactive mode."""
        with mock.patch("serial.Serial", new=DummySerial):
            result = CliRunner().invoke(gateway_cli, "start --interactive", input="stop\n")
            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)
            self.assertTrue("Stopping gateway." in result.output)

    def test_start_with_config_file(self):
        """Ensure a configuration file can be provided via the CLI."""
        config_path = os.path.join(os.path.dirname(__file__), "valid_configuration.json")
        with mock.patch("serial.Serial", new=DummySerial):
            result = CliRunner().invoke(gateway_cli, f"start --interactive --config-file={config_path}", input="stop\n")

        self.assertTrue("Loaded configuration file" in result.output)
