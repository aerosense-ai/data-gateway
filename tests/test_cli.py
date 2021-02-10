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

    def test_start_and_stop(self):
        """Ensure the gateway can be started and stopped via the CLI in interactive mode."""
        with mock.patch("serial.Serial", new=DummySerial):
            result = CliRunner().invoke(gateway_cli, "start --interactive", input="stop\n")
            self.assertIsNone(result.exception)
            self.assertEqual(result.exit_code, 0)
            self.assertTrue("Stopping gateway." in result.output)
