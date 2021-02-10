import logging
import os
from _thread import start_new_thread
import click
import pkg_resources
import serial
from octue.logging_handlers import get_remote_handler

import sys
from data_gateway.readers import PacketReader, constants


SUPERVISORD_PROGRAM_NAME = "AerosenseGateway"

logger = logging.getLogger(__name__)

global_cli_context = {}


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--logger-uri",
    default=None,
    type=click.STRING,
    show_default=True,
    help="Stream logs to a websocket at the given URI (useful for monitoring what's happening remotely)",
)
@click.option(
    "--log-level",
    default="info",
    type=click.Choice(["debug", "info", "warning", "error"], case_sensitive=False),
    show_default=True,
    help="Log level used for the analysis.",
)
@click.version_option(version=pkg_resources.get_distribution("data_gateway").version)
def gateway_cli(logger_uri, log_level):
    """AeroSense Gateway CLI.

    Runs the on-nacelle gateway service to read data from the bluetooth receivers and send it to AeroSense Cloud.
    """
    global_cli_context["logger_uri"] = logger_uri
    global_cli_context["log_handler"] = None
    global_cli_context["log_level"] = log_level.upper()

    # Stealing a remote logging trick from the octue library
    if global_cli_context["logger_uri"]:
        global_cli_context["log_handler"] = get_remote_handler(
            logger_uri=global_cli_context["logger_uri"], log_level=global_cli_context["log_level"]
        )


@gateway_cli.command()
@click.option(
    "--config-file",
    type=click.Path(),
    default="config.json",
    show_default=True,
    help="Path to your aerosense deployment configuration file.",
)
def start(config_file):
    """Start the gateway service (daemonise this for a deployment)."""
    serial_port = serial.Serial(port=constants.SERIAL_PORT, baudrate=constants.BAUDRATE)
    serial_port.set_buffer_size(rx_size=constants.SERIAL_BUFFER_RX_SIZE, tx_size=constants.SERIAL_BUFFER_TX_SIZE)
    packet_reader = PacketReader()

    # This new thread will parse the serial data while the main thread stays ready to take in commands from stdin.
    start_new_thread(packet_reader.read_packets, (serial_port,))

    """
    time.sleep(1)
    ser.write(("configMics "  + str(MICS_FREQ)  + " " + str(MICS_BM) + "\n").encode('utf_8'))
    time.sleep(1)
    ser.write(("configBaros " + str(BAROS_FREQ) + " " + str(BAROS_BM) + "\n").encode('utf_8'))
    time.sleep(1)
    ser.write(("configAccel " + str(ACC_FREQ)   + " " + str(ACC_RANGE) + "\n").encode('utf_8'))
    time.sleep(1)
    ser.write(("configGyro "  + str(GYRO_FREQ)  + " " + str(GYRO_RANGE) + "\n").encode('utf_8'))
    """

    while not packet_reader.stop:
        for line in sys.stdin:
            if line == "stop\n":
                packet_reader.stop = True
                break

            serial_port.write(line.encode("utf_8"))


@gateway_cli.command()
@click.option(
    "--config-file",
    type=click.Path(),
    default="config.json",
    show_default=True,
    help="Path to your aerosense deployment configuration file.",
)
def supervisord_conf(config_file):
    """Print conf entry for use with supervisord

    Daemonising a process ensures it automatically restarts after a failure and on startup of the operating system
    failure.
    """

    supervisord_conf_str = """

[program:{prg_name}]
command=gateway --config-file {cfg_file}""".format(
        prg_name=SUPERVISORD_PROGRAM_NAME, cfg_file=os.path.abspath(config_file)
    )

    print(supervisord_conf_str)
    return 0


if __name__ == "__main__":
    args = sys.argv[1:] if len(sys.argv) > 1 else []
    gateway_cli(args)
