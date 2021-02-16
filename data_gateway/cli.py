import json
import logging
import os
import threading
import click
import pkg_resources
import serial
from octue.logging_handlers import get_remote_handler

import sys
from data_gateway.reader import PacketReader
from data_gateway.reader.configuration import Configuration


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
@click.option(
    "--interactive",
    "-i",
    is_flag=True,
    default=False,
    show_default=True,
    help="""
        Run the gateway in interactive mode, allowing commands to be sent to the serial port via the command line.\n
        WARNING: the output of the gateway will be saved to files locally and not uploaded to the cloud if this
        option is used. The same file/folder structure will be used either way.
    """,
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False),
    default="data_gateway",
    show_default=True,
    help="The directory in which to save the batch files.",
)
@click.option(
    "--batch-interval",
    type=click.FLOAT,
    default=600,
    show_default=True,
    help="The time interval in which to batch data into to be persisted locally or to the cloud.",
)
@click.option(
    "--gcp-project-name",
    type=click.STRING,
    default=None,
    show_default=True,
    help="The name of the Google Cloud Platform (GCP) project to use.",
)
@click.option(
    "--gcp-bucket-name",
    type=click.STRING,
    default=None,
    show_default=True,
    help="The name of the Google Cloud Platform (GCP) storage bucket to use.",
)
def start(config_file, interactive, output_dir, batch_interval, gcp_project_name, gcp_bucket_name):
    """Start the gateway service (daemonise this for a deployment)."""
    if os.path.exists(config_file):
        with open(config_file) as f:
            config = Configuration.from_dict(json.load(f))
        logger.info("Loaded configuration file from %r.", config_file)
    else:
        config = None

    serial_port = serial.Serial(port=config.serial_port, baudrate=config.baudrate)
    serial_port.set_buffer_size(rx_size=config.serial_buffer_rx_size, tx_size=config.serial_buffer_tx_size)

    if not interactive:
        print(
            "Starting packet reader in non-interactive mode - files will be uploaded to cloud storage at intervals of "
            f"{batch_interval} seconds."
        )

        PacketReader(
            save_locally=False,
            upload_to_cloud=True,
            output_directory=output_dir,
            batch_interval=batch_interval,
            project_name=gcp_project_name,
            bucket_name=gcp_bucket_name,
            configuration=config,
        ).read_packets(serial_port)

        return

    # Start a new thread to parse the serial data while the main thread stays ready to take in commands from stdin.
    packet_reader = PacketReader(
        save_locally=True,
        upload_to_cloud=False,
        output_directory=output_dir,
        batch_interval=batch_interval,
        configuration=config,
    )

    threading.Thread(target=packet_reader.read_packets, args=(serial_port,), daemon=True)
    print(
        "Starting gateway in interactive mode - files will *not* be uploaded to cloud storage but will instead be saved"
        f" to disk at {os.path.join('.', packet_reader.writer.output_directory)!r} at intervals of {batch_interval} "
        "seconds."
    )

    try:
        while not packet_reader.stop:
            for line in sys.stdin:
                if line == "stop\n":
                    packet_reader.stop = True
                    break

                serial_port.write(line.encode("utf_8"))

    except KeyboardInterrupt:
        packet_reader.stop = True

    print("Stopping gateway.")
    packet_reader.writer.force_persist()


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
    supervisord_conf_str = f"""

[program:{SUPERVISORD_PROGRAM_NAME,}]
command=gateway start --config-file {os.path.abspath(config_file)}"""

    print(supervisord_conf_str)
    return 0


if __name__ == "__main__":
    args = sys.argv[1:] if len(sys.argv) > 1 else []
    gateway_cli(args)
