import logging
import os
import time

import click
import pkg_resources


SUPERVISORD_PROGRAM_NAME = "AerosenseGateway"

logger = logging.getLogger(__name__)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--logger-uri",
    default=None,
    type=click.STRING,
    show_default=True,
    help="Stream logs to a websocket at the given URI (useful for monitoring what's happening remotely).",
)
@click.option(
    "--log-level",
    default="info",
    type=click.Choice(["debug", "info", "warning", "error"], case_sensitive=False),
    show_default=True,
    help="Set the log level.",
)
@click.version_option(version=pkg_resources.get_distribution("data_gateway").version)
def gateway_cli(logger_uri, log_level):
    """AeroSense Gateway CLI. Run the on-tower gateway service to read data from the bluetooth receivers and send it
    to AeroSense Cloud.
    """
    if logger_uri is not None:
        from octue.log_handlers import apply_log_handler, get_remote_handler

        handler = get_remote_handler(logger_uri=logger_uri)
        apply_log_handler(logger_name=__name__, handler=handler, log_level=log_level.upper())


@gateway_cli.command()
@click.option(
    "--config-file",
    type=click.Path(dir_okay=False),
    default="config.json",
    show_default=True,
    help="Path to your Aerosense deployment configuration file.",
)
@click.option(
    "--interactive",
    "-i",
    is_flag=True,
    default=False,
    show_default=True,
    help="""
        Run the gateway in interactive mode, allowing commands to be sent to the serial port via the command line.\n
        WARNING: the output of the gateway will be saved to disk locally and not uploaded to the cloud if this option
        is used. The same file/folder structure will be used either way.
    """,
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False),
    default="data_gateway",
    show_default=True,
    help="The directory in which to save batch files from the data gateway.",
)
@click.option(
    "--batch-interval",
    type=click.FLOAT,
    default=600,
    show_default=True,
    help="The time interval for which data is collected into a batch before being persisted locally or to the cloud.",
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
@click.option(
    "--stop-when-no-more-data",
    is_flag=True,
    default=False,
    show_default=True,
    help="Stop the gateway when no more data is received by the serial port (this is mainly for testing).",
)
def start(
    config_file, interactive, output_dir, batch_interval, gcp_project_name, gcp_bucket_name, stop_when_no_more_data
):
    """Start the gateway service (daemonise this for a deployment).

    Node commands are: [startBaros, startMics, startIMU ...]
    """
    import json
    import threading

    import serial

    import sys
    from data_gateway.reader import PacketReader
    from data_gateway.reader.configuration import Configuration

    if os.path.exists(config_file):
        with open(config_file) as f:
            config = Configuration.from_dict(json.load(f))
        logger.info("Loaded configuration file from %r.", config_file)
    else:
        config = Configuration()
        logger.info("Using default configuration.")

    serial_port = serial.Serial(port=config.serial_port, baudrate=config.baudrate)

    if os.name == "nt":  # set_buffer_size is available only on windows
        serial_port.set_buffer_size(rx_size=config.serial_buffer_rx_size, tx_size=config.serial_buffer_tx_size)

    if not interactive:
        logger.info(
            "Starting packet reader in non-interactive mode - files will be uploaded to cloud storage at intervals of "
            "%s seconds.",
            batch_interval,
        )

        PacketReader(
            save_locally=False,
            upload_to_cloud=True,
            output_directory=output_dir,
            batch_interval=batch_interval,
            project_name=gcp_project_name,
            bucket_name=gcp_bucket_name,
            configuration=config,
        ).read_packets(serial_port, stop_when_no_more_data=stop_when_no_more_data)

        return

    # Start a new thread to parse the serial data while the main thread stays ready to take in commands from stdin.
    packet_reader = PacketReader(
        save_locally=True,
        upload_to_cloud=False,
        output_directory=output_dir,
        batch_interval=batch_interval,
        configuration=config,
    )

    if not output_dir.startswith("/"):
        output_dir = os.path.join(".", output_dir)

    thread = threading.Thread(
        target=packet_reader.read_packets, args=(serial_port, stop_when_no_more_data), daemon=True
    )
    thread.start()

    logger.info(
        "Starting gateway in interactive mode - files will *not* be uploaded to cloud storage but will instead be saved"
        " to disk at %r at intervals of %s seconds.",
        output_dir,
        batch_interval,
    )

    # Keep a record of the commands given.
    commands_record_file = os.path.join(output_dir, "commands.txt")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        while not packet_reader.stop:
            for line in sys.stdin:

                with open(commands_record_file, "a") as f:
                    f.write(line)

                if line.startswith("sleep") and line.endswith("\n"):
                    time.sleep(int(line.split(" ")[-1].strip()))
                elif line == "stop\n":
                    packet_reader.stop = True
                    break

                # Send the command to the node
                serial_port.write(line.encode("utf_8"))

    except KeyboardInterrupt:
        packet_reader.stop = True

    logger.info("Stopping gateway.")
    packet_reader.writer.force_persist()


@gateway_cli.command()
@click.option(
    "--config-file",
    type=click.Path(),
    default="config.json",
    show_default=True,
    help="Path to your Aerosense deployment configuration file.",
)
def supervisord_conf(config_file):
    """Print conf entry for use with supervisord. Daemonising a process ensures it automatically restarts after a
    failure and on startup of the operating system failure.
    """
    supervisord_conf_str = f"""

[program:{SUPERVISORD_PROGRAM_NAME,}]
command=gateway start --config-file {os.path.abspath(config_file)}"""

    print(supervisord_conf_str)
    return 0
