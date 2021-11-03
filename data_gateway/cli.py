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
@click.argument("installation_reference", type=str)
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
    help="The directory in which to save data windows from the gateway.",
)
@click.option(
    "--window-size",
    type=click.FLOAT,
    default=600,
    show_default=True,
    help="The window length in seconds that data is grouped into before being persisted locally or to the cloud.",
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
    "--label",
    type=str,
    default=None,
    show_default=True,
    help="An optional label to associate with data persisted in this run of the gateway.",
)
@click.option(
    "--stop-when-no-more-data",
    is_flag=True,
    default=False,
    show_default=True,
    help="Stop the gateway when no more data is received by the serial port (this is mainly for testing).",
)
def start(
    installation_reference,
    config_file,
    interactive,
    output_dir,
    window_size,
    gcp_project_name,
    gcp_bucket_name,
    label,
    stop_when_no_more_data,
):
    """Begin persisting data from the serial port for sensors at INSTALLATION_REFERENCE. Daemonise this for a deployment.

    INSTALLATION_REFERENCE is the name associated with the geographical installation of sensors e.g. on a specific wind
    turbine or wind tunnel.
    """
    import json
    import threading

    import serial

    import sys
    from data_gateway.configuration import Configuration
    from data_gateway.packet_reader import PacketReader

    if os.path.exists(config_file):
        with open(config_file) as f:
            config = Configuration.from_dict(json.load(f))
        logger.info("Loaded configuration file from %r.", config_file)
    else:
        config = Configuration()
        logger.info("Using default configuration.")

    config.user_data["installation_reference"] = installation_reference
    config.user_data["label"] = label

    serial_port = serial.Serial(port=config.serial_port, baudrate=config.baudrate)
    serial_port.set_buffer_size(rx_size=config.serial_buffer_rx_size, tx_size=config.serial_buffer_tx_size)

    if not interactive:
        logger.info(
            "Starting packet reader in non-interactive mode - files will be uploaded to cloud storage at intervals of "
            "%s seconds.",
            window_size,
        )

        PacketReader(
            save_locally=False,
            upload_to_cloud=True,
            output_directory=output_dir,
            window_size=window_size,
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
        window_size=window_size,
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
        window_size,
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
