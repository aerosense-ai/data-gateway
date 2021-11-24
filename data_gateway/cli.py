import json
import logging
import os
import time

import click
import pkg_resources
import requests
from requests import HTTPError
from slugify import slugify

from data_gateway.exceptions import WrongNumberOfSensorCoordinates


SUPERVISORD_PROGRAM_NAME = "AerosenseGateway"
CREATE_INSTALLATION_CLOUD_FUNCTION_URL = "https://europe-west6-aerosense-twined.cloudfunctions.net/create-installation"

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
    from octue.log_handlers import apply_log_handler, get_remote_handler

    # Apply log handler locally.
    apply_log_handler(log_level=log_level.upper())

    # Stream logs to remote handler if required.
    if logger_uri is not None:
        apply_log_handler(handler=get_remote_handler(logger_uri=logger_uri), log_level=log_level.upper())


@gateway_cli.command()
@click.option(
    "--serial-port",
    type=str,
    default="/dev/ttyACM0",
    show_default=True,
    help="The serial port to read data from.",
)
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
    help="Stop the gateway when no more data is received by the serial port (this is mainly for testing).",
)
@click.option(
    "--save-csv-files",
    is_flag=True,
    default=False,
    help="In interactive mode, also save sensor data as CSV files. This is useful for debugging.",
)
@click.option(
    "--use-dummy-serial-port",
    is_flag=True,
    default=False,
    help="Use a dummy serial port instead of a real one (useful for certain tests and demonstrating the CLI on machines "
    "with no serial port).",
)
def start(
    serial_port,
    config_file,
    interactive,
    output_dir,
    window_size,
    gcp_project_name,
    gcp_bucket_name,
    label,
    save_csv_files,
    stop_when_no_more_data,
    use_dummy_serial_port,
):
    """Begin reading and persisting data from the serial port for the sensors at the installation defined in
    `configuration.json`. Daemonise this for a deployment. In interactive mode, commands can be sent to the
    nodes/sensors via the serial port by typing them into stdin and pressing enter. These commands are:
    [startBaros, startMics, startIMU, getBattery, stop].
    """
    import json
    import sys
    import threading

    import serial

    from data_gateway.configuration import Configuration
    from data_gateway.packet_reader import PacketReader

    if os.path.exists(config_file):
        with open(config_file) as f:
            config = Configuration.from_dict(json.load(f))
        logger.info("Loaded configuration file from %r.", config_file)
    else:
        config = Configuration()
        logger.info("Using default configuration.")

    config.session_data["label"] = label

    if not use_dummy_serial_port:
        serial_port = serial.Serial(port=serial_port, baudrate=config.baudrate)
    else:
        from data_gateway.dummy_serial import DummySerial

        serial_port = DummySerial(port=serial_port, baudrate=config.baudrate)

    # `set_buffer_size` is only available on Windows.
    if os.name == "nt":
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

    if not output_dir.startswith("/"):
        output_dir = os.path.join(".", output_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Start a new thread to parse the serial data while the main thread stays ready to take in commands from stdin.
    packet_reader = PacketReader(
        save_locally=True,
        upload_to_cloud=False,
        output_directory=output_dir,
        window_size=window_size,
        configuration=config,
        save_csv_files=save_csv_files,
    )

    thread = threading.Thread(
        target=packet_reader.read_packets, args=(serial_port, stop_when_no_more_data), daemon=True
    )
    thread.start()

    logger.info(
        "Starting gateway in interactive mode - files will *not* be uploaded to cloud storage but will instead be saved"
        " to disk at %r at intervals of %s seconds.",
        os.path.join(packet_reader.output_directory, packet_reader.session_subdirectory),
        window_size,
    )

    # Keep a record of the commands given.
    commands_record_file = os.path.join(
        packet_reader.output_directory, packet_reader.session_subdirectory, "commands.txt"
    )

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
    "--configuration-file",
    type=click.Path(),
    default="configuration.json",
    help="A path to a JSON configuration file.",
)
def create_installation(configuration_file):
    """Create an installation representing a collection of sensors that data can be collected from. The installation
    information is read from the "installation_data" field of `configuration.json`.
    """
    with open(configuration_file or "configuration.json") as f:
        configuration = json.load(f)

    installation_data = configuration["installation_data"]
    slugified_reference = slugify(installation_data["installation_reference"])

    while True:
        user_confirmation = input(f"Create installation with reference {slugified_reference!r}? [Y/n]\n")

        if user_confirmation.upper() == "N":
            return

        if user_confirmation.upper() in {"Y", ""}:
            break

    for sensor, coordinates in installation_data["sensor_coordinates"].items():
        number_of_sensors = configuration["number_of_sensors"][sensor]

        if len(coordinates) != number_of_sensors:
            raise WrongNumberOfSensorCoordinates(
                f"In the configuration file, the number of sensors for the {sensor!r} sensor type is "
                f"{number_of_sensors} but coordinates were given for {len(coordinates)} sensors - these numbers must "
                f"match."
            )

    # Required parameters:
    parameters = {
        "reference": slugified_reference,
        "turbine_id": installation_data["turbine_id"],
        "blade_id": installation_data["blade_id"],
        "hardware_version": installation_data["hardware_version"],
        "sensor_coordinates": json.dumps(installation_data["sensor_coordinates"]),
    }

    # Optional parameters:
    if installation_data.get("longitude"):
        parameters["longitude"] = installation_data["longitude"]

    if installation_data.get("latitude"):
        parameters["latitude"] = installation_data["latitude"]

    print("Creating...")

    response = requests.post(url=CREATE_INSTALLATION_CLOUD_FUNCTION_URL, json=parameters)

    if not response.status_code == 200:
        raise HTTPError(f"{response.status_code}: {response.text}")

    logger.info("Installation created: %r", parameters)


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


if __name__ == "__main__":
    gateway_cli()
