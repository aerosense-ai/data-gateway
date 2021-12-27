import json
import logging
import os
import time

import click
import pkg_resources
import requests
import serial
from requests import HTTPError
from slugify import slugify

from data_gateway.configuration import Configuration
from data_gateway.dummy_serial import DummySerial
from data_gateway.exceptions import DataMustBeSavedError, WrongNumberOfSensorCoordinatesError
from data_gateway.routine import Routine


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
    """Enter the AeroSense Gateway CLI. Run the on-tower gateway service to read data from the bluetooth receivers and
    send it to AeroSense Cloud.
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
    help="Path to your Aerosense deployment configuration JSON file.",
)
@click.option(
    "--routine-file",
    type=click.Path(dir_okay=False),
    default="routine.json",
    show_default=True,
    help="Path to sensor command routine JSON file.",
)
@click.option(
    "--save-locally", "-l", is_flag=True, default=False, show_default=True, help="Save output JSON data to disk."
)
@click.option(
    "--no-upload-to-cloud",
    "-nc",
    is_flag=True,
    default=False,
    show_default=True,
    help="Don't upload output JSON data to the cloud.",
)
@click.option(
    "--interactive",
    "-i",
    is_flag=True,
    default=False,
    show_default=True,
    help="Run the gateway in interactive mode, allowing commands to be sent to the serial port via the command line.",
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
    routine_file,
    save_locally,
    no_upload_to_cloud,
    interactive,
    output_dir,
    window_size,
    gcp_project_name,
    gcp_bucket_name,
    label,
    save_csv_files,
    use_dummy_serial_port,
):
    """Begin reading and persisting data from the serial port for the sensors at the installation defined in
    `configuration.json`. Daemonise this for a deployment. In interactive mode, commands can be sent to the
    nodes/sensors via the serial port by typing them into stdin and pressing enter. These commands are:
    [startBaros, startMics, startIMU, getBattery, stop].
    """
    import sys
    import threading

    from data_gateway.packet_reader import PacketReader

    if not save_locally and no_upload_to_cloud:
        raise DataMustBeSavedError(
            "Data from the gateway must either be saved locally or uploaded to the cloud. Please adjust the CLI "
            "options provided."
        )

    config = _load_configuration(configuration_path=config_file)
    config.session_data["label"] = label

    serial_port = _get_serial_port(serial_port, configuration=config, use_dummy_serial_port=use_dummy_serial_port)
    routine = _load_routine(routine_path=routine_file, interactive=interactive, serial_port=serial_port)
    output_directory = _update_and_create_output_directory(output_directory_path=output_dir)

    # Start a new thread to parse the serial data while the main thread stays ready to take in commands from stdin.
    packet_reader = PacketReader(
        save_locally=save_locally,
        upload_to_cloud=not no_upload_to_cloud,
        output_directory=output_directory,
        window_size=window_size,
        project_name=gcp_project_name,
        bucket_name=gcp_bucket_name,
        configuration=config,
        save_csv_files=save_csv_files,
    )

    logger.info("Starting packet reader.")

    if not no_upload_to_cloud:
        logger.info("Files will be uploaded to cloud storage at intervals of %s seconds.", window_size)

    if save_locally:
        logger.info(
            "Files will be saved locally to disk at %r at intervals of %s seconds.",
            os.path.join(packet_reader.output_directory, packet_reader.session_subdirectory),
            window_size,
        )

    # Start packet reader in a separate thread so commands can be sent to it in real time in interactive mode or by a
    # routine.
    reader_thread = threading.Thread(target=packet_reader.read_packets, args=(serial_port,), daemon=True)
    reader_thread.start()

    try:
        if interactive:
            # Keep a record of the commands given.
            commands_record_file = os.path.join(
                packet_reader.output_directory, packet_reader.session_subdirectory, "commands.txt"
            )

            os.makedirs(os.path.join(packet_reader.output_directory, packet_reader.session_subdirectory), exist_ok=True)

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

        else:
            if routine is not None:
                routine.run()

    except KeyboardInterrupt:
        packet_reader.stop = True

    logger.info("Stopping gateway.")
    packet_reader.writer.force_persist()


@gateway_cli.command()
@click.option(
    "--config-file",
    type=click.Path(),
    default="configuration.json",
    help="A path to a JSON configuration file.",
)
def create_installation(config_file):
    """Create an installation representing a collection of sensors that data can be collected from. The installation
    information is read from the "installation_data" field of `configuration.json`.
    """
    with open(config_file or "configuration.json") as f:
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
            raise WrongNumberOfSensorCoordinatesError(
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


def _load_configuration(configuration_path):
    """Load a configuration from the path if it exists, otherwise load the default configuration.

    :param str configuration_path:
    :return data_gateway.configuration.Configuration:
    """
    if os.path.exists(configuration_path):
        with open(configuration_path) as f:
            configuration = Configuration.from_dict(json.load(f))

        logger.info("Loaded configuration file from %r.", configuration_path)
        return configuration

    configuration = Configuration()
    logger.info("No configuration file provided - using default configuration.")
    return configuration


def _get_serial_port(serial_port, configuration, use_dummy_serial_port):
    """Get the serial port or a dummy serial port if specified.

    :param str serial_port:
    :param data_gateway.configuration.Configuration configuration:
    :param bool use_dummy_serial_port:
    :return serial.Serial:
    """
    if not use_dummy_serial_port:
        serial_port = serial.Serial(port=serial_port, baudrate=configuration.baudrate)
    else:
        serial_port = DummySerial(port=serial_port, baudrate=configuration.baudrate)

    # The buffer size can only be set on Windows.
    if os.name == "nt":
        serial_port.set_buffer_size(
            rx_size=configuration.serial_buffer_rx_size,
            tx_size=configuration.serial_buffer_tx_size,
        )
    else:
        logger.warning("Serial port buffer size can only be set on Windows.")

    return serial_port


def _load_routine(routine_path, interactive, serial_port):
    """Load a sensor commands routine from the path if exists, otherwise return no routine. If in interactive mode, the
    routine file is ignored. Note that "\n" has to be added to the end of each command sent to the serial port for it to
    be executed - this is done automatically in this method.

    :param str routine_path:
    :param bool interactive:
    :param serial.Serial serial_port:
    :return data_gateway.routine.Routine|None:
    """
    if os.path.exists(routine_path):
        if interactive:
            logger.warning("Sensor command routine files are ignored in interactive mode.")
            return
        else:
            with open(routine_path) as f:
                routine = Routine(
                    **json.load(f),
                    action=lambda command: serial_port.write((command + "\n").encode("utf_8")),
                )

            logger.info("Loaded routine file from %r.", routine_path)
            return routine

    logger.info(
        "No routine file found at %r - no commands will be sent to the sensors unless given in interactive mode.",
        routine_path,
    )


def _update_and_create_output_directory(output_directory_path):
    """Set the output directory to a path relative to the current directory if the path does not start with "/" and
    create it if it does not already exist.

    :param str output_directory_path:
    :return str:
    """
    if not output_directory_path.startswith("/"):
        output_directory_path = os.path.join(".", output_directory_path)

    os.makedirs(output_directory_path, exist_ok=True)
    return output_directory_path


if __name__ == "__main__":
    gateway_cli()
