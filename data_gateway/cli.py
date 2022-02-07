import json
import logging
import os

import click
import pkg_resources
import requests
from requests import HTTPError
from slugify import slugify

from data_gateway.data_gateway import DataGateway
from data_gateway.exceptions import WrongNumberOfSensorCoordinatesError


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
    apply_log_handler(log_level=log_level.upper(), include_thread_name=True)

    # Stream logs to remote handler if required.
    if logger_uri is not None:
        apply_log_handler(
            handler=get_remote_handler(logger_uri=logger_uri),
            log_level=log_level.upper(),
            include_thread_name=True,
        )


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
    data_gateway = DataGateway(
        serial_port=serial_port,
        configuration_path=config_file,
        routine_path=routine_file,
        save_locally=save_locally,
        upload_to_cloud=not no_upload_to_cloud,
        interactive=interactive,
        output_directory=output_dir,
        window_size=window_size,
        project_name=gcp_project_name,
        bucket_name=gcp_bucket_name,
        label=label,
        save_csv_files=save_csv_files,
        use_dummy_serial_port=use_dummy_serial_port,
    )

    data_gateway.start()


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


if __name__ == "__main__":
    gateway_cli()
