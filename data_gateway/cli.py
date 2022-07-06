import multiprocessing
import os

import click
import pkg_resources
import requests
from slugify import slugify


SUPERVISORD_PROGRAM_NAME = "AerosenseGateway"
CREATE_INSTALLATION_CLOUD_FUNCTION_URL = "https://europe-west6-aerosense-twined.cloudfunctions.net/create-installation"
ADD_SENSOR_TYPE_CLOUD_FUNCTION_URL = "https://europe-west6-aerosense-twined.cloudfunctions.net/add-sensor-type"

global_cli_context = {}

logger = multiprocessing.get_logger()


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
    """Enter the Aerosense Gateway CLI. Run the on-tower gateway service to read data from the bluetooth receivers and
    send it to Aerosense Cloud.
    """
    # Store log level to apply to multi-processed logger in `DataGateway` in the `start` command.
    global_cli_context["log_level"] = log_level.upper()

    # Stream logs to remote handler if required.
    if logger_uri is not None:
        from octue.log_handlers import apply_log_handler, get_remote_handler

        apply_log_handler(
            logger=logger,
            handler=get_remote_handler(logger_uri=logger_uri),
            log_level=global_cli_context["log_level"],
            include_process_name=True,
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
    help="Path to your Aerosense deployment configuration JSON file. This value is overridden by the environment variable GATEWAY_CONFIG_FILE if set",
)
@click.option(
    "--routine-file",
    type=click.Path(dir_okay=False),
    default="routine.json",
    show_default=True,
    help="Path to sensor command routine JSON file. This value is overridden by the environment variable GATEWAY_ROUTINE_FILE if set",
)
@click.option(
    "--stop-routine-file",
    type=click.Path(dir_okay=False),
    default="stop_routine.json",
    show_default=True,
    help="Path to sensor command routine JSON file to be executed on exit of the gateway loop (i.e. a routine which will shut down the sensors after running the gateway). This value is overridden by the environment variable GATEWAY_ROUTINE_FILE if set",
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
    help="The directory in which to save data windows from the gateway. This value is overridden by the environment variable GATEWAY_OUTPUT_DIR if set",
)
@click.option(
    "--window-size",
    type=click.FLOAT,
    default=600,
    show_default=True,
    help="The window length in seconds that data is grouped into before being persisted locally or to the cloud.",
)
@click.option(
    "--gcp-bucket-name",
    type=click.STRING,
    default=None,
    show_default=True,
    help="The name of the Google Cloud Platform (GCP) storage bucket to use. This value is overridden by the environment variable GATEWAY_GCP_BUCKET_NAME if set.",
)
@click.option(
    "--label",
    type=str,
    default=None,
    show_default=True,
    help="An optional label to associate with data persisted in this run of the gateway.",
)
@click.option(
    "--no-stop-sensors-on-exit",
    is_flag=True,
    default=False,
    help="Don't stop the sensors on gateway exit.",
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
    stop_routine_file,
    save_locally,
    no_upload_to_cloud,
    interactive,
    output_dir,
    window_size,
    gcp_bucket_name,
    label,
    no_stop_sensors_on_exit,
    save_csv_files,
    use_dummy_serial_port,
):
    """Begin reading and persisting data from the serial port for the sensors at the installation defined in
    `configuration.json`. Daemonise this for a deployment. In interactive mode, commands can be sent to the
    nodes/sensors via the serial port by typing them into stdin and pressing enter. These commands are:
    [startBaros, startMics, startIMU, getBattery, stop].
    """
    from data_gateway.data_gateway import DataGateway

    # Allow override of defaults from the environment
    overridden_config_file = os.environ.get("GATEWAY_CONFIG_FILE", None) or config_file
    overridden_output_dir = os.environ.get("GATEWAY_OUTPUT_DIR", None) or output_dir
    overridden_routine_file = os.environ.get("GATEWAY_ROUTINE_FILE", None) or routine_file
    overridden_stop_routine_file = os.environ.get("GATEWAY_STOP_ROUTINE_FILE", None) or stop_routine_file
    gcp_bucket_name = os.environ.get("GATEWAY_GCP_BUCKET_NAME", None) or gcp_bucket_name

    data_gateway = DataGateway(
        serial_port=serial_port,
        configuration_path=overridden_config_file,
        routine_path=overridden_routine_file,
        stop_routine_path=overridden_stop_routine_file,
        save_locally=save_locally,
        upload_to_cloud=not no_upload_to_cloud,
        interactive=interactive,
        output_directory=overridden_output_dir,
        window_size=window_size,
        bucket_name=gcp_bucket_name,
        label=label,
        save_csv_files=save_csv_files,
        use_dummy_serial_port=use_dummy_serial_port,
        log_level=global_cli_context["log_level"],
        stop_sensors_on_exit=not no_stop_sensors_on_exit,
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
    information is read from the "gateway" field of `configuration.json`.
    """
    import json

    with open(config_file or "configuration.json") as f:
        configuration = json.load(f)

    gateway_configuration = configuration["gateway"]
    slugified_reference = slugify(gateway_configuration["installation_reference"])

    while True:
        user_confirmation = input(f"Create installation with reference {slugified_reference!r}? [Y/n]\n")

        if user_confirmation.upper() == "N":
            return

        if user_confirmation.upper() in {"Y", ""}:
            break

    # Required parameters:
    parameters = {
        "reference": slugified_reference,
        "turbine_id": gateway_configuration["turbine_id"],
        "receiver_firmware_version": gateway_configuration["receiver_firmware_version"],
    }

    # Optional parameters:
    if gateway_configuration.get("longitude"):
        parameters["longitude"] = gateway_configuration["longitude"]

    if gateway_configuration.get("latitude"):
        parameters["latitude"] = gateway_configuration["latitude"]

    print("Creating...")

    response = requests.post(url=CREATE_INSTALLATION_CLOUD_FUNCTION_URL, json=parameters)

    if not response.status_code == 200:
        raise requests.HTTPError(f"{response.status_code}: {response.text}")

    print(f"Installation created: {parameters!r}")


@gateway_cli.command()
@click.argument("name", type=str)
@click.option(
    "--description",
    type=str,
    default=None,
    help="A description of the sensor type.",
)
@click.option(
    "--measuring-unit",
    type=str,
    default=None,
    help="The SI unit that the sensor type measures the relevant quantity in.",
)
@click.option(
    "--metadata",
    type=str,
    default=None,
    help="Other metadata about the sensor type in JSON format.",
)
def add_sensor_type(name, description, measuring_unit, metadata):
    """Add a sensor type to the BigQuery dataset.

    NAME: The name of the sensor type
    """
    reference = slugify(name)

    while True:
        user_confirmation = input(f"Add sensor type with reference {reference!r}? [Y/n]\n")

        if user_confirmation.upper() == "N":
            return

        if user_confirmation.upper() in {"Y", ""}:
            break

    # Required parameters:
    parameters = {"name": name, "reference": reference}

    # Optional parameters:
    for name, parameter in (("description", description), ("measuring_unit", measuring_unit), ("metadata", metadata)):
        if parameter:
            parameters[name] = parameter

    print("Creating...")

    response = requests.post(url=ADD_SENSOR_TYPE_CLOUD_FUNCTION_URL, json=parameters)

    if not response.status_code == 200:
        raise requests.HTTPError(f"{response.status_code}: {response.text}")

    print(f"New sensor type added: {parameters!r}")


@gateway_cli.command()
def supervisord_conf():
    """Print conf entry for use with supervisord. Daemonising a process ensures it automatically restarts after a
    failure and on startup of the operating system failure.
    """
    supervisord_conf_str = f"""

[program:{SUPERVISORD_PROGRAM_NAME,}]
command=gateway start"""

    print(supervisord_conf_str)
    return 0


if __name__ == "__main__":
    gateway_cli()
