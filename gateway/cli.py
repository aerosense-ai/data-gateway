import logging
import os
import click
import pkg_resources
from octue.logging_handlers import get_remote_handler

import sys


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
@click.version_option(version=pkg_resources.get_distribution("gateway").version)
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
    """Start the gateway service (daemonise this for a deployment)"""
    while True:
        logger.info("Do Stuff with", config_file)
    return 0


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
