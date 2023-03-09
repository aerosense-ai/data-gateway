import datetime
import json
import logging
import multiprocessing
import os
import re
import sys
import threading
import time

import coolname
from octue.log_handlers import apply_log_handler

from data_gateway import stop_gateway
from data_gateway.configuration import Configuration
from data_gateway.exceptions import DataMustBeSavedError
from data_gateway.packet_reader import PacketReader
from data_gateway.routine import Routine
from data_gateway.serial_port import get_serial_port


logger = multiprocessing.get_logger()
apply_log_handler(logger=logger, include_process_name=True)

# Ignore logs from the dummy serial port.
logging.getLogger("data_gateway.dummy_serial.dummy_serial").setLevel(logging.WARNING)


class DataGateway:
    """A class for running the data gateway to collect wind turbine sensor data. The gateway is run as three processes:
    1. The `MainProcess` process, which starts the other two processes and sends commands to the serial port (via a
       separate thread) interactively or through a routine
    2. The `Reader` process, which reads packets from the serial port and puts them on a queue
    3. The `Parser` process, which takes packets off the queue, parses them, and persists them

    All processes and threads are stopped and any data in the current window is persisted if:
    - A "stop" signal is sent as a command interactively or in a routine
    - An error is raised in any process or thread
    - A `KeyboardInterrupt` is raised (i.e. the user presses `Ctrl + C`)
    - No more data is received by the `Parser` process after `stop_when_no_more_data_after` seconds (if it is set in the
      `DataGateway.run` method)

    :param str|serial.Serial serial_port: the name of the serial port or a `serial.Serial` instance to read from
    :param str configuration_path: the path to a JSON configuration file for the packet reader
    :param str routine_path: the path to a JSON routine file containing sensor commands to be run automatically
    :param str stop_routine_path: the path to a JSON routine file containing sensor commands to be run automatically on exiot of the gateway (e.g. safe shutdown)
    :param bool save_locally: if `True`, save data windows to disk locally
    :param bool upload_to_cloud: if `True`, upload data windows to Google Cloud Storage
    :param bool interactive: if `True`, allow commands entered into `stdin` to be sent to the sensors in real time
    :param str output_directory: the name of the directory in which to save data in the cloud bucket or local file system
    :param float window_size: the period in seconds at which data is persisted
    :param str|None bucket_name: the name of the Google Cloud bucket to upload to
    :param bool save_csv_files: if `True`, also save windows locally as CSV files for debugging
    :param bool use_dummy_serial_port: if `True` use a dummy serial port for testing
    :param bool stop_sensors_on_exit: if true, and a `stop_routine_file` path is present, hte stop routine will be executed by the gateway main thread prior to quitting
    :param bool save_local_logs: Add a RotatingFileHandler to write logs to the local file system as well as stdout.
    :return None:
    """

    def __init__(
        self,
        serial_port,
        configuration_path="config.json",
        routine_path="routine.json",
        stop_routine_path="stop_routine.json",
        save_locally=False,
        upload_to_cloud=True,
        interactive=False,
        output_directory="data_gateway",
        window_size=600,
        bucket_name=None,
        save_csv_files=False,
        use_dummy_serial_port=False,
        log_level=logging.INFO,
        stop_sensors_on_exit=True,
        save_local_logs=False,
    ):
        # Set `multiprocessing` logger level.
        logger.setLevel(log_level)
        for handler in logger.handlers:
            handler.setLevel(log_level)

        if not save_locally and not upload_to_cloud:
            raise DataMustBeSavedError(
                "Data from the gateway must either be saved locally or uploaded to the cloud. Please adjust the "
                "parameters provided."
            )

        self.interactive = interactive

        packet_reader_configuration = self._load_configuration(configuration_path=configuration_path)

        self.serial_port_name = serial_port
        self.use_dummy_serial_port = use_dummy_serial_port

        self.serial_port = get_serial_port(
            serial_port,
            configuration=packet_reader_configuration,
            use_dummy_serial_port=self.use_dummy_serial_port,
        )

        self.packet_reader = PacketReader(
            save_locally=save_locally,
            upload_to_cloud=upload_to_cloud,
            output_directory=output_directory,
            window_size=window_size,
            bucket_name=bucket_name,
            configuration=packet_reader_configuration,
            save_csv_files=save_csv_files,
            save_local_logs=save_local_logs,
        )

        self.routine = self._load_routine(routine_path=routine_path)
        self.stop_routine = self._load_routine(routine_path=stop_routine_path)
        self.stop_sensors_on_exit = stop_sensors_on_exit

    def start(self, stop_when_no_more_data_after=False):
        """Begin reading and persisting data from the serial port for the sensors at the installation defined in
        the configuration. In interactive mode, commands can be sent to the nodes/sensors via the serial port by typing
        them into `stdin` and pressing enter. These commands are: [startBaros, startMics, startIMU, getBattery, stop].

        :param float|bool stop_when_no_more_data_after: the number of seconds after receiving no data to stop the gateway (mainly for testing); if `False`, no limit is applied
        :return None:
        """
        packet_queue = multiprocessing.Queue()
        stop_signal = multiprocessing.Value("i", 0)

        reader_process = multiprocessing.Process(
            name="Reader",
            target=self.packet_reader.read_packets,
            kwargs={
                "serial_port_name": self.serial_port_name,
                "packet_queue": packet_queue,
                "stop_signal": stop_signal,
                "use_dummy_serial_port": self.use_dummy_serial_port,
            },
            daemon=True,
        )

        parser_process = multiprocessing.Process(
            name="Parser",
            target=self.packet_reader.parse_packets,
            kwargs={
                "packet_queue": packet_queue,
                "stop_signal": stop_signal,
                "stop_when_no_more_data_after": stop_when_no_more_data_after,
            },
            daemon=True,
        )

        self._add_mandatory_measurement_campaign_metadata()
        reader_process.start()
        parser_process.start()

        try:
            if self.interactive:
                interactive_commands_thread = threading.Thread(
                    name="InteractiveCommandsThread",
                    target=self._send_commands_from_stdin_to_sensors,
                    kwargs={"stop_signal": stop_signal},
                    daemon=True,
                )

                interactive_commands_thread.start()

            elif self.routine is not None:
                routine_thread = threading.Thread(
                    name="RoutineCommandsThread",
                    target=self.routine.run,
                    kwargs={"stop_signal": stop_signal},
                    daemon=True,
                )
                routine_thread.start()

            # Wait for the stop signal before exiting.
            while stop_signal.value == 0:
                time.sleep(5)

        finally:
            self._stop(stop_signal)

    def _load_configuration(self, configuration_path):
        """Load a configuration from the path if it exists; otherwise load the default configuration.

        :param str configuration_path: path to the configuration JSON file
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

    def _load_routine(self, routine_path):
        """Load a sensor commands routine from the path if it exists, otherwise return no routine. If in interactive
        mode, the routine file is ignored. Note that "\n" has to be added to the end of each command sent to the serial
        port for it to be executed - this is done automatically in this method.

        :param str routine_path: the path to the JSON routine file
        :return data_gateway.routine.Routine|None: a sensor routine instance
        """
        if os.path.exists(routine_path):
            if self.interactive:
                logger.warning("Sensor command routine files are ignored in interactive mode.")
                return

            with open(routine_path) as f:
                routine = Routine(**json.load(f), action=self._send_command_to_sensors)

            logger.info("Loaded routine file from %r.", routine_path)
            return routine

        if not self.interactive:
            logger.warning(
                "No routine was provided and interactive mode is off - no commands will be sent to the sensors in this "
                "session."
            )

    def _add_mandatory_measurement_campaign_metadata(self):
        """Add the measurement campaign's start time and the names of the available sensors on each node to the
        configuration. If the configuration doesn't contain a reference for the measurement campaign, a name is
        generated so a new measurement campaign can be created.

        :return None:
        """
        measurement_campaign = self.packet_reader.config.measurement_campaign

        if "reference" not in measurement_campaign:
            measurement_campaign["reference"] = coolname.generate_slug(4)

            logger.info(
                "No measurement campaign reference specified in configuration - creating new campaign called %r.",
                measurement_campaign["reference"],
            )

        measurement_campaign["start_time"] = datetime.datetime.now()

        measurement_campaign["nodes"] = {
            node_id: node.sensor_names for node_id, node in self.packet_reader.config.nodes.items()
        }

    def _send_command_to_sensors(self, command):
        """Send a textual command to the sensors.

        :param str command: the command to send
        :return None:
        """
        self.serial_port.write((command + "\n").encode("utf_8"))
        logger.info("Sent %r command to sensors.", command)

    def _send_commands_from_stdin_to_sensors(self, stop_signal):
        """Send commands from `stdin` to the sensors until the "stop" command is received or the packet reader is
        otherwise stopped. A record is kept of the commands sent to the sensors as a text file in the session
        subdirectory. Available commands: [startBaros, startMics, startIMU, getBattery, stop].

        :return None:
        """
        commands_record_file = os.path.join(self.packet_reader.local_output_directory, "commands.txt")

        try:
            while stop_signal.value == 0:
                for line in sys.stdin:
                    with open(commands_record_file, "a") as f:
                        f.write(line)

                    line = line.strip()

                    # The `sleep` command is mainly for facilitating testing.
                    if re.match(r"sleep\s\d+", line):
                        time.sleep(int(line.split(" ")[-1].strip()))
                        continue

                    self._send_command_to_sensors(line)

                    if line == "stop":
                        stop_gateway(logger, stop_signal)
                        break

        except Exception as e:
            stop_gateway(logger, stop_signal)
            raise e

    def _stop(self, stop_signal):
        """Stop the data gateway.

        :param multiprocessing.Value stop_signal: a value of 0 means don't stop; a value of 1 means stop
        :return None:
        """
        if self.stop_sensors_on_exit:
            if self.stop_routine is not None:
                logger.info(
                    "Safely shutting down sensors using stop_routine. Press ctrl+c again to hard-exit (unsafe!)"
                )
                # Run a thread to execute the stop routine
                routine_thread = threading.Thread(
                    name="RoutineCommandsThread",
                    target=self.stop_routine.run,
                    kwargs={"stop_signal": stop_signal},
                    daemon=True,
                )
                routine_thread.start()
                # Wait a sensible amount of time for the stop signals to flush, then exit
                time.sleep(5)

            else:
                logger.warning("No stop_routine file supplied - sensors cannot be automatically stopped.")
