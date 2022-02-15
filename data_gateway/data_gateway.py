import json
import multiprocessing
import os
import sys
import threading
import time

import serial
from octue.log_handlers import apply_log_handler

from data_gateway import stop_gateway
from data_gateway.configuration import Configuration
from data_gateway.dummy_serial import DummySerial
from data_gateway.exceptions import DataMustBeSavedError
from data_gateway.packet_reader import PacketReader
from data_gateway.routine import Routine


logger = multiprocessing.get_logger()
apply_log_handler(logger=logger, include_process_name=True)


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
    :param bool save_locally: if `True`, save data windows to disk locally
    :param bool upload_to_cloud: if `True`, upload data windows to Google Cloud Storage
    :param bool interactive: if `True`, allow commands entered into `stdin` to be sent to the sensors in real time
    :param str output_directory: the name of the directory in which to save data in the cloud bucket or local file system
    :param float window_size: the period in seconds at which data is persisted
    :param str|None project_name: the name of the Google Cloud project to upload to
    :param str|None bucket_name: the name of the Google Cloud bucket to upload to
    :param str|None label: a label to be associated with the data collected in this run of the data gateway
    :param bool save_csv_files: if `True`, also save windows locally as CSV files for debugging
    :param bool use_dummy_serial_port: if `True` use a dummy serial port for testing
    :return None:
    """

    def __init__(
        self,
        serial_port,
        configuration_path="config.json",
        routine_path="routine.json",
        save_locally=False,
        upload_to_cloud=True,
        interactive=False,
        output_directory="data_gateway",
        window_size=600,
        project_name=None,
        bucket_name=None,
        label=None,
        save_csv_files=False,
        use_dummy_serial_port=False,
    ):
        if not save_locally and not upload_to_cloud:
            raise DataMustBeSavedError(
                "Data from the gateway must either be saved locally or uploaded to the cloud. Please adjust the "
                "parameters provided."
            )

        self.interactive = interactive

        packet_reader_configuration = self._load_configuration(configuration_path=configuration_path)
        packet_reader_configuration.session_data["label"] = label

        self.serial_port = self._get_serial_port(
            serial_port,
            configuration=packet_reader_configuration,
            use_dummy_serial_port=use_dummy_serial_port,
        )

        self.packet_reader = PacketReader(
            save_locally=save_locally,
            upload_to_cloud=upload_to_cloud,
            output_directory=output_directory,
            window_size=window_size,
            project_name=project_name,
            bucket_name=bucket_name,
            configuration=packet_reader_configuration,
            save_csv_files=save_csv_files,
        )

        self.routine = self._load_routine(routine_path=routine_path)

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
                "serial_port": self.serial_port,
                "packet_queue": packet_queue,
                "stop_signal": stop_signal,
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

        reader_process.start()
        parser_process.start()

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

    def _get_serial_port(self, serial_port, configuration, use_dummy_serial_port):
        """Get the serial port or a dummy serial port if specified. If a serial port instance is provided, return that
        as the serial port to use.

        :param str|serial.Serial serial_port: the name of a serial port or a `serial.Serial` instance
        :param data_gateway.configuration.Configuration configuration: the packet reader configuration
        :param bool use_dummy_serial_port: if `True`, use a dummy serial port instead
        :return serial.Serial|data_gateway.dummy_serial.DummySerial:
        """
        if isinstance(serial_port, str):
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
                logger.debug("Serial port buffer size can only be set on Windows.")

        return serial_port

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
                routine = Routine(
                    **json.load(f),
                    action=lambda command: self.serial_port.write((command + "\n").encode("utf_8")),
                )

            logger.info("Loaded routine file from %r.", routine_path)
            return routine

        if not self.interactive:
            logger.warning(
                "No routine was provided and interactive mode is off - no commands will be sent to the sensors in this "
                "session."
            )

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

                    # The `sleep` command is mainly for facilitating testing.
                    if line.startswith("sleep") and line.endswith("\n"):
                        time.sleep(int(line.split(" ")[-1].strip()))
                        continue

                    if line == "stop\n":
                        self.serial_port.write(line.encode("utf_8"))
                        stop_gateway(logger, stop_signal)
                        break

                    # Send the command to the node.
                    self.serial_port.write(line.encode("utf_8"))

        except Exception as e:
            stop_gateway(logger, stop_signal)
            raise e
