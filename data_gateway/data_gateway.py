import json
import logging
import os
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import serial

from data_gateway.configuration import Configuration
from data_gateway.dummy_serial import DummySerial
from data_gateway.exceptions import DataMustBeSavedError
from data_gateway.packet_reader import PacketReader
from data_gateway.routine import Routine


logger = logging.getLogger(__name__)


class DataGateway:
    """A class for running the data gateway for wind turbine sensor data. The gateway is run with multiple threads
    reading from the serial port which put the packets they read into a queue for a single parser thread to process and
    persist. An additional thread is run for sending commands to the sensors either interactively or via a routine. If
    a "stop" signal is sent as a command, all threads are stopped and any data in the current window is persisted.

    :param str|serial.Serial serial_port: the name of the serial port to use or a `serial.Serial` instance
    :param str configuration_path: the path to a JSON configuration file for reading and parsing data
    :param str routine_path: the path to a JSON routine file containing sensor commands to be run automatically
    :param bool save_locally: if `True`, save data windows locally
    :param bool upload_to_cloud: if `True`, upload data windows to Google cloud
    :param bool interactive: if `True`, allow commands to be sent to the sensors automatically
    :param str output_directory: the directory in which to save data in the cloud bucket or local file system
    :param float window_size: the period in seconds at which data is persisted
    :param str|None project_name: the name of Google Cloud project to upload to
    :param str|None bucket_name: the name of Google Cloud bucket to upload to
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
                "Data from the gateway must either be saved locally or uploaded to the cloud. Please adjust the CLI "
                "options provided."
            )

        self.interactive = interactive

        packet_reader_configuration = self._load_configuration(configuration_path=configuration_path)
        packet_reader_configuration.session_data["label"] = label

        self.serial_port = self._get_serial_port(
            serial_port,
            configuration=packet_reader_configuration,
            use_dummy_serial_port=use_dummy_serial_port,
        )

        # Start a new thread to parse the serial data while the main thread stays ready to take in commands from stdin.
        self.packet_reader = PacketReader(
            save_locally=save_locally,
            upload_to_cloud=upload_to_cloud,
            output_directory=self._update_and_create_output_directory(output_directory_path=output_directory),
            window_size=window_size,
            project_name=project_name,
            bucket_name=bucket_name,
            configuration=packet_reader_configuration,
            save_csv_files=save_csv_files,
        )

        self.routine = self._load_routine(routine_path=routine_path)

    def start(self, stop_when_no_more_data=False):
        """Begin reading and persisting data from the serial port for the sensors at the installation defined in
        the configuration. In interactive mode, commands can be sent to the nodes/sensors via the serial port by typing
        them into stdin and pressing enter. These commands are: [startBaros, startMics, startIMU, getBattery, stop].

        :return None:
        """
        logger.info("Starting packet reader.")

        if self.packet_reader.upload_to_cloud:
            logger.info(
                "Files will be uploaded to cloud storage at intervals of %s seconds.", self.packet_reader.window_size
            )

        if self.packet_reader.save_locally:
            logger.info(
                "Files will be saved locally to disk at %r at intervals of %s seconds.",
                os.path.join(self.packet_reader.output_directory, self.packet_reader.session_subdirectory),
                self.packet_reader.window_size,
            )

        self.packet_reader.persist_configuration()

        reader_thread_pool = ThreadPoolExecutor(thread_name_prefix="ReaderThread")
        packet_queue = queue.Queue()
        error_queue = queue.Queue()

        try:
            for _ in range(reader_thread_pool._max_workers):
                reader_thread_pool.submit(
                    self.packet_reader.read_packets,
                    self.serial_port,
                    packet_queue,
                    error_queue,
                    stop_when_no_more_data,
                )

            parser_thread = threading.Thread(
                name="ParserThread",
                target=self.packet_reader.parse_payload,
                kwargs={"packet_queue": packet_queue, "error_queue": error_queue},
                daemon=True,
            )

            parser_thread.start()

            if self.interactive:
                interactive_commands_thread = threading.Thread(
                    name="InteractiveCommandsThread",
                    target=self._send_commands_to_sensors,
                    daemon=True,
                )

                interactive_commands_thread.start()

            elif self.routine is not None:
                routine_thread = threading.Thread(name="RoutineCommandsThread", target=self.routine.run, daemon=True)
                routine_thread.start()

            while not self.packet_reader.stop:
                if not error_queue.empty():
                    raise error_queue.get()

        except KeyboardInterrupt:
            pass

        finally:
            logger.info("Stopping gateway.")
            self.packet_reader.stop = True
            reader_thread_pool.shutdown(wait=False)
            self.packet_reader.writer.force_persist()
            self.packet_reader.uploader.force_persist()

    def _load_configuration(self, configuration_path):
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

    def _get_serial_port(self, serial_port, configuration, use_dummy_serial_port):
        """Get the serial port or a dummy serial port if specified.

        :param str|serial.Serial serial_port:
        :param data_gateway.configuration.Configuration configuration:
        :param bool use_dummy_serial_port:
        :return serial.Serial:
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
                logger.warning("Serial port buffer size can only be set on Windows.")

        return serial_port

    def _load_routine(self, routine_path):
        """Load a sensor commands routine from the path if exists, otherwise return no routine. If in interactive mode,
        the routine file is ignored. Note that "\n" has to be added to the end of each command sent to the serial port
        for it to be executed - this is done automatically in this method.

        :param str routine_path:
        :return data_gateway.routine.Routine|None:
        """
        if os.path.exists(routine_path):
            if self.interactive:
                logger.warning("Sensor command routine files are ignored in interactive mode.")
                return
            else:
                with open(routine_path) as f:
                    routine = Routine(
                        **json.load(f),
                        action=lambda command: self.serial_port.write((command + "\n").encode("utf_8")),
                        packet_reader=self.packet_reader,
                    )

                logger.info("Loaded routine file from %r.", routine_path)
                return routine

        logger.info(
            "No routine file found at %r - no commands will be sent to the sensors unless given in interactive mode.",
            routine_path,
        )

    def _update_and_create_output_directory(self, output_directory_path):
        """Set the output directory to a path relative to the current directory if the path does not start with "/" and
        create it if it does not already exist.

        :param str output_directory_path:
        :return str:
        """
        if not output_directory_path.startswith("/"):
            output_directory_path = os.path.join(".", output_directory_path)

        os.makedirs(output_directory_path, exist_ok=True)
        return output_directory_path

    def _send_commands_to_sensors(self):
        """Send commands from `stdin` to the sensors until the "stop" command is received or the packet reader is
        otherwise stopped. A record is kept of the commands sent to the sensors as a text file in the session
        subdirectory. Available commands: [startBaros, startMics, startIMU, getBattery, stop].

        :return None:
        """
        commands_record_file = os.path.join(
            self.packet_reader.output_directory,
            self.packet_reader.session_subdirectory,
            "commands.txt",
        )

        os.makedirs(
            os.path.join(self.packet_reader.output_directory, self.packet_reader.session_subdirectory),
            exist_ok=True,
        )

        while not self.packet_reader.stop:
            for line in sys.stdin:
                with open(commands_record_file, "a") as f:
                    f.write(line)

                if line.startswith("sleep") and line.endswith("\n"):
                    time.sleep(int(line.split(" ")[-1].strip()))
                elif line == "stop\n":
                    self.packet_reader.stop = True

                # Send the command to the node
                self.serial_port.write(line.encode("utf_8"))
