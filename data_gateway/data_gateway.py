import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import serial

from data_gateway.configuration import Configuration
from data_gateway.dummy_serial import DummySerial
from data_gateway.exceptions import DataMustBeSavedError, PacketReaderStopError
from data_gateway.packet_reader import PacketReader
from data_gateway.routine import Routine


logger = logging.getLogger(__name__)


class DataGateway:
    def __init__(
        self,
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
        if not save_locally and no_upload_to_cloud:
            raise DataMustBeSavedError(
                "Data from the gateway must either be saved locally or uploaded to the cloud. Please adjust the CLI "
                "options provided."
            )

        self.serial_port = serial_port
        self.interactive = interactive
        self.no_upload_to_cloud = no_upload_to_cloud
        self.save_locally = save_locally
        self.window_size = window_size

        packet_reader_configuration = self._load_configuration(configuration_path=config_file)
        packet_reader_configuration.session_data["label"] = label

        self.serial_port = self._get_serial_port(
            serial_port,
            configuration=packet_reader_configuration,
            use_dummy_serial_port=use_dummy_serial_port,
        )

        self.routine = self._load_routine(routine_path=routine_file)

        # Start a new thread to parse the serial data while the main thread stays ready to take in commands from stdin.
        self.packet_reader = PacketReader(
            save_locally=save_locally,
            upload_to_cloud=not no_upload_to_cloud,
            output_directory=self._update_and_create_output_directory(output_directory_path=output_dir),
            window_size=window_size,
            project_name=gcp_project_name,
            bucket_name=gcp_bucket_name,
            configuration=packet_reader_configuration,
            save_csv_files=save_csv_files,
        )

    def start(self):
        """Begin reading and persisting data from the serial port for the sensors at the installation defined in
        the configuration. In interactive mode, commands can be sent to the nodes/sensors via the serial port by typing
        them into stdin and pressing enter. These commands are: [startBaros, startMics, startIMU, getBattery, stop].

        :return None:
        """
        logger.info("Starting packet reader.")

        if not self.no_upload_to_cloud:
            logger.info("Files will be uploaded to cloud storage at intervals of %s seconds.", self.window_size)

        if self.save_locally:
            logger.info(
                "Files will be saved locally to disk at %r at intervals of %s seconds.",
                os.path.join(self.packet_reader.output_directory, self.packet_reader.session_subdirectory),
                self.window_size,
            )

        # Start packet reader in a thread pool for parallelised reading and so commands can be sent to the serial port
        # in real time.
        reader_thread_pool = ThreadPoolExecutor(thread_name_prefix="ReaderThread")

        try:
            for _ in range(reader_thread_pool._max_workers):
                reader_thread_pool.submit(self.packet_reader.read_packets, self.serial_port)

            if self.interactive:
                # Keep a record of the commands given.
                commands_record_file = os.path.join(
                    self.packet_reader.output_directory, self.packet_reader.session_subdirectory, "commands.txt"
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
                            raise PacketReaderStopError()

                        # Send the command to the node
                        self.serial_port.write(line.encode("utf_8"))

            else:
                if self.routine is not None:
                    self.routine.run()

        except (KeyboardInterrupt, PacketReaderStopError):
            pass

        finally:
            logger.info("Stopping gateway.")
            self.packet_reader.stop = True
            reader_thread_pool.shutdown(wait=False)
            self.packet_reader.writer.force_persist()

    def _load_configuration(self, configuration_path):
        """Load a configuration from the path if it exists, otherwise load the default configuration.

        :param str configuration_path:
        :return data_gateway.configuration.Configuration:
        """
        if os.path.exists(configuration_path):
            with open(configuration_path) as f:
                configuration = Configuration.from_dict(json.load(f))

            logger.debug("Loaded configuration file from %r.", configuration_path)
            return configuration

        configuration = Configuration()
        logger.info("No configuration file provided - using default configuration.")
        return configuration

    def _get_serial_port(self, serial_port, configuration, use_dummy_serial_port):
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
