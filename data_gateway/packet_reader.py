import datetime
import json
import multiprocessing
import os
import queue
import struct

from octue.cloud import storage
from octue.log_handlers import apply_log_handler

from data_gateway import MICROPHONE_SENSOR_NAME, exceptions
from data_gateway.configuration import Configuration
from data_gateway.persistence import BatchingFileWriter, BatchingUploader, NoOperationContextManager


logger = multiprocessing.get_logger()
apply_log_handler(logger=logger)


class PacketReader:
    """A serial port packet reader.

    :param bool save_locally: save data windows locally
    :param bool upload_to_cloud: upload data windows to Google cloud
    :param str|None output_directory: the directory in which to save data in the cloud bucket or local file system
    :param float window_size: the period in seconds at which data is persisted.
    :param str|None project_name: name of Google Cloud project to upload to
    :param str|None bucket_name: name of Google Cloud bucket to upload to
    :param data_gateway.configuration.Configuration|None configuration: the configuration for reading and parsing data
    :param bool save_csv_files: save sensor data to .csv when in interactive mode
    :return None:
    """

    def __init__(
        self,
        save_locally,
        upload_to_cloud,
        output_directory=None,
        window_size=600,
        project_name=None,
        bucket_name=None,
        configuration=None,
        save_csv_files=False,
    ):
        self.save_locally = save_locally
        self.upload_to_cloud = upload_to_cloud
        self.output_directory = output_directory
        self.window_size = window_size
        self.project_name = project_name
        self.bucket_name = bucket_name
        self.config = configuration or Configuration()
        self.save_csv_files = save_csv_files

        self.uploader = None
        self.writer = None
        self.handles = self.config.default_handles
        self.sleep = False
        self.sensor_time_offset = None
        self.session_subdirectory = str(hash(datetime.datetime.now()))[1:7]

        os.makedirs(os.path.join(output_directory, self.session_subdirectory), exist_ok=True)
        logger.warning("Timestamp synchronisation unavailable with current hardware; defaulting to using system clock.")

    def read_packets(self, serial_port, packet_queue, error_queue, stop_signal, stop_when_no_more_data=False):
        """Read packets from a serial port and send them to the parser thread for processing and persistence.

        :param serial.Serial serial_port: name of serial port to read from
        :param queue.Queue packet_queue: a thread-safe queue to put packets on to for the parser thread to pick up
        :param queue.Queue error_queue: a thread-safe queue to put any exceptions on to for the main thread to handle
        :param bool stop_when_no_more_data: if `True`, stop reading when no more data is received from the port (for testing)
        :return None:
        """
        try:
            logger.info("Beginning reading packets from serial port.")

            previous_timestamp = {}
            data = {}

            for sensor_name in self.config.sensor_names:
                previous_timestamp[sensor_name] = -1
                data[sensor_name] = [
                    ([0] * self.config.samples_per_packet[sensor_name])
                    for _ in range(self.config.number_of_sensors[sensor_name])
                ]

            while stop_signal.value == 0:
                serial_data = serial_port.read()

                if len(serial_data) == 0:
                    if stop_when_no_more_data:
                        logger.info("Sending stop signal.")
                        stop_signal.value = 1
                        break
                    continue

                if serial_data[0] != self.config.packet_key:
                    continue

                packet_type = str(int.from_bytes(serial_port.read(), self.config.endian))
                length = int.from_bytes(serial_port.read(), self.config.endian)
                payload = serial_port.read(length)
                logger.info("Read packet from serial port.")

                if packet_type == str(self.config.type_handle_def):
                    self.update_handles(payload)
                    continue

                # Check for bytes in serial input buffer. A full buffer results in overflow.
                if serial_port.in_waiting == self.config.serial_buffer_rx_size:
                    logger.warning(
                        "Buffer is full: %d bytes waiting. Re-opening serial port, to avoid overflow",
                        serial_port.in_waiting,
                    )
                    serial_port.close()
                    serial_port.open()
                    continue

                packet_queue.put(
                    {
                        "packet_type": packet_type,
                        "payload": payload,
                        "data": data,
                        "previous_timestamp": previous_timestamp,
                    }
                )

        except Exception as e:
            error_queue.put(e)
            logger.info("Sending stop signal.")
            stop_signal.value = 1

    def parse_packets(self, packet_queue, error_queue, stop_signal):
        """Get packets from a thread-safe packet queue, check if a full payload has been received (i.e. correct length)
        with the correct packet type handle, then parse the payload. After parsing/processing, upload them to Google
        Cloud storage and/or write them to disk. If any errors are raised, put them on the error queue for the main
        thread to handle.

        :param queue.Queue packet_queue: a thread-safe queue of packets provided by a reader thread
        :param queue.Queue error_queue: a thread-safe queue to put any exceptions on to for the main thread to handle
        :return None:
        """
        logger.info("Beginning parsing packets from serial port.")

        if self.upload_to_cloud:
            self.uploader = BatchingUploader(
                sensor_names=self.config.sensor_names,
                project_name=self.project_name,
                bucket_name=self.bucket_name,
                window_size=self.window_size,
                session_subdirectory=self.session_subdirectory,
                output_directory=self.output_directory,
                metadata={"data_gateway__configuration": self.config.to_dict()},
            )
        else:
            self.uploader = NoOperationContextManager()

        if self.save_locally:
            self.writer = BatchingFileWriter(
                sensor_names=self.config.sensor_names,
                window_size=self.window_size,
                session_subdirectory=self.session_subdirectory,
                output_directory=self.output_directory,
                save_csv_files=self.save_csv_files,
            )
        else:
            self.writer = NoOperationContextManager()

        try:
            with self.uploader:
                with self.writer:
                    while stop_signal.value == 0:
                        packet_type, payload, data, previous_timestamp = packet_queue.get(timeout=1).values()
                        logger.info("Received packet for parsing.")

                        if packet_type not in self.handles:
                            logger.error("Received packet with unknown type: %s", packet_type)
                            continue

                        if len(payload) == 244:  # If the full data payload is received, proceed parsing it
                            timestamp = int.from_bytes(payload[240:244], self.config.endian, signed=False) / (2 ** 16)

                            data, sensor_names = self._parse_sensor_packet_data(
                                self.handles[packet_type], payload, data
                            )

                            for sensor_name in sensor_names:
                                self._check_for_packet_loss(sensor_name, timestamp, previous_timestamp)
                                self._timestamp_and_persist_data(
                                    data, sensor_name, timestamp, self.config.period[sensor_name]
                                )

                        elif len(payload) >= 1 and self.handles[packet_type] in [
                            "Mic 1",
                            "Cmd Decline",
                            "Sleep State",
                            "Info Message",
                        ]:
                            self._parse_info_packet(self.handles[packet_type], payload)

        except queue.Empty:
            pass

        except Exception as e:
            error_queue.put(e)

        finally:
            logger.info("Sending stop signal.")
            stop_signal.value = 1

    def update_handles(self, payload):
        """Update the Bluetooth handles object. Handles are updated every time a new Bluetooth connection is
        established.

        :param iter payload:
        :return None:
        """
        start_handle = int.from_bytes(payload[0:1], self.config.endian)
        end_handle = int.from_bytes(payload[2:3], self.config.endian)

        if end_handle - start_handle == 26:
            self.handles = {
                str(start_handle + 2): "Abs. baros",
                str(start_handle + 4): "Diff. baros",
                str(start_handle + 6): "Mic 0",
                str(start_handle + 8): "Mic 1",
                str(start_handle + 10): "IMU Accel",
                str(start_handle + 12): "IMU Gyro",
                str(start_handle + 14): "IMU Magnetometer",
                str(start_handle + 16): "Analog1",
                str(start_handle + 18): "Analog2",
                str(start_handle + 20): "Constat",
                str(start_handle + 22): "Cmd Decline",
                str(start_handle + 24): "Sleep State",
                str(start_handle + 26): "Info message",
            }

            logger.info("Successfully updated handles.")
            return

        logger.error("Handle error: %s %s", start_handle, end_handle)

    def persist_configuration(self):
        """Persist the configuration to disk and/or cloud storage.

        :return None:
        """
        configuration_dictionary = self.config.to_dict()

        if self.save_locally:
            with open(
                os.path.abspath(os.path.join(self.output_directory, self.session_subdirectory, "configuration.json")),
                "w",
            ) as f:
                json.dump(configuration_dictionary, f)

        if self.upload_to_cloud:
            self.uploader.client.upload_from_string(
                string=json.dumps(configuration_dictionary),
                bucket_name=self.uploader.bucket_name,
                path_in_bucket=storage.path.join(
                    self.output_directory, self.session_subdirectory, "configuration.json"
                ),
            )

    def _parse_sensor_packet_data(self, packet_type, payload, data):
        """Parse sensor data type payloads.

        :param str packet_type: Type of the packet
        :param iter payload: Raw payload to be parsed
        :param dict data: Initialised data dict to be completed with parsed data
        :return dict data:
        """
        if packet_type == "Abs. baros":
            # Write the received payload to the data field
            # TODO bytes_per_sample should probably be in the configuration
            bytes_per_sample = 6
            for i in range(self.config.baros_samples_per_packet):
                for j in range(self.config.number_of_sensors["Baros_P"]):
                    data["Baros_P"][j][i] = int.from_bytes(
                        payload[(bytes_per_sample * j) : (bytes_per_sample * j + 4)],
                        self.config.endian,
                        signed=False,
                    )

                    data["Baros_T"][j][i] = int.from_bytes(
                        payload[(bytes_per_sample * j + 4) : (bytes_per_sample * j + 6)],
                        self.config.endian,
                        signed=True,
                    )

            return data, ["Baros_P", "Baros_T"]

        if packet_type == "Diff. baros":
            bytes_per_sample = 2
            for i in range(self.config.diff_baros_samples_per_packet):
                for j in range(self.config.number_of_sensors["Diff_Baros"]):
                    data["Diff_Baros"][j][i] = int.from_bytes(
                        payload[
                            (bytes_per_sample * (self.config.number_of_sensors["Diff_Baros"] * i + j)) : (
                                bytes_per_sample * (self.config.number_of_sensors["Diff_Baros"] * i + j + 1)
                            )
                        ],
                        self.config.endian,
                        signed=False,
                    )

            return data, ["Diff_Baros"]

        if packet_type == "Mic 0":
            # Write the received payload to the data field
            bytes_per_sample = 3

            for i in range(self.config.mics_samples_per_packet // 2):
                for j in range(self.config.number_of_sensors[MICROPHONE_SENSOR_NAME] // 2):

                    index = j + 20 * i

                    data[MICROPHONE_SENSOR_NAME][j][2 * i] = int.from_bytes(
                        payload[(bytes_per_sample * index) : (bytes_per_sample * index + 3)],
                        "big",  # Unlike the other sensors, the microphone data come in big-endian
                        signed=True,
                    )
                    data[MICROPHONE_SENSOR_NAME][j][2 * i + 1] = int.from_bytes(
                        payload[(bytes_per_sample * (index + 5)) : (bytes_per_sample * (index + 5) + 3)],
                        "big",  # Unlike the other sensors, the microphone data come in big-endian
                        signed=True,
                    )
                    data[MICROPHONE_SENSOR_NAME][j + 5][2 * i] = int.from_bytes(
                        payload[(bytes_per_sample * (index + 10)) : (bytes_per_sample * (index + 10) + 3)],
                        "big",  # Unlike the other sensors, the microphone data come in big-endian
                        signed=True,
                    )
                    data[MICROPHONE_SENSOR_NAME][j + 5][2 * i + 1] = int.from_bytes(
                        payload[(bytes_per_sample * (index + 15)) : (bytes_per_sample * (index + 15) + 3)],
                        "big",  # Unlike the other sensors, the microphone data come in big-endian
                        signed=True,
                    )

            return data, [MICROPHONE_SENSOR_NAME]

        if packet_type.startswith("IMU"):

            imu_sensor_names = {"IMU Accel": "Acc", "IMU Gyro": "Gyro", "IMU Magnetometer": "Mag"}

            imu_sensor = imu_sensor_names[packet_type]

            # Write the received payload to the data field
            for i in range(self.config.imu_samples_per_packet):
                index = 6 * i

                data[imu_sensor][0][i] = int.from_bytes(payload[index : (index + 2)], self.config.endian, signed=True)
                data[imu_sensor][1][i] = int.from_bytes(
                    payload[(index + 2) : (index + 4)], self.config.endian, signed=True
                )
                data[imu_sensor][2][i] = int.from_bytes(
                    payload[(index + 4) : (index + 6)], self.config.endian, signed=True
                )

            return data, [imu_sensor]

        # TODO Analog sensor definitions
        if packet_type in {"Analog Kinetron", "Analog1", "Analog2"}:
            logger.error("Received Analog packet. Not supported atm")
            raise exceptions.UnknownPacketTypeError(f"Packet of type {packet_type!r} is unknown.")

        if packet_type == "Analog Vbat":

            def val_to_v(val):
                return val / 1e6

            for i in range(self.config.analog_samples_per_packet):
                index = 4 * i

                data["Analog Vbat"][0][i] = val_to_v(
                    int.from_bytes(payload[index : (index + 4)], self.config.endian, signed=False)
                )

            return data, ["Analog Vbat"]

        if packet_type == "Constat":
            bytes_per_sample = 10
            for i in range(self.config.constat_samples_per_packet):
                data["Constat"][0][i] = struct.unpack(
                    "<f" if self.config.endian == "little" else ">f",
                    payload[(bytes_per_sample * i) : (bytes_per_sample * i + 4)],
                )[0]
                data["Constat"][1][i] = int.from_bytes(
                    payload[(bytes_per_sample * i + 4) : (bytes_per_sample * i + 5)],
                    self.config.endian,
                    signed=True,
                )
                data["Constat"][2][i] = int.from_bytes(
                    payload[(bytes_per_sample * i + 5) : (bytes_per_sample * i + 6)],
                    self.config.endian,
                    signed=True,
                )
                data["Constat"][3][i] = int.from_bytes(
                    payload[(bytes_per_sample * i + 6) : (bytes_per_sample * i + 10)],
                    self.config.endian,
                    signed=False,
                )

            return data, ["Constat"]

        else:  # if packet_type not in self.handles
            logger.error("Sensor of type %r is unknown.", packet_type)
            raise exceptions.UnknownPacketTypeError(f"Sensor of type {packet_type!r} is unknown.")

    def _parse_info_packet(self, information_type, payload):
        """Parse information type packet and send the information to logger.

        :param str information_type: From packet handles, defines what information is stored in payload.
        :param iter payload:
        :return None:
        """
        if information_type == "Mic 1":
            if payload[0] == 1:
                logger.info("Microphone data reading done")
            elif payload[0] == 2:
                logger.info("Microphone data erasing done")
            elif payload[0] == 3:
                logger.info("Microphones started ")

        elif information_type == "Cmd Decline":
            reason_index = str(int.from_bytes(payload, self.config.endian, signed=False))
            logger.info("Command declined, %s", self.config.decline_reason[reason_index])

        elif information_type == "Sleep State":
            state_index = str(int.from_bytes(payload, self.config.endian, signed=False))
            logger.info("\n%s\n", self.config.sleep_state[state_index])
            self.sleep = bool(int(state_index))

        elif information_type == "Info Message":
            info_index = str(int.from_bytes(payload[0:1], self.config.endian, signed=False))
            logger.info(self.config.info_type[info_index])

            if self.config.info_type[info_index] == "Battery info":
                voltage = int.from_bytes(payload[1:5], self.config.endian, signed=False)
                cycle = int.from_bytes(payload[5:9], self.config.endian, signed=False)
                state_of_charge = int.from_bytes(payload[9:13], self.config.endian, signed=False)

                logger.info(
                    "Voltage : %fV\n Cycle count: %f\nState of charge: %f%%",
                    voltage / 1000000,
                    cycle / 100,
                    state_of_charge / 256,
                )

    def _check_for_packet_loss(self, sensor_name, timestamp, previous_timestamp):
        """Check if a packet was lost by looking at the time interval between previous_timestamp and timestamp for
        the sensor_name.

        The sensor data arrives in packets that contain n samples from some sensors of the same type, e.g. one barometer
        packet contains 40 samples from 4 barometers each. Timestamp arrives once per packet. The difference between
        timestamps in two consecutive packets is expected to be approximately equal to the number of samples in the
        packet times sampling period.

        :param str sensor_name:
        :param float timestamp: Current timestamp for the first sample in the packet Unit: s
        :param dict previous_timestamp: Timestamp for the first sample in the previous packet. Must be initialized with -1. Unit: s
        :return None:
        """
        if self.sleep:
            # During sleep, there are no new packets coming in.
            # TODO Make previous_timestamp an attribute, move this to information packet parser and perform on wake-up
            for sensor_name in self.config.sensor_names:
                previous_timestamp[sensor_name] = -1
            return

        if previous_timestamp[sensor_name] == -1:
            logger.info("Received first %s packet" % sensor_name)
        else:
            expected_current_timestamp = (
                previous_timestamp[sensor_name]
                + self.config.samples_per_packet[sensor_name] * self.config.period[sensor_name]
            )
            timestamp_deviation = timestamp - expected_current_timestamp

            if abs(timestamp_deviation) > self.config.max_timestamp_slack:
                logger.warning(
                    "Possible packet loss. %s sensor packet is timestamped %d ms later than expected",
                    sensor_name,
                    timestamp_deviation * 1000,
                )

                if sensor_name in ["Acc", "Gyro", "Mag"]:
                    # IMU sensors are not synchronised to CPU, so their actual periods might differ
                    self.config.period[sensor_name] = (
                        timestamp - previous_timestamp[sensor_name]
                    ) / self.config.samples_per_packet[sensor_name]
                    logger.debug("Updated %s period to %f ms.", sensor_name, self.config.period[sensor_name] * 1000)

        previous_timestamp[sensor_name] = timestamp

    def _timestamp_and_persist_data(self, data, sensor_name, timestamp, period):
        """Persist data to the required storage media.
        Since timestamps only come at a packet level, this function assumes constant period for
         the within-packet-timestamps

        :param dict data: data to persist
        :param str sensor_name: sensor type to persist data from
        :param float timestamp: timestamp in s
        :param float period:
        :return None:
        """
        number_of_samples = len(data[sensor_name][0])
        time = None

        # Iterate through all sample times.
        for i in range(number_of_samples):
            time = timestamp + i * period
            sample = [time]

            for meas in data[sensor_name]:
                sample.append(meas[i])

            self._add_data_to_current_window(sensor_name, data=sample)

        # The first time this method runs, calculate the offset between the last timestamp of the first sample and the
        # UTC time now. Store it as the `start_timestamp` metadata in the windows.
        if sensor_name == "Constat":
            logger.debug("Constat packet: %d" % timestamp)
            if time and self.sensor_time_offset is None:
                self._calculate_and_store_sensor_timestamp_offset(time)

    def _calculate_and_store_sensor_timestamp_offset(self, timestamp):
        """Calculate the offset between the given timestamp and the UTC time now, storing it in the metadata of the
        windows in the uploader and/or writer.

        :param float timestamp: posix timestamp from sensor
        :return None:
        """
        now = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).timestamp()
        self.sensor_time_offset = now - timestamp

        if hasattr(self.writer, "current_window"):
            self.writer.current_window["sensor_time_offset"] = self.sensor_time_offset
            self.writer.ready_window["sensor_time_offset"] = self.sensor_time_offset

        if hasattr(self.uploader, "current_window"):
            self.uploader.current_window["sensor_time_offset"] = self.sensor_time_offset
            self.uploader.ready_window["sensor_time_offset"] = self.sensor_time_offset

    def _add_data_to_current_window(self, sensor_name, data):
        """Add data to the current window.

        :param str sensor_name: sensor type to persist data from
        :param iter data: data to persist
        :return None:
        """
        if self.save_locally:
            self.writer.add_to_current_window(sensor_name, data)

        if self.upload_to_cloud:
            self.uploader.add_to_current_window(sensor_name, data)
