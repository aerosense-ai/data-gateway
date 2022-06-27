import datetime
import json
import multiprocessing
import os
import queue
import struct

from octue.cloud import storage
from octue.log_handlers import apply_log_handler

from data_gateway import exceptions, stop_gateway
from data_gateway.configuration import DEFAULT_SENSOR_NAMES, Configuration
from data_gateway.persistence import (
    DEFAULT_OUTPUT_DIRECTORY,
    BatchingFileWriter,
    BatchingUploader,
    NoOperationContextManager,
)
from data_gateway.serial_port import get_serial_port


logger = multiprocessing.get_logger()
apply_log_handler(logger=logger, include_process_name=True)


class PacketReader:
    """A serial port packet reader. Note that timestamp synchronisation is unavailable with the current sensor hardware
    so the system clock is used instead.

    :param bool save_locally: save data windows locally
    :param bool upload_to_cloud: upload data windows to Google cloud
    :param str|None output_directory: the directory in which to save data in the cloud bucket or local file system
    :param float window_size: the period in seconds at which data is persisted.
    :param str|None bucket_name: name of Google Cloud bucket to upload to
    :param data_gateway.configuration.Configuration|None configuration: the configuration for reading and parsing data
    :param bool save_csv_files: save sensor data to .csv when in interactive mode
    :return None:
    """

    def __init__(
        self,
        save_locally,
        upload_to_cloud,
        output_directory=DEFAULT_OUTPUT_DIRECTORY,
        window_size=600,
        bucket_name=None,
        configuration=None,
        save_csv_files=False,
    ):
        self.save_locally = save_locally
        self.upload_to_cloud = upload_to_cloud
        self.session_subdirectory = str(hash(datetime.datetime.now()))[1:7]

        self.cloud_output_directory = storage.path.join(output_directory, self.session_subdirectory)
        self.local_output_directory = os.path.abspath(os.path.join(output_directory, self.session_subdirectory))
        os.makedirs(self.local_output_directory, exist_ok=True)

        self.window_size = window_size
        self.bucket_name = bucket_name
        self.config = configuration or Configuration()
        self.save_csv_files = save_csv_files

        self.uploader = None
        self.writer = None
        self.handles = dict((k, v.default_handles) for k, v in self.config.nodes.items())
        self.sleep = False
        self.sensor_time_offset = None

    def read_packets(self, serial_port_name, packet_queue, stop_signal, use_dummy_serial_port=False):
        """Read packets from a serial port and send them to the parser thread for processing and persistence.

        :param str serial_port_name: the name of the serial port to read from
        :param queue.Queue packet_queue: a thread-safe queue to put packets on to for the parser thread to pick up
        :param multiprocessing.Value stop_signal: a value of 0 means don't stop; a value of 1 means stop
        :param bool use_dummy_serial_port: if `True` use a dummy serial port for testing
        :return None:
        """
        try:
            logger.info("Packet reader process started.")

            serial_port = get_serial_port(
                serial_port=serial_port_name,
                configuration=self.config,
                use_dummy_serial_port=use_dummy_serial_port,
            )

            while stop_signal.value == 0:
                serial_data = serial_port.read()

                # Handle no data on the serial port.
                if len(serial_data) == 0:
                    continue

                # Get the ID of the node the packet is coming from.
                if serial_data not in self.config.packet_key_map:
                    continue

                node_id = self.config.packet_key_map[serial_data]

                # Read the packet from the serial port.
                packet_type = str(int.from_bytes(serial_port.read(), self.config.gateway.endian))
                length = int.from_bytes(serial_port.read(), self.config.gateway.endian)
                packet = serial_port.read(length)

                # Check for bytes in serial input buffer. A full buffer results in overflow.
                if serial_port.in_waiting == self.config.gateway.serial_buffer_rx_size:
                    logger.warning("Serial port buffer is full - buffer overflow may occur, resulting in data loss.")
                    continue

                packet_queue.put({"node_id": node_id, "packet_type": packet_type, "packet": packet})

        except KeyboardInterrupt:
            pass

        finally:
            stop_gateway(logger, stop_signal)

    def parse_packets(self, packet_queue, stop_signal, stop_when_no_more_data_after=False):
        """Get packets from a thread-safe packet queue, check if a full payload has been received (i.e. correct length)
        with the correct packet type handle, then parse the payload. After parsing/processing, upload them to Google
        Cloud storage and/or write them to disk. If any errors are raised, put them on the error queue for the main
        thread to handle.

        :param queue.Queue packet_queue: a thread-safe queue of packets provided by a reader thread
        :param float|bool stop_when_no_more_data_after: the number of seconds after receiving no data to stop the gateway (mainly for testing); if `False`, no limit is applied
        :return None:
        """
        logger.info("Packet parser process started.")

        if self.upload_to_cloud:
            self.uploader = BatchingUploader(
                node_ids=self.config.node_ids,
                bucket_name=self.bucket_name,
                window_size=self.window_size,
                output_directory=self.cloud_output_directory,
                metadata={"data_gateway__configuration": self.config.to_dict()},
            )
        else:
            self.uploader = NoOperationContextManager()

        if self.save_locally:
            self.writer = BatchingFileWriter(
                node_ids=self.config.node_ids,
                window_size=self.window_size,
                output_directory=self.local_output_directory,
                save_csv_files=self.save_csv_files,
            )

            self._save_configuration_to_disk()

        else:
            self.writer = NoOperationContextManager()

        previous_timestamp = {}
        data = {}

        for node_id in self.config.node_ids:
            node_config = self.config.nodes[node_id]
            data[node_id] = {}
            previous_timestamp[node_id] = {}

            for sensor_name in node_config.sensor_names:
                previous_timestamp[node_id][sensor_name] = -1

                data[node_id][sensor_name] = [
                    ([0] * node_config.samples_per_packet[sensor_name])
                    for _ in range(node_config.number_of_sensors[sensor_name])
                ]

        if stop_when_no_more_data_after is False:
            timeout = 5
        else:
            timeout = stop_when_no_more_data_after

        try:
            with self.uploader:
                with self.writer:
                    while stop_signal.value == 0:
                        try:
                            node_id, packet_type, packet = packet_queue.get(timeout=timeout).values()
                        except queue.Empty:
                            if stop_when_no_more_data_after is not False:
                                break
                            continue

                        if packet_type == str(self.config.nodes[node_id].type_handle_def):
                            logger.warning("Updating handles (not node-specific) for node %s", node_id)
                            self.update_handles(packet, node_id)
                            continue

                        if packet_type not in self.handles[node_id]:
                            logger.error("Received packet with unknown type: %s", packet_type)
                            continue

                        if len(packet) == 244:  # If the full data payload is received, proceed parsing it
                            timestamp = int.from_bytes(packet[240:244], self.config.gateway.endian, signed=False) / (
                                2**16
                            )

                            data, sensor_names = self._parse_sensor_packet_data(
                                node_id=node_id,
                                packet_type=self.handles[node_id][packet_type],
                                payload=packet,
                                data=data,
                            )

                            for sensor_name in sensor_names:
                                self._check_for_packet_loss(
                                    node_id=node_id,
                                    sensor_name=sensor_name,
                                    timestamp=timestamp,
                                    previous_timestamp=previous_timestamp,
                                )

                                self._timestamp_and_persist_data(
                                    data=data,
                                    node_id=node_id,
                                    sensor_name=sensor_name,
                                    timestamp=timestamp,
                                    period=self.config.nodes[node_id].periods[sensor_name],
                                )

                            continue

                        if self.handles[node_id][packet_type] in [
                            "Mic 1",
                            "Cmd Decline",
                            "Sleep State",
                            "Info Message",
                        ]:
                            self._parse_info_packet(
                                node_id=node_id,
                                information_type=self.handles[node_id][packet_type],
                                payload=packet,
                                previous_timestamp=previous_timestamp,
                            )

        except KeyboardInterrupt:
            pass

        finally:
            stop_gateway(logger, stop_signal)

    def update_handles(self, payload, node_id):
        """Update the Bluetooth handles object. Handles are updated every time a new Bluetooth connection is
        established.

        :param iter payload:
        :return None:
        """
        start_handle = int.from_bytes(payload[0:1], self.config.gateway.endian)
        end_handle = int.from_bytes(payload[2:3], self.config.gateway.endian)

        if end_handle - start_handle == 26:
            self.handles[node_id] = {
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

            self.sensor_time_offset = None

            logger.info("Successfully updated handles for node %s", node_id)
            return

        logger.error(
            "Error while updating handles for node %s: start handle is %s, end handle is %s.",
            node_id,
            start_handle,
            end_handle,
        )

    def _save_configuration_to_disk(self):
        """Save the configuration to disk as a JSON file.

        :return None:
        """
        with open(os.path.join(self.local_output_directory, "configuration.json"), "w") as f:
            json.dump(self.config.to_dict(), f)

    def _parse_sensor_packet_data(self, node_id, packet_type, payload, data):
        """Parse sensor data type payloads.

        :param str node_id:
        :param str packet_type: Type of the packet
        :param iter payload: Raw payload to be parsed
        :param dict data: Initialised data dict to be completed with parsed data
        :return dict data:
        """
        if packet_type == "Abs. baros":
            # Write the received payload to the data field
            # TODO bytes_per_sample should probably be in the configuration
            bytes_per_sample = 6
            for i in range(self.config.nodes[node_id].samples_per_packet["Baros_P"]):
                for j in range(self.config.nodes[node_id].number_of_sensors["Baros_P"]):
                    data[node_id]["Baros_P"][j][i] = int.from_bytes(
                        payload[(bytes_per_sample * j) : (bytes_per_sample * j + 4)],
                        self.config.gateway.endian,
                        signed=False,
                    )

                    data[node_id]["Baros_T"][j][i] = int.from_bytes(
                        payload[(bytes_per_sample * j + 4) : (bytes_per_sample * j + 6)],
                        self.config.gateway.endian,
                        signed=True,
                    )

            return data, ["Baros_P", "Baros_T"]

        if packet_type == "Diff. baros":
            bytes_per_sample = 2
            number_of_diff_baros_sensors = self.config.nodes[node_id].number_of_sensors["Diff_Baros"]

            for i in range(self.config.nodes[node_id].samples_per_packet["Diff_Baros"]):
                for j in range(number_of_diff_baros_sensors):
                    data[node_id]["Diff_Baros"][j][i] = int.from_bytes(
                        payload[
                            (bytes_per_sample * (number_of_diff_baros_sensors * i + j)) : (
                                bytes_per_sample * (number_of_diff_baros_sensors * i + j + 1)
                            )
                        ],
                        self.config.gateway.endian,
                        signed=False,
                    )

            return data, ["Diff_Baros"]

        if packet_type == "Mic 0":
            # Write the received payload to the data field
            bytes_per_sample = 3

            for i in range(self.config.nodes[node_id].samples_per_packet["Mics"] // 2):
                for j in range(self.config.nodes[node_id].number_of_sensors[DEFAULT_SENSOR_NAMES[0]] // 2):

                    index = j + 20 * i

                    data[node_id][DEFAULT_SENSOR_NAMES[0]][j][2 * i] = int.from_bytes(
                        payload[(bytes_per_sample * index) : (bytes_per_sample * index + 3)],
                        "big",  # Unlike the other sensors, the microphone data come in big-endian
                        signed=True,
                    )
                    data[node_id][DEFAULT_SENSOR_NAMES[0]][j][2 * i + 1] = int.from_bytes(
                        payload[(bytes_per_sample * (index + 5)) : (bytes_per_sample * (index + 5) + 3)],
                        "big",  # Unlike the other sensors, the microphone data come in big-endian
                        signed=True,
                    )
                    data[node_id][DEFAULT_SENSOR_NAMES[0]][j + 5][2 * i] = int.from_bytes(
                        payload[(bytes_per_sample * (index + 10)) : (bytes_per_sample * (index + 10) + 3)],
                        "big",  # Unlike the other sensors, the microphone data come in big-endian
                        signed=True,
                    )
                    data[node_id][DEFAULT_SENSOR_NAMES[0]][j + 5][2 * i + 1] = int.from_bytes(
                        payload[(bytes_per_sample * (index + 15)) : (bytes_per_sample * (index + 15) + 3)],
                        "big",  # Unlike the other sensors, the microphone data come in big-endian
                        signed=True,
                    )

            return data, [DEFAULT_SENSOR_NAMES[0]]

        if packet_type.startswith("IMU"):
            imu_sensor_names = {"IMU Accel": "Acc", "IMU Gyro": "Gyro", "IMU Magnetometer": "Mag"}
            imu_sensor = imu_sensor_names[packet_type]

            # Write the received payload to the data field
            for i in range(self.config.nodes[node_id].samples_per_packet["Acc"]):
                index = 6 * i

                data[node_id][imu_sensor][0][i] = int.from_bytes(
                    payload[index : (index + 2)], self.config.gateway.endian, signed=True
                )
                data[node_id][imu_sensor][1][i] = int.from_bytes(
                    payload[(index + 2) : (index + 4)], self.config.gateway.endian, signed=True
                )
                data[node_id][imu_sensor][2][i] = int.from_bytes(
                    payload[(index + 4) : (index + 6)], self.config.gateway.endian, signed=True
                )

            return data, [imu_sensor]

        # TODO Analog sensor definitions
        if packet_type in {"Analog Kinetron", "Analog1", "Analog2"}:
            logger.error("Received Analog packet. Not supported atm")
            raise exceptions.UnknownPacketTypeError(f"Packet of type {packet_type!r} is unknown.")

        if packet_type == "Analog Vbat":

            def val_to_v(val):
                return val / 1e6

            for i in range(self.config.nodes[node_id].samples_per_packet["Analog Vbat"]):
                index = 4 * i

                data[node_id]["Analog Vbat"][0][i] = val_to_v(
                    int.from_bytes(payload[index : (index + 4)], self.config.gateway.endian, signed=False)
                )

            return data, ["Analog Vbat"]

        if packet_type == "Constat":
            bytes_per_sample = 10
            for i in range(self.config.nodes[node_id].samples_per_packet["Constat"]):
                data[node_id]["Constat"][0][i] = struct.unpack(
                    "<f" if self.config.gateway.endian == "little" else ">f",
                    payload[(bytes_per_sample * i) : (bytes_per_sample * i + 4)],
                )[0]
                data[node_id]["Constat"][1][i] = int.from_bytes(
                    payload[(bytes_per_sample * i + 4) : (bytes_per_sample * i + 5)],
                    self.config.gateway.endian,
                    signed=True,
                )
                data[node_id]["Constat"][2][i] = int.from_bytes(
                    payload[(bytes_per_sample * i + 5) : (bytes_per_sample * i + 6)],
                    self.config.gateway.endian,
                    signed=True,
                )
                data[node_id]["Constat"][3][i] = int.from_bytes(
                    payload[(bytes_per_sample * i + 6) : (bytes_per_sample * i + 10)],
                    self.config.gateway.endian,
                    signed=False,
                )

            return data, ["Constat"]

        else:  # if packet_type not in self.handles
            logger.error("Sensor of type %r is unknown.", packet_type)
            raise exceptions.UnknownPacketTypeError(f"Sensor of type {packet_type!r} is unknown.")

    def _parse_info_packet(self, node_id, information_type, payload, previous_timestamp):
        """Parse information type packet and send the information to logger.

        :param str node_id:
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
            return

        if information_type == "Cmd Decline":
            reason_index = str(int.from_bytes(payload, self.config.gateway.endian, signed=False))
            logger.info("Command declined, %s", self.config.nodes[node_id].decline_reason[reason_index])
            return

        if information_type == "Sleep State":
            state_index = str(int.from_bytes(payload, self.config.gateway.endian, signed=False))
            logger.info("\n%s\n", self.config.nodes[node_id].sleep_state[state_index])

            if bool(int(state_index)):
                self.sleep = True
            else:
                self.sleep = False
                # Reset previous timestamp on wake up
                for sensor_name in self.config.nodes[node_id].sensor_names:
                    previous_timestamp[node_id][sensor_name] = -1

            return

        if information_type == "Info Message":
            info_index = str(int.from_bytes(payload[0:1], self.config.gateway.endian, signed=False))
            info_type = self.config.nodes[node_id].info_type[info_index]
            logger.info(info_type)

            if info_type == "Battery info":
                voltage = int.from_bytes(payload[1:5], self.config.gateway.endian, signed=False) / 1000000
                cycle = int.from_bytes(payload[5:9], self.config.gateway.endian, signed=False) / 100
                state_of_charge = int.from_bytes(payload[9:13], self.config.gateway.endian, signed=False) / 256

                logger.info(
                    "Voltage : %fV\n Cycle count: %f\nState of charge: %f%%",
                    voltage,
                    cycle,
                    state_of_charge,
                )

            return

    def _check_for_packet_loss(self, node_id, sensor_name, timestamp, previous_timestamp):
        """Check if a packet was lost by looking at the time interval between previous_timestamp and timestamp for
        the sensor_name.

        The sensor data arrives in packets that contain n samples from some sensors of the same type, e.g. one barometer
        packet contains 40 samples from 4 barometers each. Timestamp arrives once per packet. The difference between
        timestamps in two consecutive packets is expected to be approximately equal to the number of samples in the
        packet times sampling period.

        :param str node_id:
        :param str sensor_name:
        :param float timestamp: Current timestamp for the first sample in the packet Unit: s
        :param dict previous_timestamp: Timestamp for the first sample in the previous packet. Must be initialized with -1. Unit: s
        :return None:
        """
        if previous_timestamp[node_id][sensor_name] == -1:
            logger.info("Received first %s packet." % sensor_name)
        else:
            expected_current_timestamp = (
                previous_timestamp[node_id][sensor_name]
                + self.config.nodes[node_id].samples_per_packet[sensor_name]
                * self.config.nodes[node_id].periods[sensor_name]
            )
            timestamp_deviation = timestamp - expected_current_timestamp

            if abs(timestamp_deviation) > self.config.nodes[node_id].max_timestamp_slack:

                if self.sleep:
                    # Only Constat (Connections statistics) comes during sleep
                    return

                if sensor_name in ["Acc", "Gyro", "Mag"]:
                    # IMU sensors are not synchronised to CPU, so their actual periods might differ
                    self.config.nodes[node_id].periods[sensor_name] = (
                        timestamp - previous_timestamp[node_id][sensor_name]
                    ) / self.config.nodes[node_id].samples_per_packet[sensor_name]

                    logger.debug(
                        "Updated %s period to %f ms.",
                        sensor_name,
                        self.config.nodes[node_id].periods[sensor_name] * 1000,
                    )

                else:
                    logger.warning(
                        "Possible packet loss. %s sensor packet is timestamped %d ms later than expected",
                        sensor_name,
                        timestamp_deviation * 1000,
                    )

        previous_timestamp[node_id][sensor_name] = timestamp

    def _timestamp_and_persist_data(self, data, node_id, sensor_name, timestamp, period):
        """Persist data to the required storage media.
        Since timestamps only come at a packet level, this function assumes constant period for
         the within-packet-timestamps

        :param dict data: data to persist
        :param str node_id:
        :param str sensor_name: sensor type to persist data from
        :param float timestamp: timestamp in s
        :param float period:
        :return None:
        """
        number_of_samples = len(data[node_id][sensor_name][0])

        # Iterate through all sample times.
        for i in range(number_of_samples):
            time = timestamp + i * period
            sample = [time]

            for meas in data[node_id][sensor_name]:
                sample.append(meas[i])

            self._add_data_to_current_window(node_id, sensor_name, data=sample)

    def _add_data_to_current_window(self, node_id, sensor_name, data):
        """Add data to the current window.

        :param str node_id:
        :param str sensor_name: sensor type to persist data from
        :param iter data: data to persist
        :return None:
        """
        if self.save_locally:
            self.writer.add_to_current_window(node_id, sensor_name, data)

        if self.upload_to_cloud:
            self.uploader.add_to_current_window(node_id, sensor_name, data)
