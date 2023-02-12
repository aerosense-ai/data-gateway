import datetime
import json
import logging
import multiprocessing
import os
import queue
import statistics
import struct
import time

from octue.cloud import storage
from octue.log_handlers import apply_log_handler

from data_gateway import exceptions, stop_gateway
from data_gateway.configuration import (
    BASE_STATION_ID,
    DEFAULT_SENSOR_NAMES,
    HANDLE_DEFINITION_PACKET_TYPE,
    Configuration,
)
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
    :param bool save_local_logs: Add a RotatingFileHandler to write logs to the local file system as well as stdout.
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
        save_local_logs=False,
    ):
        self.save_locally = save_locally
        self.upload_to_cloud = upload_to_cloud
        self.session_subdirectory = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")

        self.cloud_output_directory = storage.path.join(output_directory, self.session_subdirectory)
        self.local_output_directory = os.path.abspath(os.path.join(output_directory, self.session_subdirectory))
        os.makedirs(self.local_output_directory, exist_ok=True)

        if save_local_logs:
            level = logger.handlers[0].level
            formatter = logger.handlers[0].formatter
            self.local_log_file = os.path.abspath(
                os.path.join(output_directory, self.session_subdirectory, "gateway.log")
            )
            handler = logging.handlers.RotatingFileHandler(
                self.local_log_file, maxBytes=(1024 * 1024 * 1024), backupCount=1
            )
            handler.setLevel(level)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        self.window_size = window_size
        self.bucket_name = bucket_name
        self.config = configuration or Configuration()
        self.save_csv_files = save_csv_files

        self.uploader = None
        self.writer = None
        self.handles = {node_id: node_config.initial_node_handles for node_id, node_config in self.config.nodes.items()}
        self.handles[BASE_STATION_ID] = self.config.gateway.initial_gateway_handles

        self.time_offsets = {}

    def read_packets(self, serial_port_name, packet_queue, stop_signal, use_dummy_serial_port=False):
        """Read packets from a serial port and send them to the parser thread for processing and persistence.

        :param str serial_port_name: the name of the serial port to read from
        :param queue.Queue packet_queue: a thread-safe queue to put packets on to for the parser thread to pick up
        :param multiprocessing.Value stop_signal: a value of 0 means don't stop; a value of 1 means stop
        :param bool use_dummy_serial_port: if `True` use a dummy serial port for testing
        :return None:
        """
        try:
            process_id = os.getpid()
            logger.info("Packet reader process (pid %s) started from main process.", process_id)

            try:
                nice_value = os.nice(-15)
                logger.info("Packet reader process prioritised with niceness %s", nice_value)
            except PermissionError:
                logger.warning("Could not increase priority of packet reader - PermissionError")
            except AttributeError:
                logger.warning(
                    "Could not increase priority of packet reader - AttributeError. Note: os.nice() is not available on some systems eg windows"
                )

            serial_port = get_serial_port(
                serial_port=serial_port_name,
                configuration=self.config,
                use_dummy_serial_port=use_dummy_serial_port,
            )

            packet_count = 0

            while stop_signal.value == 0:
                # Check the leading byte of the packet
                leading_byte = serial_port.read()

                # Handle no data on the serial port.
                if len(leading_byte) == 0:
                    continue

                # Get the ID of the packet origin
                if leading_byte in self.config.leading_bytes_map:
                    packet_origin = self.config.leading_bytes_map[leading_byte]
                else:
                    continue

                # Read the packet from the serial port.
                packet_type = str(int.from_bytes(serial_port.read(), self.config.gateway.endian))
                length = int.from_bytes(serial_port.read(), self.config.gateway.endian)
                packet = serial_port.read(length)

                # Check for bytes in serial input buffer. A full buffer results in overflow.
                if serial_port.in_waiting == self.config.gateway.serial_buffer_rx_size:
                    logger.warning("Serial port buffer is full - buffer overflow may occur, resulting in data loss.")
                    continue

                logger.debug("Received packet_type %s from packet_origin %s", packet_type, packet_origin)

                # Record the time of packet receipt
                packet_timestamp = time.time()

                packet_queue.put(
                    {
                        "packet_origin": packet_origin,
                        "packet_type": packet_type,
                        "packet": packet,
                        "packet_timestamp": packet_timestamp,
                    }
                )

                # Log progress at info level so we know it's not gone dead during long reads
                packet_count += 1
                if packet_count % 200 == 0:
                    logger.info("Total packets read: %d", packet_count)

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
        process_id = os.getpid()
        logger.info("Packet parser process (pid %s) started from main process.", process_id)

        try:
            nice_value = os.nice(15)
            logger.info("Packet parser process prioritised with niceness %s", nice_value)
        except PermissionError:
            logger.warning("Could not increase priority of packet parser - PermissionError")
        except AttributeError:
            logger.warning(
                "Could not increase priority of packet parser - AttributeError. Note: os.nice() is not available on some systems eg windows"
            )

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

        # Initialise data as zeros
        data = {}
        for node_id in self.config.node_ids:
            node_config = self.config.nodes[node_id]
            data[node_id] = {}
            for sensor_name in node_config.sensor_names:
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
                            packet_origin, packet_type, packet, packet_timestamp = packet_queue.get(
                                timeout=timeout
                            ).values()

                        except queue.Empty:
                            if stop_when_no_more_data_after is not False:
                                break
                            continue
                        if packet_type == str(HANDLE_DEFINITION_PACKET_TYPE):
                            logger.warning(
                                "Origin %s (re)connected, updating handles and re-setting time offsets.", packet_origin
                            )
                            self.update_handles(packet, packet_origin)
                            self._reset_time_offset(packet_origin, packet_timestamp)

                        elif packet_type not in self.handles[packet_origin]:
                            logger.error(
                                "Received packet from origin %s with unknown type %s at time %s. The most common cause of this is running two instances of the gateway.",
                                packet_origin,
                                packet_type,
                                packet_timestamp,
                            )

                        else:
                            logger.debug(
                                "Processing packet from origin %s with type %s (%s)",
                                packet_origin,
                                packet_type,
                                self.handles[packet_origin][packet_type],
                            )

                            packet_type_name = self.handles[packet_origin][packet_type]

                            timestamp = self._apply_time_offset(
                                packet_origin, packet_type_name, packet, packet_timestamp
                            )

                            if timestamp is None:
                                logger.error(
                                    "Unable to apply time offset to packet, skipped packet parsing (origin %s, type %s)",
                                    packet_origin,
                                    packet_timestamp,
                                )

                            elif packet_type_name == "Local Info Message":
                                local_info_type = str(
                                    int.from_bytes(packet[0:1], self.config.gateway.endian, signed=False)
                                )
                                local_info_type_name = self.config.gateway.local_info_types[local_info_type]
                                logger.info(
                                    "Received %s from origin %s: %s",
                                    packet_type_name,
                                    packet_origin,
                                    local_info_type_name,
                                )

                                if local_info_type_name == "Time synchronization info":
                                    # sync_info_type = int.from_bytes(payload[1:5], ENDIAN, signed=False)
                                    # if sync_info_type == 0:
                                    #     print("seq data")
                                    #     for i in range(15):
                                    #         seqDataFile.write(str(int.from_bytes(payload[5 + i * 4 : 9 + i * 4], ENDIAN, signed=False)) + ",")
                                    #     for i in range(15, 18):
                                    #         seqDataFile.write(str(int.from_bytes(payload[5 + i * 4 : 9 + i * 4], ENDIAN, signed=True)) + ",")
                                    #     seqDataFile.close()
                                    # elif sync_info_type == 1:
                                    #     print("central data")
                                    #     for i in range(60):
                                    #         centralDataFile.write(
                                    #             str(int.from_bytes(payload[5 + i * 4 : 9 + i * 4], ENDIAN, signed=False)) + ","
                                    #         )
                                    #         centralCnt = centralCnt + 1
                                    #         if centralCnt == 187:
                                    #             centralDataFile.close()
                                    #             break
                                    # elif sync_info_type == 2:
                                    #     print("perif 0 data")
                                    #     for i in range(61):
                                    #         perif0DataFile.write(
                                    #             str(int.from_bytes(payload[5 + i * 4 : 9 + i * 4], ENDIAN, signed=False)) + ","
                                    #         )
                                    #     perif0DataFile.close()
                                    # elif sync_info_type == 3:
                                    #     print("perif 1 data")
                                    #     for i in range(61):
                                    #         perif1DataFile.write(
                                    #             str(int.from_bytes(payload[5 + i * 4 : 9 + i * 4], ENDIAN, signed=False)) + ","
                                    #         )
                                    #     perif1DataFile.close()
                                    # elif sync_info_type == 4:
                                    #     print("perif 2 data")
                                    #     for i in range(61):
                                    #         perif2DataFile.write(
                                    #             str(int.from_bytes(payload[5 + i * 4 : 9 + i * 4], ENDIAN, signed=False)) + ","
                                    #         )
                                    #     perif2DataFile.close()
                                    logger.warning("Time synchronisation information received but not yet handled")

                            elif packet_type_name in [
                                "Abs. baros",
                                "Diff. baros",
                                "Mic 0",
                                "IMU Accel",
                                "IMU Gyro",
                                "IMU Magnetometer",
                                "Analog1",
                                "Analog2",
                                "Constat",
                                "Timestamp Packet 0",
                                "Timestamp Packet 1",
                            ]:
                                data, sensor_names = self._parse_sensor_packet_data(
                                    packet_origin=packet_origin,
                                    packet_type_name=packet_type_name,
                                    payload=packet,
                                    data=data,
                                )

                                for sensor_name in sensor_names:
                                    self._timestamp_and_persist_data(
                                        data=data,
                                        node_id=packet_origin,
                                        sensor_name=sensor_name,
                                        timestamp=timestamp,
                                        period=node_config.periods[sensor_name],
                                    )

                            elif packet_type_name in [
                                "Mic 1",
                                "Cmd Decline",
                                "Sleep State",
                                "Remote Info Message",
                            ]:
                                self._parse_remote_info_packet(
                                    packet_origin=packet_origin,
                                    packet_type_name=packet_type_name,
                                    timestamp=timestamp,
                                    packet=packet,
                                )

                            else:
                                logger.error(
                                    "Unprocessed packet of type %s from packet_origin %s - check packet type names for a corresponding parser routine",
                                    packet_type_name,
                                    packet_origin,
                                )

        except KeyboardInterrupt:
            pass

        finally:
            stop_gateway(logger, stop_signal)

    def update_handles(self, payload, node_id):
        """Update the Bluetooth handles object. Handles are updated every time a new Bluetooth connection is
        established.

        :param iter payload:
        :param str node_id: the ID of the node the packet is from
        :return None:
        """
        start_handle = int.from_bytes(payload[0:1], self.config.gateway.endian)
        end_handle = int.from_bytes(payload[2:3], self.config.gateway.endian)

        # TODO - resolve whether this actually ever gets updated or not (it always overwrites with the same)
        # TODO resolve why there is a 30 difference between start and end handle, but there's a 32 difference in the handle range below
        if end_handle - start_handle != 30:
            logger.error(
                "Error while updating handles for node %s: start handle is %s, end handle is %s.",
                node_id,
                start_handle,
                end_handle,
            )

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
            str(start_handle + 26): "Remote Info Message",
            str(start_handle + 28): "Timestamp Packet 0",
            str(start_handle + 30): "Timestamp Packet 1",
            str(start_handle + 32): "Local Info Message",
        }

        logger.info("Successfully updated handles for node %s.", node_id)

    def _apply_time_offset(self, packet_origin, packet_type_name, packet, packet_timestamp):
        # Full length packets are always suffixed by a timestamp
        if len(packet) == 244:
            node_timestamp = (
                int.from_bytes(
                    packet[240:244],
                    self.config.gateway.endian,
                    signed=False,
                )
                / 2**16
            )
            if packet_type_name == "Constat":
                # Use the node_timestamp in the constats to update the offset (synchronise to the packet timestamp)
                current_offset = packet_timestamp - node_timestamp

                # Store up to 10 historic entries of the time offset to enable median filtering, preventing jittering
                # timestamps from corrupting data sequences
                offset_history = self.time_offsets.get(packet_origin, [])
                offset_history.append(current_offset)
                if len(offset_history) > 10:
                    offset_history.pop(0)
                self.time_offsets[packet_origin] = offset_history

                # Use the median to adjust timestamps even for the reference packets
                median_offset = statistics.median(offset_history)
                absolute_timestamp = node_timestamp + median_offset

                logger.debug(
                    "Updated time offset for packet_origin %s to %s (current_offset = %s, median_offset = %s)",
                    packet_origin,
                    absolute_timestamp,
                    current_offset,
                    median_offset,
                )

            elif self.time_offsets.get(packet_origin, None) is None:
                # If there's no offset stored, we can't correctly handle the packet timestamp.
                # This can occur on startup where we could be processing raw data left on the buffer, prior to
                # receiving a constats packet or a handles update
                absolute_timestamp = None
                logger.debug(
                    "No offset available for packet_origin %s",
                    packet_origin,
                )

            else:
                # Convert the node timestamp in the packet to an absolute timestamp
                median_offset = statistics.median(self.time_offsets[packet_origin])
                absolute_timestamp = node_timestamp + median_offset

        else:
            # No node timestamp, packet_timestamp is the best approximation
            absolute_timestamp = packet_timestamp

        return absolute_timestamp

    def _reset_time_offset(self, packet_origin, packet_timestamp, node_timestamp=0):
        """Set the time offset to the packet timestamp
        This should be issued on restart of a node (typically when the node_timestamp is assumed to be zero)
        """
        self.time_offsets[packet_origin] = [packet_timestamp - node_timestamp]

    def _save_configuration_to_disk(self):
        """Save the configuration to disk as a JSON file.

        :return None:
        """
        with open(os.path.join(self.local_output_directory, "configuration.json"), "w") as f:
            json.dump(self.config.to_dict(), f)

    def _parse_sensor_packet_data(self, packet_origin, packet_type_name, payload, data):
        """Parse sensor data type payloads.

        :param str packet_origin: the ID of the node the packet is from
        :param str packet_type: Type of the packet
        :param iter payload: Raw payload to be parsed
        :param dict data: Initialised data dict to be completed with parsed data
        :return dict data:
        """
        node_config = self.config.nodes[packet_origin]

        if packet_type_name == "Abs. baros":
            # Write the received payload to the data field
            # TODO bytes_per_sample should probably be in the configuration
            bytes_per_sample = 6
            for i in range(node_config.samples_per_packet["Baros_P"]):
                for j in range(node_config.number_of_sensors["Baros_P"]):
                    data[packet_origin]["Baros_P"][j][i] = int.from_bytes(
                        payload[(bytes_per_sample * j) : (bytes_per_sample * j + 4)],
                        self.config.gateway.endian,
                        signed=False,
                    )

                    data[packet_origin]["Baros_T"][j][i] = int.from_bytes(
                        payload[(bytes_per_sample * j + 4) : (bytes_per_sample * j + 6)],
                        self.config.gateway.endian,
                        signed=True,
                    )

            return data, ["Baros_P", "Baros_T"]

        if packet_type_name == "Diff. baros":
            bytes_per_sample = 2
            number_of_diff_baros_sensors = node_config.number_of_sensors["Diff_Baros"]

            #             TODO REMOVE THIS DEBUGGING LOG LOOP AS ITS WASTEFUL
            logger.debug("Checking contents of Diff Baros payload %s", payload)
            for i in range(len(payload)):
                if (i % 2) == 0:
                    the_integer = int.from_bytes(
                        payload[i : i + 1],
                        self.config.gateway.endian,
                        signed=False,
                    )
                    logger.debug("decoded payload value %i at position %i", the_integer, i)

            for i in range(node_config.samples_per_packet["Diff_Baros"]):
                for j in range(number_of_diff_baros_sensors):
                    data[packet_origin]["Diff_Baros"][j][i] = int.from_bytes(
                        payload[
                            (bytes_per_sample * (number_of_diff_baros_sensors * i + j)) : (
                                bytes_per_sample * (number_of_diff_baros_sensors * i + j + 1)
                            )
                        ],
                        self.config.gateway.endian,
                        signed=False,
                    )

            return data, ["Diff_Baros"]

        if packet_type_name == "Mic 0":
            # Write the received payload to the data field
            bytes_per_sample = 3

            for i in range(node_config.samples_per_packet["Mics"] // 2):
                for j in range(node_config.number_of_sensors[DEFAULT_SENSOR_NAMES[0]] // 2):
                    index = j + 20 * i

                    data[packet_origin][DEFAULT_SENSOR_NAMES[0]][j][2 * i] = int.from_bytes(
                        payload[(bytes_per_sample * index) : (bytes_per_sample * index + 3)],
                        "big",  # Unlike the other sensors, the microphone data come in big-endian
                        signed=True,
                    )
                    data[packet_origin][DEFAULT_SENSOR_NAMES[0]][j][2 * i + 1] = int.from_bytes(
                        payload[(bytes_per_sample * (index + 5)) : (bytes_per_sample * (index + 5) + 3)],
                        "big",  # Unlike the other sensors, the microphone data come in big-endian
                        signed=True,
                    )
                    data[packet_origin][DEFAULT_SENSOR_NAMES[0]][j + 5][2 * i] = int.from_bytes(
                        payload[(bytes_per_sample * (index + 10)) : (bytes_per_sample * (index + 10) + 3)],
                        "big",  # Unlike the other sensors, the microphone data come in big-endian
                        signed=True,
                    )
                    data[packet_origin][DEFAULT_SENSOR_NAMES[0]][j + 5][2 * i + 1] = int.from_bytes(
                        payload[(bytes_per_sample * (index + 15)) : (bytes_per_sample * (index + 15) + 3)],
                        "big",  # Unlike the other sensors, the microphone data come in big-endian
                        signed=True,
                    )

            return data, [DEFAULT_SENSOR_NAMES[0]]

        if packet_type_name.startswith("IMU"):
            imu_sensor_names = {"IMU Accel": "Acc", "IMU Gyro": "Gyro", "IMU Magnetometer": "Mag"}
            imu_sensor = imu_sensor_names[packet_type_name]

            # Write the received payload to the data field
            for i in range(node_config.samples_per_packet["Acc"]):
                index = 6 * i

                data[packet_origin][imu_sensor][0][i] = int.from_bytes(
                    payload[index : (index + 2)], self.config.gateway.endian, signed=True
                )
                data[packet_origin][imu_sensor][1][i] = int.from_bytes(
                    payload[(index + 2) : (index + 4)], self.config.gateway.endian, signed=True
                )
                data[packet_origin][imu_sensor][2][i] = int.from_bytes(
                    payload[(index + 4) : (index + 6)], self.config.gateway.endian, signed=True
                )

            return data, [imu_sensor]

        if packet_type_name in {"Analog Kinetron", "Analog1", "Analog2"}:
            logger.error("Received Analog Kinetron, Analog1 or Analog2 packet. Not supported atm")
            raise exceptions.UnknownPacketTypeError(f"Packet of type {packet_type_name!r} is unknown.")

        if packet_type_name == "Analog Vbat":

            def val_to_v(val):
                return val / 1e6

            for i in range(node_config.samples_per_packet["Analog Vbat"]):
                index = 4 * i

                data[packet_origin]["Analog Vbat"][0][i] = val_to_v(
                    int.from_bytes(payload[index : (index + 4)], self.config.gateway.endian, signed=False)
                )

            return data, ["Analog Vbat"]

        if packet_type_name == "Constat":
            bytes_per_sample = 10
            for i in range(node_config.samples_per_packet["Constat"]):
                data[packet_origin]["Constat"][0][i] = struct.unpack(
                    "<f" if self.config.gateway.endian == "little" else ">f",
                    payload[(bytes_per_sample * i) : (bytes_per_sample * i + 4)],
                )[0]
                data[packet_origin]["Constat"][1][i] = int.from_bytes(
                    payload[(bytes_per_sample * i + 4) : (bytes_per_sample * i + 5)],
                    self.config.gateway.endian,
                    signed=True,
                )
                data[packet_origin]["Constat"][2][i] = int.from_bytes(
                    payload[(bytes_per_sample * i + 5) : (bytes_per_sample * i + 6)],
                    self.config.gateway.endian,
                    signed=True,
                )
                data[packet_origin]["Constat"][3][i] = int.from_bytes(
                    payload[(bytes_per_sample * i + 6) : (bytes_per_sample * i + 10)],
                    self.config.gateway.endian,
                    signed=False,
                )
                # Display only the first as an indication to avoid flooding logs
                if i == 0:
                    logger.info(
                        "Constats received from node %s: filtered_rssi=%s, raw_rssi=%s, tx_power=%s, allocated_heap_memory=%s",
                        packet_origin,
                        data[packet_origin]["Constat"][0][i],
                        data[packet_origin]["Constat"][1][i],
                        data[packet_origin]["Constat"][2][i],
                        data[packet_origin]["Constat"][3][i],
                    )

            return data, ["Constat"]

        else:  # if packet_type not in self.handles
            logger.error("Sensor of type %r is unknown.", packet_type_name)
            raise exceptions.UnknownPacketTypeError(f"Sensor of type {packet_type_name!r} is unknown.")

    def _parse_remote_info_packet(self, packet_origin, packet_type_name, timestamp, packet):
        """Parse information type packet and send the information to logger.

        :param str packet_origin: the ID of the node the packet is from
        :param str packet_type_name: From packet handles, defines what information is stored in the packet.
        :param iter packet: The packet
        :return None:
        """
        node_config = self.config.nodes[packet_origin]

        if packet_type_name == "Mic 1":
            if packet[0] == 1:
                logger.info("Sensor reading from flash done")
            elif packet[0] == 2:
                logger.info("Flash erasing done")
            elif packet[0] == 3:
                logger.info("Sensor started")

        elif packet_type_name == "Cmd Decline":
            reason_index = str(int.from_bytes(packet, self.config.gateway.endian, signed=False))
            logger.info("Command declined, %s", node_config.decline_reason[reason_index])

        elif packet_type_name == "Sleep State":
            state_index = str(int.from_bytes(packet, self.config.gateway.endian, signed=False))
            logger.info("Sleep state updated on node %s: %s", packet_origin, node_config.sleep_state[state_index])

        elif packet_type_name == "Remote Info Message":
            remote_info_key = str(int.from_bytes(packet[0:1], self.config.gateway.endian, signed=False))
            info_subtype = node_config.remote_info_type[remote_info_key]
            logger.info("Received remote info packet from node %s: %s", packet_origin, info_subtype)

            # TODO store the voltage in results so that we'll be able to display it in the dashboard
            if info_subtype == "Battery info":
                voltage = int.from_bytes(packet[1:5], self.config.gateway.endian, signed=False) / 1000000
                cycle = int.from_bytes(packet[5:9], self.config.gateway.endian, signed=False) / 100
                state_of_charge = int.from_bytes(packet[9:13], self.config.gateway.endian, signed=False) / 256

                self._add_data_to_current_window(
                    packet_origin, sensor_name="battery_info", data=[timestamp, voltage, cycle, state_of_charge]
                )

                logger.info(
                    "Voltage : %fV\n Cycle count: %f\nState of charge: %f%%",
                    voltage,
                    cycle,
                    state_of_charge,
                )

        elif packet_type_name == "Timestamp Packet 0":
            # print("timestamp packet", int(len / 4), len)
            # for i in range(int(len / 4)):
            #     files["ts" + str(packet_source)].write(
            #         str(int.from_bytes(payload[i * 4 : (i + 1) * 4], ENDIAN, signed=False)) + ","
            #     )
            logger.warning("Received Timestamp Packet 0, handling not implemented yet")

        elif packet_type_name == "Timestamp Packet 1":
            # print("time elapse packet", int(len / 4), len)
            # for i in range(int(len / 4)):
            #     files["sampleElapse" + str(packet_source)].write(
            #         str(int.from_bytes(payload[i * 4 : (i + 1) * 4], ENDIAN, signed=False)) + ","
            #     )
            logger.warning("Received Timestamp Packet 1, handling not implemented yet")

    def _timestamp_and_persist_data(self, data, node_id, sensor_name, timestamp, period):
        """Persist data to the required storage media. Since timestamps only come at a packet level, this function
        assumes constant period for the within-packet-timestamps.

        :param dict data: data to persist
        :param str node_id: the ID of the node the data is from
        :param str sensor_name: sensor type to persist data from
        :param float timestamp: timestamp in s
        :param float period:
        :return None:
        """
        number_of_samples = len(data[node_id][sensor_name][0])

        # Iterate through all sample times.
        for i in range(number_of_samples):
            sample_time = timestamp + i * period
            sample = [sample_time]

            for meas in data[node_id][sensor_name]:
                sample.append(meas[i])

            self._add_data_to_current_window(node_id, sensor_name, data=sample)

    def _add_data_to_current_window(self, node_id, sensor_name, data):
        """Add data to the current window.

        :param str node_id: the ID of the node the data is from
        :param str sensor_name: sensor type to persist data from
        :param iter data: data to persist
        :return None:
        """
        if self.save_locally:
            self.writer.add_to_current_window(node_id, sensor_name, data)

        if self.upload_to_cloud:
            self.uploader.add_to_current_window(node_id, sensor_name, data)
