import datetime
import json
import logging
import os
import struct

from octue.cloud import storage

from data_gateway import exceptions
from data_gateway.persistence import BatchingFileWriter, BatchingUploader, NoOperationContextManager
from data_gateway.reader.configuration import Configuration


logger = logging.getLogger(__name__)


class PacketReader:
    """A serial port packet reader.

    :param bool save_locally: save batches of data locally
    :param bool upload_to_cloud: upload batches of data to Google cloud
    :param str|None output_directory:
    :param float batch_interval: the interval of time in seconds between batches
    :param str|None project_name: name of Google Cloud project to upload to
    :param str|None bucket_name: name of Google Cloud project to upload to
    :param data_gateway.reader.configuration.Configuration|None configuration:
    :return None:
    """

    def __init__(
        self,
        save_locally,
        upload_to_cloud,
        output_directory=None,
        batch_interval=600,
        project_name=None,
        bucket_name=None,
        configuration=None,
    ):
        self.save_locally = save_locally
        self.upload_to_cloud = upload_to_cloud
        self.output_directory = output_directory
        self.config = configuration or Configuration()
        self.handles = self.config.default_handles
        self.stop = False
        self.sensor_names = ("Mics", "Baros_P", "Baros_T", "Acc", "Gyro", "Mag", "Analog Vbat", "Constat")
        self.sensor_time_offset = None
        self.session_subdirectory = str(hash(datetime.datetime.now()))[1:7]

        logger.warning("Timestamp synchronisation unavailable with current hardware; defaulting to using system clock.")

        if upload_to_cloud:
            self.uploader = BatchingUploader(
                sensor_names=self.sensor_names,
                project_name=project_name,
                bucket_name=bucket_name,
                window_size=batch_interval,
                session_subdirectory=self.session_subdirectory,
                output_directory=output_directory,
                metadata={"data_gateway__configuration": self.config},
            )
        else:
            self.uploader = NoOperationContextManager()

        if save_locally:
            self.writer = BatchingFileWriter(
                sensor_names=self.sensor_names,
                window_size=batch_interval,
                session_subdirectory=self.session_subdirectory,
                output_directory=output_directory,
            )
        else:
            self.writer = NoOperationContextManager()

    def read_packets(self, serial_port, stop_when_no_more_data=False):
        """Read and process packets from a serial port, uploading them to Google Cloud storage and/or writing them to
        disk.

        :param serial.Serial serial_port: name of serial port to read from
        :param bool stop_when_no_more_data: stop reading when no more data is received from the port (for testing)
        :return None:
        """
        self._persist_configuration()

        current_timestamp = {}
        previous_ideal_timestamp = {}
        data = {}

        for sensor_name in self.sensor_names:
            current_timestamp[sensor_name] = 0
            previous_ideal_timestamp[sensor_name] = 0
            data[sensor_name] = [
                ([0] * self.config.samples_per_packet[sensor_name]) for _ in range(self.config.n_meas_qty[sensor_name])
            ]

        with self.uploader:
            with self.writer:
                while not self.stop:

                    serial_data = serial_port.read()

                    if len(serial_data) == 0:
                        if stop_when_no_more_data:
                            break
                        continue

                    if serial_data[0] != self.config.packet_key:
                        continue

                    packet_type = int.from_bytes(serial_port.read(), self.config.endian)
                    length = int.from_bytes(serial_port.read(), self.config.endian)
                    payload = serial_port.read(length)

                    if packet_type == self.config.type_handle_def:
                        self.update_handles(payload)
                        continue

                    self._parse_sensor_packet(
                        sensor_type=packet_type,
                        payload=payload,
                        data=data,
                        current_timestamp=current_timestamp,
                        previous_ideal_timestamp=previous_ideal_timestamp,
                    )

    def update_handles(self, payload):
        """Update the Bluetooth handles object.

        :param iter payload:
        :return None:
        """
        start_handle = int.from_bytes(payload[0:1], self.config.endian)
        end_handle = int.from_bytes(payload[2:3], self.config.endian)

        if end_handle - start_handle == 20:
            self.handles = {
                start_handle + 2: "Abs. baros",
                start_handle + 4: "Diff. baros",
                start_handle + 6: "Mic 0",
                start_handle + 8: "Mic 1",
                start_handle + 10: "IMU Accel",
                start_handle + 12: "IMU Gyro",
                start_handle + 14: "IMU Magnetometer",
                start_handle + 16: "Analog1",
                start_handle + 18: "Analog2",
                start_handle + 20: "Constat",
            }

            logger.info("Successfully updated handles.")
            return

        logger.error("Handle error: %s %s", start_handle, end_handle)

    def _persist_configuration(self):
        """Persist the configuration to disk and/or cloud storage.

        :return None:
        """
        configuration_dictionary = self.config.to_dict()

        if self.save_locally:
            with open(
                os.path.abspath(
                    os.path.join(".", self.output_directory, self.session_subdirectory, "configuration.json")
                ),
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

    def _parse_sensor_packet(self, sensor_type, payload, data, current_timestamp, previous_ideal_timestamp):
        """Parse a packet from a sensor.

        :param int sensor_type:
        :param iter payload:
        :param dict data:
        :param dict current_timestamp:
        :param dict previous_ideal_timestamp:
        :return None:
        """
        if sensor_type not in self.handles:
            raise exceptions.UnknownPacketTypeException("Received packet with unknown type: {}".format(sensor_type))

        t = int.from_bytes(payload[240:244], self.config.endian, signed=False)

        if self.handles[sensor_type] == "Abs. baros":
            # Write data to files when set is complete.
            self._wait_until_set_is_complete("Baros_P", t, data, current_timestamp, previous_ideal_timestamp)
            self._wait_until_set_is_complete("Baros_T", t, data, current_timestamp, previous_ideal_timestamp)

            # Write the received payload to the data field
            # TODO bytes_per_sample should probably be in the configuration
            bytes_per_sample = 6
            for i in range(self.config.baros_samples_per_packet):
                for j in range(self.config.n_meas_qty["Baros_P"]):

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

        elif self.handles[sensor_type] == "Mic 0":
            self._wait_until_set_is_complete("Mics", t, data, current_timestamp, previous_ideal_timestamp)

            # Write the received payload to the data field
            # TODO bytes_per_sample should probably be in the configuration
            bytes_per_sample = 3

            for i in range(self.config.mics_samples_per_packet // 2):
                # TODO what is 5 and? move to configuration?
                #    self.config.endian = 'big', unlike for baros..
                for j in range(5):
                    data["Mics"][j][2 * i] = int.from_bytes(
                        payload[
                            (bytes_per_sample * j + 20 * bytes_per_sample * i) : (
                                bytes_per_sample * j + 20 * bytes_per_sample * i + 3
                            )
                        ],
                        "big",
                        signed=True,
                    )
                    data["Mics"][j][2 * i + 1] = int.from_bytes(
                        payload[
                            (bytes_per_sample * j + 20 * bytes_per_sample * i + 5 * bytes_per_sample) : (
                                bytes_per_sample * j + 20 * bytes_per_sample * i + 3 + 5 * bytes_per_sample
                            )
                        ],
                        "big",
                        signed=True,
                    )
                    data["Mics"][j + 5][2 * i] = int.from_bytes(
                        payload[
                            (bytes_per_sample * j + 20 * bytes_per_sample * i + 10 * bytes_per_sample) : (
                                bytes_per_sample * j + 20 * bytes_per_sample * i + 3 + 10 * bytes_per_sample
                            )
                        ],
                        "big",
                        signed=True,
                    )
                    data["Mics"][j + 5][2 * i + 1] = int.from_bytes(
                        payload[
                            (bytes_per_sample * j + 20 * bytes_per_sample * i + 15 * bytes_per_sample) : (
                                bytes_per_sample * j + 20 * bytes_per_sample * i + 3 + 15 * bytes_per_sample
                            )
                        ],
                        "big",
                        signed=True,
                    )
        elif self.handles[sensor_type] == "Mic 1":
            if payload[0] == 1:
                logger.info("Microphone data reading done")
            elif payload[0] == 2:
                logger.info("Microphone data erasing done")
            elif payload[0] == 3:
                logger.info("Microphones started ")

        elif self.handles[sensor_type].startswith("IMU Accel"):
            self._wait_until_set_is_complete("Acc", t, data, current_timestamp, previous_ideal_timestamp)

            # Write the received payload to the data field
            for i in range(self.config.imu_samples_per_packet):
                data["Acc"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], self.config.endian, signed=True)
                data["Acc"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], self.config.endian, signed=True)
                data["Acc"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], self.config.endian, signed=True)

        elif self.handles[sensor_type] == "IMU Gyro":
            self._wait_until_set_is_complete("Gyro", t, data, current_timestamp, previous_ideal_timestamp)

            # Write the received payload to the data field
            for i in range(self.config.imu_samples_per_packet):
                data["Gyro"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], self.config.endian, signed=True)
                data["Gyro"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], self.config.endian, signed=True)
                data["Gyro"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], self.config.endian, signed=True)

        elif self.handles[sensor_type] == "IMU Magnetometer":
            self._wait_until_set_is_complete("Mag", t, data, current_timestamp, previous_ideal_timestamp)

            # Write the received payload to the data field
            for i in range(self.config.imu_samples_per_packet):
                data["Mag"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], self.config.endian, signed=True)
                data["Mag"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], self.config.endian, signed=True)
                data["Mag"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], self.config.endian, signed=True)

        # TODO Analog sensor definitions
        elif self.handles[sensor_type] in {"Analog Kinetron", "Analog1", "Analog2"}:
            logger.error("Received Kinetron packet. Not supported atm")

        elif self.handles[sensor_type] == "Analog Vbat":
            self._wait_until_set_is_complete("Analog Vbat", t, data, current_timestamp, previous_ideal_timestamp)

            def val_to_v(val):
                return val / 1e6

            for i in range(self.config.analog_samples_per_packet):
                data["Analog Vbat"][0][i] = val_to_v(
                    int.from_bytes(payload[(4 * i) : (4 * i + 4)], self.config.endian, signed=False)
                )

        elif self.handles[sensor_type] == "Constat":
            self._wait_until_set_is_complete("Constat", t, data, current_timestamp, previous_ideal_timestamp)

            print("Constat packet: %d" % (t / (2 ** 16)))

            bytes_per_sample = 10
            for i in range(self.config.constat_samples_per_packet):
                data["Constat"][0][i] = struct.unpack(
                    "<f" if self.config.endian == "little" else ">f",
                    payload[(bytes_per_sample * i) : (bytes_per_sample * i + 4)],
                )[0]
                data["Constat"][1][i] = int.from_bytes(
                    payload[(bytes_per_sample * i + 4) : (bytes_per_sample * i + 5)], self.config.endian, signed=True
                )
                data["Constat"][2][i] = int.from_bytes(
                    payload[(bytes_per_sample * i + 5) : (bytes_per_sample * i + 6)], self.config.endian, signed=True
                )
                data["Constat"][3][i] = int.from_bytes(
                    payload[(bytes_per_sample * i + 6) : (bytes_per_sample * i + 10)], self.config.endian, signed=False
                )

        else:
            raise exceptions.UnknownSensorTypeException(f"Sensor of type {self.handles[sensor_type]!r} is unknown.")

    def _wait_until_set_is_complete(self, sensor_type, t, data, current_timestamp, prev_ideal_timestamp):
        """timestamp in 1/(2**16) s

        :param str sensor_type:
        :param t:
        :param dict data:
        :param dict current_timestamp:
        :param dict prev_ideal_timestamp:
        :return None:
        """
        if sensor_type in {"Mics", "Baros_P", "Baros_T", "Analog Vbat", "Constat"}:
            # For those measurement types, the samples are inherently synchronized to the CPU time already. The
            # timestamps may be slightly off, so it takes the first one as a reference and then uses the following ones
            # only to check if a packet has been dropped. Also, for mics and baros, there exist packet sets: Several
            # packets arrive with the same timestamp.
            if t != current_timestamp[sensor_type] and current_timestamp[sensor_type] != 0:

                ideal_new_timestamp = prev_ideal_timestamp[sensor_type] + self.config.samples_per_packet[
                    sensor_type
                ] * self.config.period[sensor_type] * (2 ** 16)

                # If at least one set (= one packet per mic/baro group) of packets was lost
                if abs(ideal_new_timestamp - current_timestamp[sensor_type]) > self.config.max_timestamp_slack * (
                    2 ** 16
                ):

                    if prev_ideal_timestamp[sensor_type] != 0:
                        ms_gap = (current_timestamp[sensor_type] - ideal_new_timestamp) / (2 ** 16) * 1000
                        logger.warning("Lost set of %s packets: %s ms gap", sensor_type, ms_gap)
                    else:
                        logger.info("Received first set of %s packets", sensor_type)

                    ideal_new_timestamp = current_timestamp[sensor_type]

                self._persist_data(data, sensor_type, ideal_new_timestamp / (2 ** 16), self.config.period[sensor_type])

                # clean up data buffer(?)
                data[sensor_type] = [
                    ([0] * self.config.samples_per_packet[sensor_type])
                    for _ in range(self.config.n_meas_qty[sensor_type])
                ]

                prev_ideal_timestamp[sensor_type] = ideal_new_timestamp
                current_timestamp[sensor_type] = t

            elif current_timestamp[sensor_type] == 0:
                current_timestamp[sensor_type] = t

        else:  # The IMU values are not synchronized to the CPU time, so we simply always take the timestamp we have
            if current_timestamp[sensor_type] != 0:

                # If there is a previous timestamp, calculate the actual sampling period from the difference to the
                # current timestamp
                if prev_ideal_timestamp[sensor_type] != 0:
                    period = (
                        (current_timestamp[sensor_type] - prev_ideal_timestamp[sensor_type])
                        / self.config.samples_per_packet[sensor_type]
                        / (2 ** 16)
                    )

                    # If the calculated period is reasonable, accept it. If not, most likely a packet got lost
                    if (
                        abs(period - self.config.period[sensor_type]) / self.config.period[sensor_type]
                        < self.config.max_period_drift
                    ):
                        self.config.period[sensor_type] = period

                    else:
                        ms_gap = (current_timestamp[sensor_type] - prev_ideal_timestamp[sensor_type]) / (2 ** 16) * 1000
                        logger.warning("Lost %s packet: %s ms gap", sensor_type, ms_gap)

                else:
                    logger.info("Received first %s packet", sensor_type)

                self._persist_data(data, sensor_type, t / (2 ** 16), self.config.period[sensor_type])

            prev_ideal_timestamp[sensor_type] = current_timestamp[sensor_type]
            current_timestamp[sensor_type] = t

    def _persist_data(self, data, sensor_type, timestamp, period):
        """Persist data to the required storage media.

        :param dict data: data to persist
        :param str sensor_type: sensor type to persist data from
        :param float timestamp: timestamp in s
        :param float period:
        :return None:
        """
        number_of_samples = len(data[sensor_type][0])
        time = None

        # Iterate through all sample times.
        for i in range(number_of_samples):
            time = timestamp - (number_of_samples - i) * period
            sample = [time]

            for meas in data[sensor_type]:
                sample.append(meas[i])

            self._add_to_required_storage_media_batches(sensor_type, data=sample)

        # The first time this method runs, calculate the offset between the last timestamp of the first sample and the
        # UTC time now. Store it as the `start_timestamp` metadata in the batches.
        if sensor_type == "Baros_P" and self.sensor_time_offset is None:
            if time:
                self._calculate_and_store_sensor_timestamp_offset(time)

    def _calculate_and_store_sensor_timestamp_offset(self, timestamp):
        """Calculate the offset between the given timestamp and the UTC time now, storing it in the metadata of the
        batches in the uploader and/or writer.

        :param float timestamp: posix timestamp from sensor
        :return None:
        """
        now = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).timestamp()
        self.sensor_time_offset = now - timestamp

        if hasattr(self.writer, "current_batch"):
            self.writer.current_batch["sensor_time_offset"] = self.sensor_time_offset
            self.writer.ready_batch["sensor_time_offset"] = self.sensor_time_offset

        if hasattr(self.uploader, "current_batch"):
            self.uploader.current_batch["sensor_time_offset"] = self.sensor_time_offset
            self.uploader.ready_batch["sensor_time_offset"] = self.sensor_time_offset

    def _add_to_required_storage_media_batches(self, sensor_type, data):
        """Add the data to the required storage media batches (currently a file writer batch and/or a cloud uploader
        batch).

        :param str sensor_type: sensor type to persist data from
        :param iter data: data to persist
        :return None:
        """
        if self.save_locally:
            self.writer.add_to_current_window(sensor_type, data)

        if self.upload_to_cloud:
            self.uploader.add_to_current_window(sensor_type, data)
