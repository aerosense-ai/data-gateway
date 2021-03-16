import json
import logging
import os
from datetime import datetime
from octue.utils.cloud import storage

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

        self.sensor_names = ("Mics", "Baros", "Acc", "Gyro", "Mag", "Analog")

        session_subdirectory = str(hash(datetime.now()))[1:7]

        if upload_to_cloud:
            self.uploader = BatchingUploader(
                sensor_names=self.sensor_names,
                project_name=project_name,
                bucket_name=bucket_name,
                batch_interval=batch_interval,
                session_subdirectory=session_subdirectory,
                output_directory=output_directory,
                metadata=self.config.user_data,
            )
        else:
            self.uploader = NoOperationContextManager()

        if save_locally:
            self.writer = BatchingFileWriter(
                sensor_names=self.sensor_names,
                batch_interval=batch_interval,
                session_subdirectory=session_subdirectory,
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

        if end_handle - start_handle == 50:
            self.handles = {
                start_handle + 2: "Baro group 0",
                start_handle + 4: "Baro group 1",
                start_handle + 6: "Baro group 2",
                start_handle + 8: "Baro group 3",
                start_handle + 10: "Baro group 4",
                start_handle + 12: "Baro group 5",
                start_handle + 14: "Baro group 6",
                start_handle + 16: "Baro group 7",
                start_handle + 18: "Baro group 8",
                start_handle + 20: "Baro group 9",
                start_handle + 22: "Mic 0",
                start_handle + 24: "Mic 1",
                start_handle + 26: "Mic 2",
                start_handle + 28: "Mic 3",
                start_handle + 30: "Mic 4",
                start_handle + 32: "Mic 5",
                start_handle + 34: "Mic 6",
                start_handle + 36: "Mic 7",
                start_handle + 38: "Mic 8",
                start_handle + 40: "Mic 9",
                start_handle + 42: "IMU Accel",
                start_handle + 44: "IMU Gyro",
                start_handle + 46: "IMU Magnetometer",
                start_handle + 48: "Analog",
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
            with open(os.path.abspath(os.path.join(".", self.output_directory, "configuration.json")), "w") as f:
                json.dump(configuration_dictionary, f)

        if self.upload_to_cloud:
            self.uploader.client.upload_from_string(
                string=json.dumps(configuration_dictionary),
                bucket_name=self.uploader.bucket_name,
                path_in_bucket=storage.path.join(self.output_directory, "configuration.json"),
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

        if self.handles[sensor_type].startswith("Baro group"):
            # Write data to files when set is complete.
            self._wait_until_set_is_complete("Baros", t, data, current_timestamp, previous_ideal_timestamp)

            # Write the received payload to the data field
            baro_group_number = int(self.handles[sensor_type][11:])

            for i in range(self.config.baros_samples_per_packet):
                for j in range(self.config.baros_group_size):
                    data["Baros"][baro_group_number * self.config.baros_group_size + j][i] = (
                        int.from_bytes(
                            payload[
                                (4 * (self.config.baros_group_size * i + j)) : (
                                    4 * (self.config.baros_group_size * i + j) + 4
                                )
                            ],
                            self.config.endian,
                            signed=False,
                        )
                        / 4096
                    )

        elif self.handles[sensor_type].startswith("Mic"):
            self._wait_until_set_is_complete("Mics", t, data, current_timestamp, previous_ideal_timestamp)

            # Write the received payload to the data field
            mic_number = int(self.handles[sensor_type][4:])
            for i in range(self.config.mics_samples_per_packet):
                data["Mics"][mic_number][i] = int.from_bytes(
                    payload[(2 * i) : (2 * i + 2)], self.config.endian, signed=True
                )

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

        elif self.handles[sensor_type] == "Analog":
            self._wait_until_set_is_complete("Analog", t, data, current_timestamp, previous_ideal_timestamp)

            def val_to_v(val):
                return (val << 6) / 1e6

            for i in range(self.config.analog_samples_per_packet):
                data["Analog"][0][i] = val_to_v(
                    int.from_bytes(payload[(4 * i) : (4 * i + 2)], self.config.endian, signed=False)
                )
                data["Analog"][1][i] = val_to_v(
                    int.from_bytes(payload[(4 * i + 2) : (4 * i + 4)], self.config.endian, signed=False)
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
        if sensor_type in {"Mics", "Baros", "Analog"}:
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
        :param timestamp: timestamp in s
        :param period:
        :return None:
        """
        number_of_samples = len(data[sensor_type][0])

        # Iterate through all sample times.
        for i in range(len(data[sensor_type][0])):
            time = timestamp - (number_of_samples - i) * period
            self._add_to_required_storage_media_batches(sensor_type, data=str(time) + ",")

            for meas in data[sensor_type]:
                self._add_to_required_storage_media_batches(sensor_type, data=str(meas[i]) + ",")

            self._add_to_required_storage_media_batches(sensor_type, data="\n")

    def _add_to_required_storage_media_batches(self, sensor_type, data):
        """Add the data to the required storage media batches (currently a file writer batch and/or a cloud uploader
        batch).

        :param str sensor_type: sensor type to persist data from
        :param str data: data to persist
        :return None:
        """
        if self.save_locally:
            self.writer.add_to_current_batch(sensor_type, data)

        if self.upload_to_cloud:
            self.uploader.add_to_current_batch(sensor_type, data)
