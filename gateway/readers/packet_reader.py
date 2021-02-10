import logging
import os
from _thread import start_new_thread
from datetime import datetime
import serial

import sys
from gateway import exceptions
from gateway.readers import constants
from gateway.uploaders import StreamingUploader


logger = logging.getLogger(__name__)


class PacketReader:
    def __init__(self, upload_interval=600):
        self.handles = constants.DEFAULT_HANDLES
        self.uploader = StreamingUploader(
            sensor_types=(
                {"name": "Mics", "extension": ".csv"},
                {"name": "Baros", "extension": ".csv"},
                {"name": "Acc", "extension": ".csv"},
                {"name": "Gyro", "extension": ".csv"},
                {"name": "Mag", "extension": ".csv"},
                {"name": "Analog", "extension": ".csv"},
            ),
            project_name=os.environ["TEST_PROJECT_NAME"],
            bucket_name=os.environ["TEST_BUCKET_NAME"],
            upload_interval=upload_interval,
        )
        self.stop = False

    def read_packets(self, serial_port, filenames=None, stop_when_no_more_data=False):
        current_timestamp = {"Mics": 0, "Baros": 0, "Acc": 0, "Gyro": 0, "Mag": 0, "Analog": 0}
        previous_ideal_timestamp = {"Mics": 0, "Baros": 0, "Acc": 0, "Gyro": 0, "Mag": 0, "Analog": 0}

        data = {
            "Mics": [([0] * constants.samplesPerPacket["Mics"]) for _ in range(constants.nMeasQty["Mics"])],
            "Baros": [([0] * constants.samplesPerPacket["Baros"]) for _ in range(constants.nMeasQty["Baros"])],
            "Acc": [([0] * constants.samplesPerPacket["Acc"]) for _ in range(constants.nMeasQty["Acc"])],
            "Gyro": [([0] * constants.samplesPerPacket["Gyro"]) for _ in range(constants.nMeasQty["Gyro"])],
            "Mag": [([0] * constants.samplesPerPacket["Mag"]) for _ in range(constants.nMeasQty["Mag"])],
            "Analog": [([0] * constants.samplesPerPacket["Analog"]) for _ in range(constants.nMeasQty["Analog"])],
        }

        with self.uploader:

            while not self.stop:
                r = serial_port.read()
                if len(r) == 0:
                    if stop_when_no_more_data:
                        break
                    continue

                if r[0] != constants.PACKET_KEY:
                    continue

                packet_type = int.from_bytes(serial_port.read(), constants.ENDIAN)
                length = int.from_bytes(serial_port.read(), constants.ENDIAN)
                payload = serial_port.read(length)

                if packet_type == constants.TYPE_HANDLE_DEF:
                    self.update_handles(payload)
                    continue

                self._parse_sensor_packet(
                    sensor_type=packet_type,
                    payload=payload,
                    filenames=filenames or self._generate_default_filenames(),
                    data=data,
                    current_timestamp=current_timestamp,
                    previous_ideal_timestamp=previous_ideal_timestamp,
                )

    def update_handles(self, payload):
        start_handle = int.from_bytes(payload[0:1], constants.ENDIAN)
        end_handle = int.from_bytes(payload[2:3], constants.ENDIAN)

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

    def _parse_sensor_packet(self, sensor_type, payload, filenames, data, current_timestamp, previous_ideal_timestamp):
        if sensor_type not in self.handles:
            raise exceptions.UnknownPacketTypeException("Received packet with unknown type: {}".format(sensor_type))

        t = int.from_bytes(payload[240:244], constants.ENDIAN, signed=False)

        if self.handles[sensor_type].startswith("Baro group"):
            # Write data to files when set is complete.
            self._wait_until_set_is_complete("Baros", t, filenames, data, current_timestamp, previous_ideal_timestamp)

            # Write the received payload to the data field
            baro_group_number = int(self.handles[sensor_type][11:])

            for i in range(constants.BAROS_SAMPLES_PER_PACKET):
                for j in range(constants.BAROS_GROUP_SIZE):
                    data["Baros"][baro_group_number * constants.BAROS_GROUP_SIZE + j][i] = (
                        int.from_bytes(
                            payload[
                                (4 * (constants.BAROS_GROUP_SIZE * i + j)) : (
                                    4 * (constants.BAROS_GROUP_SIZE * i + j) + 4
                                )
                            ],
                            constants.ENDIAN,
                            signed=False,
                        )
                        / 4096
                    )

        elif self.handles[sensor_type].startswith("Mic"):
            self._wait_until_set_is_complete("Mics", t, filenames, data, current_timestamp, previous_ideal_timestamp)

            # Write the received payload to the data field
            mic_number = int(self.handles[sensor_type][4:])
            for i in range(constants.MICS_SAMPLES_PER_PACKET):
                data["Mics"][mic_number][i] = int.from_bytes(
                    payload[(2 * i) : (2 * i + 2)], constants.ENDIAN, signed=True
                )

        elif self.handles[sensor_type].startswith("IMU Accel"):
            self._wait_until_set_is_complete("Acc", t, filenames, data, current_timestamp, previous_ideal_timestamp)

            # Write the received payload to the data field
            for i in range(constants.IMU_SAMPLES_PER_PACKET):
                data["Acc"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], constants.ENDIAN, signed=True)
                data["Acc"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], constants.ENDIAN, signed=True)
                data["Acc"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], constants.ENDIAN, signed=True)

        elif self.handles[sensor_type] == "IMU Gyro":
            self._wait_until_set_is_complete("Gyro", t, filenames, data, current_timestamp, previous_ideal_timestamp)

            # Write the received payload to the data field
            for i in range(constants.IMU_SAMPLES_PER_PACKET):
                data["Gyro"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], constants.ENDIAN, signed=True)
                data["Gyro"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], constants.ENDIAN, signed=True)
                data["Gyro"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], constants.ENDIAN, signed=True)

        elif self.handles[sensor_type] == "IMU Magnetometer":
            self._wait_until_set_is_complete("Mag", t, filenames, data, current_timestamp, previous_ideal_timestamp)

            # Write the received payload to the data field
            for i in range(constants.IMU_SAMPLES_PER_PACKET):
                data["Mag"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], constants.ENDIAN, signed=True)
                data["Mag"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], constants.ENDIAN, signed=True)
                data["Mag"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], constants.ENDIAN, signed=True)

        elif self.handles[sensor_type] == "Analog":
            self._wait_until_set_is_complete("Analog", t, filenames, data, current_timestamp, previous_ideal_timestamp)

            def val_to_v(val):
                return (val << 6) / 1e6

            for i in range(constants.ANALOG_SAMPLES_PER_PACKET):
                data["Analog"][0][i] = val_to_v(
                    int.from_bytes(payload[(4 * i) : (4 * i + 2)], constants.ENDIAN, signed=False)
                )
                data["Analog"][1][i] = val_to_v(
                    int.from_bytes(payload[(4 * i + 2) : (4 * i + 4)], constants.ENDIAN, signed=False)
                )

            # logger.info(data["Analog"][0][0])

    def _wait_until_set_is_complete(self, sensor_type, t, filenames, data, current_timestamp, prev_ideal_timestamp):
        """timestamp in 1/(2**16) s

        :param sensor_type:
        :param t:
        :return:
        """
        if sensor_type in {"Mics", "Baros", "Analog"}:
            # For those measurement types, the samples are inherently synchronized to the CPU time already. The
            # timestamps may be slightly off, so it takes the first one as a reference and then uses the following ones
            # only to check if a packet has been dropped Also, for mics and baros, there exist packet sets: Several
            # packets arrive with the same timestamp
            if t != current_timestamp[sensor_type] and current_timestamp[sensor_type] != 0:

                ideal_new_timestamp = prev_ideal_timestamp[sensor_type] + constants.samplesPerPacket[
                    sensor_type
                ] * constants.period[sensor_type] * (2 ** 16)

                # If at least one set (= one packet per mic/baro group) of packets was lost
                if abs(ideal_new_timestamp - current_timestamp[sensor_type]) > constants.MAX_TIMESTAMP_SLACK * (
                    2 ** 16
                ):

                    if prev_ideal_timestamp[sensor_type] != 0:
                        ms_gap = (current_timestamp[sensor_type] - ideal_new_timestamp) / (2 ** 16) * 1000
                        logger.warning("Lost set of %s packets: %s ms gap", sensor_type, ms_gap)
                    else:
                        logger.info("Received first set of %s packets", sensor_type)

                    ideal_new_timestamp = current_timestamp[sensor_type]

                self._write_data(
                    data, sensor_type, ideal_new_timestamp / (2 ** 16), constants.period[sensor_type], filenames
                )

                # clean up data buffer(?)
                data[sensor_type] = [
                    ([0] * constants.samplesPerPacket[sensor_type]) for _ in range(constants.nMeasQty[sensor_type])
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
                        / constants.samplesPerPacket[sensor_type]
                        / (2 ** 16)
                    )

                    # If the calculated period is reasonable, accept it. If not, most likely a packet got lost
                    if (
                        abs(period - constants.period[sensor_type]) / constants.period[sensor_type]
                        < constants.MAX_PERIOD_DRIFT
                    ):
                        constants.period[sensor_type] = period

                    else:
                        ms_gap = (current_timestamp[sensor_type] - prev_ideal_timestamp[sensor_type]) / (2 ** 16) * 1000
                        logger.warning("Lost %s packet: %s ms gap", sensor_type, ms_gap)

                else:
                    logger.info("Received first %s packet", sensor_type)

                self._write_data(data, sensor_type, t / (2 ** 16), constants.period[sensor_type], filenames)

            prev_ideal_timestamp[sensor_type] = current_timestamp[sensor_type]
            current_timestamp[sensor_type] = t

    def _write_data(self, data, sensor_type, timestamp, period, filenames):
        """Dump data to files.

        :param sensor_type:
        :param timestamp: timestamp in s
        :param period:
        :return:
        """
        number_of_samples = len(data[sensor_type][0])

        # Iterate through all sample times.
        for i in range(len(data[sensor_type][0])):
            time = timestamp - (number_of_samples - i) * period

            with open(filenames[sensor_type], "a") as f:
                f.write(str(time) + ",")
            self.uploader.add_to_stream(sensor_type, str(time) + ",")

            for meas in data[sensor_type]:  # iterate through all measured quantities
                with open(filenames[sensor_type], "a") as f:
                    f.write(str(meas[i]) + ",")
                self.uploader.add_to_stream(sensor_type, str(meas[i]) + ",")

            with open(filenames[sensor_type], "a") as f:
                f.write("\n")
            self.uploader.add_to_stream(sensor_type, "\n")

    @staticmethod
    def _generate_default_filenames():
        folder_name = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
        os.mkdir(folder_name)

        return {
            "Mics": os.path.join(folder_name, "mics.csv"),
            "Baros": os.path.join(folder_name, "baros.csv"),
            "Acc": os.path.join(folder_name, "acc.csv"),
            "Gyro": os.path.join(folder_name, "gyro.csv"),
            "Mag": os.path.join(folder_name, "mag.csv"),
            "Analog": os.path.join(folder_name, "analog.csv"),
        }


if __name__ == "__main__":
    serial_port = serial.Serial(constants.SERIAL_PORT, constants.BAUDRATE)
    serial_port.set_buffer_size(rx_size=constants.SERIAL_BUFFER_RX_SIZE, tx_size=constants.SERIAL_BUFFER_TX_SIZE)
    packet_reader = PacketReader()

    # Thread that will parse serial data and write it to files.
    start_new_thread(packet_reader.read_packets, args=(serial_port,))

    """
    time.sleep(1)
    ser.write(("configMics "  + str(MICS_FREQ)  + " " + str(MICS_BM) + "\n").encode('utf_8'))
    time.sleep(1)
    ser.write(("configBaros " + str(BAROS_FREQ) + " " + str(BAROS_BM) + "\n").encode('utf_8'))
    time.sleep(1)
    ser.write(("configAccel " + str(ACC_FREQ)   + " " + str(ACC_RANGE) + "\n").encode('utf_8'))
    time.sleep(1)
    ser.write(("configGyro "  + str(GYRO_FREQ)  + " " + str(GYRO_RANGE) + "\n").encode('utf_8'))
    """

    for line in sys.stdin:
        if line == "stop\n":
            packet_reader.stop = True
            break

        serial_port.write(line.encode("utf_8"))
