import logging
import os
from _thread import start_new_thread
from datetime import datetime
import serial

import sys
from gateway import exceptions
from gateway.readers.constants import (
    ANALOG_SAMPLES_PER_PACKET,
    BAROS_GROUP_SIZE,
    BAROS_SAMPLES_PER_PACKET,
    BAUDRATE,
    DEFAULT_HANDLES,
    ENDIAN,
    IMU_SAMPLES_PER_PACKET,
    MAX_PERIOD_DRIFT,
    MAX_TIMESTAMP_SLACK,
    MICS_SAMPLES_PER_PACKET,
    PACKET_KEY,
    SERIAL_BUFFER_RX_SIZE,
    SERIAL_BUFFER_TX_SIZE,
    SERIAL_PORT,
    TYPE_HANDLE_DEF,
    nMeasQty,
    period,
    samplesPerPacket,
)
from gateway.uploaders import StreamingUploader


logger = logging.getLogger(__name__)


class PacketReader:
    def __init__(self):
        self.handles = DEFAULT_HANDLES
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
        )
        self.stop = False

    def read_packets(self, ser, filenames=None, stop_when_no_more_data=False):
        currentTimestamp = {"Mics": 0, "Baros": 0, "Acc": 0, "Gyro": 0, "Mag": 0, "Analog": 0}
        prevIdealTimestamp = {"Mics": 0, "Baros": 0, "Acc": 0, "Gyro": 0, "Mag": 0, "Analog": 0}

        data = {
            "Mics": [([0] * samplesPerPacket["Mics"]) for _ in range(nMeasQty["Mics"])],
            "Baros": [([0] * samplesPerPacket["Baros"]) for _ in range(nMeasQty["Baros"])],
            "Acc": [([0] * samplesPerPacket["Acc"]) for _ in range(nMeasQty["Acc"])],
            "Gyro": [([0] * samplesPerPacket["Gyro"]) for _ in range(nMeasQty["Gyro"])],
            "Mag": [([0] * samplesPerPacket["Mag"]) for _ in range(nMeasQty["Mag"])],
            "Analog": [([0] * samplesPerPacket["Analog"]) for _ in range(nMeasQty["Analog"])],
        }

        while not self.stop:
            r = ser.read()  # init read data from serial port
            if len(r) == 0:
                if stop_when_no_more_data:
                    break
                continue

            if r[0] == PACKET_KEY:
                pack_type = int.from_bytes(ser.read(), ENDIAN)
                length = int.from_bytes(ser.read(), ENDIAN)
                payload = ser.read(length)

                if pack_type == TYPE_HANDLE_DEF:
                    self.update_handles(payload)
                else:
                    self._parse_sensor_packet(
                        pack_type,
                        payload,
                        filenames or self._generate_default_filenames(),
                        data,
                        currentTimestamp,
                        prevIdealTimestamp,
                    )

    def update_handles(self, payload):
        startHandle = int.from_bytes(payload[0:1], ENDIAN)
        endHandle = int.from_bytes(payload[2:3], ENDIAN)

        if endHandle - startHandle == 50:
            self.handles = {
                startHandle + 2: "Baro group 0",
                startHandle + 4: "Baro group 1",
                startHandle + 6: "Baro group 2",
                startHandle + 8: "Baro group 3",
                startHandle + 10: "Baro group 4",
                startHandle + 12: "Baro group 5",
                startHandle + 14: "Baro group 6",
                startHandle + 16: "Baro group 7",
                startHandle + 18: "Baro group 8",
                startHandle + 20: "Baro group 9",
                startHandle + 22: "Mic 0",
                startHandle + 24: "Mic 1",
                startHandle + 26: "Mic 2",
                startHandle + 28: "Mic 3",
                startHandle + 30: "Mic 4",
                startHandle + 32: "Mic 5",
                startHandle + 34: "Mic 6",
                startHandle + 36: "Mic 7",
                startHandle + 38: "Mic 8",
                startHandle + 40: "Mic 9",
                startHandle + 42: "IMU Accel",
                startHandle + 44: "IMU Gyro",
                startHandle + 46: "IMU Magnetometer",
                startHandle + 48: "Analog",
            }

            logger.info("Succesfully updated handles.")

        else:
            logger.error("Handle error: %s %s", startHandle, endHandle)

    def _parse_sensor_packet(self, sensor_type, payload, filenames, data, currentTimestamp, prevIdealTimestamp):
        if sensor_type not in self.handles:
            raise exceptions.UnknownPacketTypeException("Received packet with unknown type: {}".format(sensor_type))

        t = int.from_bytes(payload[240:244], ENDIAN, signed=False)

        if self.handles[sensor_type].startswith("Baro group"):
            self._wait_until_set_is_complete(
                "Baros", t, filenames, data, currentTimestamp, prevIdealTimestamp
            )  # Writes data to files when set is complete

            # Write the received payload to the data field
            baroGroupNum = int(self.handles[sensor_type][11:])
            for i in range(BAROS_SAMPLES_PER_PACKET):
                for j in range(BAROS_GROUP_SIZE):
                    data["Baros"][baroGroupNum * BAROS_GROUP_SIZE + j][i] = (
                        int.from_bytes(
                            payload[(4 * (BAROS_GROUP_SIZE * i + j)) : (4 * (BAROS_GROUP_SIZE * i + j) + 4)],
                            ENDIAN,
                            signed=False,
                        )
                        / 4096
                    )

        elif self.handles[sensor_type].startswith("Mic"):
            self._wait_until_set_is_complete("Mics", t, filenames, data, currentTimestamp, prevIdealTimestamp)

            # Write the received payload to the data field
            micNum = int(self.handles[sensor_type][4:])
            for i in range(MICS_SAMPLES_PER_PACKET):
                data["Mics"][micNum][i] = int.from_bytes(payload[(2 * i) : (2 * i + 2)], ENDIAN, signed=True)

        elif self.handles[sensor_type].startswith("IMU Accel"):
            self._wait_until_set_is_complete("Acc", t, filenames, data, currentTimestamp, prevIdealTimestamp)

            # Write the received payload to the data field
            for i in range(IMU_SAMPLES_PER_PACKET):
                data["Acc"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], ENDIAN, signed=True)
                data["Acc"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], ENDIAN, signed=True)
                data["Acc"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], ENDIAN, signed=True)

        elif self.handles[sensor_type] == "IMU Gyro":
            self._wait_until_set_is_complete("Gyro", t, filenames, data, currentTimestamp, prevIdealTimestamp)

            # Write the received payload to the data field
            for i in range(IMU_SAMPLES_PER_PACKET):
                data["Gyro"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], ENDIAN, signed=True)
                data["Gyro"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], ENDIAN, signed=True)
                data["Gyro"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], ENDIAN, signed=True)

        elif self.handles[sensor_type] == "IMU Magnetometer":
            self._wait_until_set_is_complete("Mag", t, filenames, data, currentTimestamp, prevIdealTimestamp)

            # Write the received payload to the data field
            for i in range(IMU_SAMPLES_PER_PACKET):
                data["Mag"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], ENDIAN, signed=True)
                data["Mag"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], ENDIAN, signed=True)
                data["Mag"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], ENDIAN, signed=True)

        elif self.handles[sensor_type] == "Analog":
            self._wait_until_set_is_complete("Analog", t, filenames, data, currentTimestamp, prevIdealTimestamp)

            def valToV(val):
                return (val << 6) / 1e6

            for i in range(ANALOG_SAMPLES_PER_PACKET):
                data["Analog"][0][i] = valToV(int.from_bytes(payload[(4 * i) : (4 * i + 2)], ENDIAN, signed=False))
                data["Analog"][1][i] = valToV(int.from_bytes(payload[(4 * i + 2) : (4 * i + 4)], ENDIAN, signed=False))

            # logger.info(data["Analog"][0][0])

    def _wait_until_set_is_complete(self, sensor_type, t, filenames, data, currentTimestamp, prevIdealTimestamp):
        """timestamp in 1/(2**16) s

        :param sensor_type:
        :param t:
        :return:
        """
        if sensor_type in {"Mics", "Baros", "Analog"}:
            # For those measurement types, the samples are inherently synchronized to the CPU time already.
            # The timestamps may be slightly off, so it takes the first one as a reference and then uses the following ones only to check if a packet has been dropped
            # Also, for mics and baros, there exist packet sets: Several packets arrive with the same timestamp
            if t != currentTimestamp[sensor_type] and currentTimestamp[sensor_type] != 0:

                idealNewTimestamp = prevIdealTimestamp[sensor_type] + samplesPerPacket[sensor_type] * period[
                    sensor_type
                ] * (2 ** 16)

                # If at least one set (= one packet per mic/baro group) of packets was lost
                if abs(idealNewTimestamp - currentTimestamp[sensor_type]) > MAX_TIMESTAMP_SLACK * (2 ** 16):
                    if prevIdealTimestamp[sensor_type] != 0:
                        ms_gap = (currentTimestamp[sensor_type] - idealNewTimestamp) / (2 ** 16) * 1000
                        logger.warning("Lost set of %s packets: %s ms gap", sensor_type, ms_gap)
                    else:
                        logger.info("Received first set of %s packets", sensor_type)

                    idealNewTimestamp = currentTimestamp[sensor_type]

                self._write_data(data, sensor_type, idealNewTimestamp / (2 ** 16), period[sensor_type], filenames)

                # clean up data buffer(?)
                data[sensor_type] = [([0] * samplesPerPacket[sensor_type]) for _ in range(nMeasQty[sensor_type])]

                prevIdealTimestamp[sensor_type] = idealNewTimestamp
                currentTimestamp[sensor_type] = t
            elif currentTimestamp[sensor_type] == 0:
                currentTimestamp[sensor_type] = t

        else:  # The IMU values are not synchronized to the CPU time, so we simply always take the timestamp we have
            if currentTimestamp[sensor_type] != 0:
                if (
                    prevIdealTimestamp[sensor_type] != 0
                ):  # If there is a previous timestamp, calculate the actual sampling period from the difference to the current timestamp
                    per = (
                        (currentTimestamp[sensor_type] - prevIdealTimestamp[sensor_type])
                        / samplesPerPacket[sensor_type]
                        / (2 ** 16)
                    )

                    # If the calculated period is reasonable, accept it. If not, most likely a packet got lost
                    if abs(per - period[sensor_type]) / period[sensor_type] < MAX_PERIOD_DRIFT:
                        period[sensor_type] = per
                    else:
                        ms_gap = (currentTimestamp[sensor_type] - prevIdealTimestamp[sensor_type]) / (2 ** 16) * 1000
                        logger.warning("Lost %s packet: %s ms gap", sensor_type, ms_gap)
                else:
                    logger.info("Received first %s packet", sensor_type)

                self._write_data(data, sensor_type, t / (2 ** 16), period[sensor_type], filenames)

            prevIdealTimestamp[sensor_type] = currentTimestamp[sensor_type]
            currentTimestamp[sensor_type] = t

    def _write_data(self, data, sensor_type, timestamp, period, filenames):
        """Dump data to files.

        :param sensor_type:
        :param timestamp: timestamp in s
        :param period:
        :return:
        """
        n = len(data[sensor_type][0])  # number of samples
        for i in range(len(data[sensor_type][0])):  # iterate through all sample times
            time = timestamp - (n - i) * period

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
        folderString = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
        os.mkdir(folderString)

        return {
            "Mics": os.path.join(folderString, "mics.csv"),
            "Baros": os.path.join(folderString, "baros.csv"),
            "Acc": os.path.join(folderString, "acc.csv"),
            "Gyro": os.path.join(folderString, "gyro.csv"),
            "Mag": os.path.join(folderString, "mag.csv"),
            "Analog": os.path.join(folderString, "analog.csv"),
        }


if __name__ == "__main__":
    ser = serial.Serial(SERIAL_PORT, BAUDRATE)  # open serial port
    ser.set_buffer_size(rx_size=SERIAL_BUFFER_RX_SIZE, tx_size=SERIAL_BUFFER_TX_SIZE)

    packet_reader = PacketReader()

    # Thread that will parse serial data and write it to files.
    start_new_thread(packet_reader.read_packets, args=(ser,))

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
            stop = True
            break
        else:
            ser.write(line.encode("utf_8"))
