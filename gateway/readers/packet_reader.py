import logging
import os
from _thread import start_new_thread
from datetime import datetime
import serial

import sys
from gateway import exceptions


logger = logging.getLogger(__name__)


MICS_FREQ = 5000
MICS_BM = 0x3FF
BAROS_FREQ = 100
BAROS_BM = 0x3FF
ACC_FREQ = 100
ACC_RANGE = 16
GYRO_FREQ = 100
GYRO_RANGE = 2000
ANALOG_FREQ = 16384

SERIAL_PORT = "COM9"
SERIAL_BUFFER_RX_SIZE = 100000
SERIAL_BUFFER_TX_SIZE = 1280

BAUDRATE = 2300000
ENDIAN = "little"
MAX_TIMESTAMP_SLACK = 5e-3  # 5ms
MAX_PERIOD_DRIFT = 0.02  # 2% difference between IMU clock and CPU clock allowed

PACKET_KEY = 0xFE

TYPE_HANDLE_DEF = 0xFF

handles = {
    34: "Baro group 0",
    36: "Baro group 1",
    38: "Baro group 2",
    40: "Baro group 3",
    42: "Baro group 4",
    44: "Baro group 5",
    46: "Baro group 6",
    48: "Baro group 7",
    50: "Baro group 8",
    52: "Baro group 9",
    54: "Mic 0",
    56: "Mic 1",
    58: "Mic 2",
    60: "Mic 3",
    62: "Mic 4",
    64: "Mic 5",
    66: "Mic 6",
    68: "Mic 7",
    70: "Mic 8",
    72: "Mic 9",
    74: "IMU Accel",
    76: "IMU Gyro",
    78: "IMU Magnetometer",
    80: "Analog",
}

MICS_SAMPLES_PER_PACKET = 120
BAROS_PACKET_SIZE = 60
BAROS_GROUP_SIZE = 4
BAROS_SAMPLES_PER_PACKET = int(BAROS_PACKET_SIZE / BAROS_GROUP_SIZE)
IMU_SAMPLES_PER_PACKET = int(240 / 2 / 3)
ANALOG_SAMPLES_PER_PACKET = 60

samplesPerPacket = {
    "Mics": MICS_SAMPLES_PER_PACKET,
    "Baros": BAROS_SAMPLES_PER_PACKET,
    "Acc": IMU_SAMPLES_PER_PACKET,
    "Gyro": IMU_SAMPLES_PER_PACKET,
    "Mag": IMU_SAMPLES_PER_PACKET,
    "Analog": ANALOG_SAMPLES_PER_PACKET,
}

nMeasQty = {
    "Mics": 10,
    "Baros": 40,
    "Acc": 3,
    "Gyro": 3,
    "Mag": 3,
    "Analog": 2,
}

data = {
    "Mics": [([0] * samplesPerPacket["Mics"]) for _ in range(nMeasQty["Mics"])],
    "Baros": [([0] * samplesPerPacket["Baros"]) for _ in range(nMeasQty["Baros"])],
    "Acc": [([0] * samplesPerPacket["Acc"]) for _ in range(nMeasQty["Acc"])],
    "Gyro": [([0] * samplesPerPacket["Gyro"]) for _ in range(nMeasQty["Gyro"])],
    "Mag": [([0] * samplesPerPacket["Mag"]) for _ in range(nMeasQty["Mag"])],
    "Analog": [([0] * samplesPerPacket["Analog"]) for _ in range(nMeasQty["Analog"])],
}

period = {
    "Mics": 1 / MICS_FREQ,
    "Baros": 1 / BAROS_FREQ,
    "Acc": 1 / ACC_FREQ,
    "Gyro": 1 / GYRO_FREQ,
    "Mag": 1 / 12.5,
    "Analog": 1 / ANALOG_FREQ,
}

currentTimestamp = {"Mics": 0, "Baros": 0, "Acc": 0, "Gyro": 0, "Mag": 0, "Analog": 0}
prevIdealTimestamp = {"Mics": 0, "Baros": 0, "Acc": 0, "Gyro": 0, "Mag": 0, "Analog": 0}


streams = {"Mics": [], "Baros": [], "Acc": [], "Gyro": [], "Mag": [], "Analog": []}


def parseHandleDef(payload):
    startHandle = int.from_bytes(payload[0:1], ENDIAN)
    endHandle = int.from_bytes(payload[2:3], ENDIAN)

    if endHandle - startHandle == 50:
        # TODO resolve with Rafael what he wants to be done here. "handles is a local variable which does not
        #  update the "handles" variable in the outer scope, which is perhaps what was intended.
        handles = {  # noqa
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
        logger.warning("Updated the handles in loacl scope only - see TODO")
    else:
        logger.error("Handle error: %s %s", startHandle, endHandle)


def writeData(sensor_type, timestamp, period, filenames):
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
        streams[sensor_type].append(str(time) + ",")

        for meas in data[sensor_type]:  # iterate through all measured quantities
            with open(filenames[sensor_type], "a") as f:
                f.write(str(meas[i]) + ",")
            streams[sensor_type].append(str(meas[i]) + ",")

        with open(filenames[sensor_type], "a") as f:
            f.write("\n")
        streams[sensor_type].append("\n")


def waitTillSetComplete(sensor_type, t, filenames):  # timestamp in 1/(2**16) s
    """
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

            writeData(sensor_type, idealNewTimestamp / (2 ** 16), period[sensor_type], filenames)

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

            writeData(sensor_type, t / (2 ** 16), period[sensor_type], filenames)

        prevIdealTimestamp[sensor_type] = currentTimestamp[sensor_type]
        currentTimestamp[sensor_type] = t


def parseSensorPacket(sensor_type, payload, filenames):
    if sensor_type not in handles:
        raise exceptions.UnknownPacketTypeException("Received packet with unknown type: {}".format(sensor_type))

    t = int.from_bytes(payload[240:244], ENDIAN, signed=False)

    if handles[sensor_type].startswith("Baro group"):
        waitTillSetComplete("Baros", t, filenames)  # Writes data to files when set is complete

        # Write the received payload to the data field
        baroGroupNum = int(handles[sensor_type][11:])
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

    elif handles[sensor_type].startswith("Mic"):
        waitTillSetComplete("Mics", t, filenames)

        # Write the received payload to the data field
        micNum = int(handles[sensor_type][4:])
        for i in range(MICS_SAMPLES_PER_PACKET):
            data["Mics"][micNum][i] = int.from_bytes(payload[(2 * i) : (2 * i + 2)], ENDIAN, signed=True)

    elif handles[sensor_type].startswith("IMU Accel"):
        waitTillSetComplete("Acc", t, filenames)

        # Write the received payload to the data field
        for i in range(IMU_SAMPLES_PER_PACKET):
            data["Acc"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], ENDIAN, signed=True)
            data["Acc"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], ENDIAN, signed=True)
            data["Acc"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], ENDIAN, signed=True)

    elif handles[sensor_type] == "IMU Gyro":
        waitTillSetComplete("Gyro", t, filenames)

        # Write the received payload to the data field
        for i in range(IMU_SAMPLES_PER_PACKET):
            data["Gyro"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], ENDIAN, signed=True)
            data["Gyro"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], ENDIAN, signed=True)
            data["Gyro"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], ENDIAN, signed=True)

    elif handles[sensor_type] == "IMU Magnetometer":
        waitTillSetComplete("Mag", t, filenames)

        # Write the received payload to the data field
        for i in range(IMU_SAMPLES_PER_PACKET):
            data["Mag"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], ENDIAN, signed=True)
            data["Mag"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], ENDIAN, signed=True)
            data["Mag"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], ENDIAN, signed=True)

    elif handles[sensor_type] == "Analog":
        waitTillSetComplete("Analog", t, filenames)

        def valToV(val):
            return (val << 6) / 1e6

        for i in range(ANALOG_SAMPLES_PER_PACKET):
            data["Analog"][0][i] = valToV(int.from_bytes(payload[(4 * i) : (4 * i + 2)], ENDIAN, signed=False))
            data["Analog"][1][i] = valToV(int.from_bytes(payload[(4 * i + 2) : (4 * i + 4)], ENDIAN, signed=False))

        # logger.info(data["Analog"][0][0])


def generate_default_filenames():
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


stop = False


def read_packets(ser, filenames=None, stop_when_no_more_data=False):
    global stop

    while not stop:
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
                parseHandleDef(payload)
            else:
                parseSensorPacket(pack_type, payload, filenames or generate_default_filenames())


if __name__ == "__main__":
    ser = serial.Serial(SERIAL_PORT, BAUDRATE)  # open serial port
    ser.set_buffer_size(rx_size=SERIAL_BUFFER_RX_SIZE, tx_size=SERIAL_BUFFER_TX_SIZE)

    # Thread that will parse serial data and write it to files.
    start_new_thread(read_packets, args=(ser,), kwargs={"filenames": generate_default_filenames()})

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
