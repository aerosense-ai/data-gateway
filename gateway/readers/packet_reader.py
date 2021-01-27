# flake8: noqa
# TODO remove the noqa and conform to flake8

import logging
import os
from datetime import datetime
import serial
from _thread import start_new_thread

import sys


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


def parseHandleDef(payload):
    startHandle = int.from_bytes(payload[0:1], ENDIAN)
    endHandle = int.from_bytes(payload[2:3], ENDIAN)

    if endHandle - startHandle == 50:
        # TODO resolve with Rafael what he wants to be done here. "handles is a local variable which does not
        #  update the "handles" variable in the outer scope, which is perhaps what was intended.
        handles = {
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


files = {}

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
    "Mics": [([0] * samplesPerPacket["Mics"]) for i in range(nMeasQty["Mics"])],
    "Baros": [([0] * samplesPerPacket["Baros"]) for i in range(nMeasQty["Baros"])],
    "Acc": [([0] * samplesPerPacket["Acc"]) for i in range(nMeasQty["Acc"])],
    "Gyro": [([0] * samplesPerPacket["Gyro"]) for i in range(nMeasQty["Gyro"])],
    "Mag": [([0] * samplesPerPacket["Mag"]) for i in range(nMeasQty["Mag"])],
    "Analog": [([0] * samplesPerPacket["Analog"]) for i in range(nMeasQty["Analog"])],
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


def writeData(type, timestamp, period):
    """
    Dump data to files.
    :param type:
    :param timestamp: timestamp in s
    :param period:
    :return:
    """
    n = len(data[type][0])  # number of samples
    for i in range(len(data[type][0])):  # iterate through all sample times
        time = timestamp - (n - i) * period
        files[type].write(str(time) + ",")
        for meas in data[type]:  # iterate through all measured quantities
            files[type].write(str(meas[i]) + ",")
        files[type].write("\n")


def waitTillSetComplete(type, t):  # timestamp in 1/(2**16) s
    """

    :param type:
    :param t:
    :return:
    """
    if type == "Mics" or type == "Baros" or type == "Analog":
        # For those measurement types, the samples are inherently synchronized to the CPU time already.
        # The timestamps may be slightly off, so it takes the first one as a reference and then uses the following ones only to check if a packet has been dropped
        # Also, for mics and baros, there exist packet sets: Several packets arrive with the same timestamp
        if t != currentTimestamp[type] and currentTimestamp[type] != 0:
            idealNewTimestamp = prevIdealTimestamp[type] + samplesPerPacket[type] * period[type] * (2 ** 16)
            if abs(idealNewTimestamp - currentTimestamp[type]) > MAX_TIMESTAMP_SLACK * (
                2 ** 16
            ):  # If at least one set (= one packet per mic/baro group) of packets was lost
                if prevIdealTimestamp[type] != 0:
                    ms_gap = (currentTimestamp[type] - idealNewTimestamp) / (2 ** 16) * 1000
                    logger.warning("Lost set of %s packets: %s ms gap", type, ms_gap)
                else:
                    logger.info("Received first set of %s packets", type)

                idealNewTimestamp = currentTimestamp[type]
            writeData(type, idealNewTimestamp / (2 ** 16), period[type])
            data[type] = [([0] * samplesPerPacket[type]) for i in range(nMeasQty[type])]  # clean up data buffer(?)
            prevIdealTimestamp[type] = idealNewTimestamp
            currentTimestamp[type] = t
        elif currentTimestamp[type] == 0:
            currentTimestamp[type] = t
    else:  # The IMU values are not synchronized to the CPU time, so we simply always take the timestamp we have
        if currentTimestamp[type] != 0:
            if (
                prevIdealTimestamp[type] != 0
            ):  # If there is a previous timestamp, calculate the actual sampling period from the difference to the current timestamp
                per = (currentTimestamp[type] - prevIdealTimestamp[type]) / samplesPerPacket[type] / (2 ** 16)
                if (
                    abs(per - period[type]) / period[type] < MAX_PERIOD_DRIFT
                ):  # If the calculated period is reasonable, accept it. If not, most likely a packet got lost
                    period[type] = per
                else:
                    ms_gap = (currentTimestamp[type] - prevIdealTimestamp[type]) / (2 ** 16) * 1000
                    logger.warning("Lost %s packet: %s ms gap", type, ms_gap)
            else:
                logger.info("Received first %s packet", type)

            writeData(type, t / (2 ** 16), period[type])
        prevIdealTimestamp[type] = currentTimestamp[type]
        currentTimestamp[type] = t


def parseSensorPacket(type, payload):
    if not type in handles:
        logger.info("Received packet with unknown type: %s", type)
        return

    t = int.from_bytes(payload[240:244], ENDIAN, signed=False)  #

    if handles[type].startswith("Baro group"):
        waitTillSetComplete("Baros", t)  # Writes data to files when set is complete

        # Write the received payload to the data field
        baroGroupNum = int(handles[type][11:])
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

    elif handles[type].startswith("Mic"):
        waitTillSetComplete("Mics", t)

        # Write the received payload to the data field
        micNum = int(handles[type][4:])
        for i in range(MICS_SAMPLES_PER_PACKET):
            data["Mics"][micNum][i] = int.from_bytes(payload[(2 * i) : (2 * i + 2)], ENDIAN, signed=True)

    elif handles[type].startswith("IMU Accel"):
        waitTillSetComplete("Acc", t)

        # Write the received payload to the data field
        for i in range(IMU_SAMPLES_PER_PACKET):
            data["Acc"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], ENDIAN, signed=True)
            data["Acc"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], ENDIAN, signed=True)
            data["Acc"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], ENDIAN, signed=True)

    elif handles[type] == "IMU Gyro":
        waitTillSetComplete("Gyro", t)

        # Write the received payload to the data field
        for i in range(IMU_SAMPLES_PER_PACKET):
            data["Gyro"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], ENDIAN, signed=True)
            data["Gyro"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], ENDIAN, signed=True)
            data["Gyro"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], ENDIAN, signed=True)

    elif handles[type] == "IMU Magnetometer":
        waitTillSetComplete("Mag", t)

        # Write the received payload to the data field
        for i in range(IMU_SAMPLES_PER_PACKET):
            data["Mag"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], ENDIAN, signed=True)
            data["Mag"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], ENDIAN, signed=True)
            data["Mag"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], ENDIAN, signed=True)

    elif handles[type] == "Analog":
        waitTillSetComplete("Analog", t)

        def valToV(val):
            return (val << 6) / 1e6

        for i in range(ANALOG_SAMPLES_PER_PACKET):
            data["Analog"][0][i] = valToV(int.from_bytes(payload[(4 * i) : (4 * i + 2)], ENDIAN, signed=False))
            data["Analog"][1][i] = valToV(int.from_bytes(payload[(4 * i + 2) : (4 * i + 4)], ENDIAN, signed=False))

        # logger.info(data["Analog"][0][0])


stop = False


def read_packets(ser):
    global stop
    while not stop:
        r = ser.read()  # init read data from serial port
        if len(r) == 0:
            continue

        if r[0] == PACKET_KEY:
            pack_type = int.from_bytes(ser.read(), ENDIAN)
            length = int.from_bytes(ser.read(), ENDIAN)
            payload = ser.read(length)

            if pack_type == TYPE_HANDLE_DEF:
                parseHandleDef(payload)
            else:
                parseSensorPacket(pack_type, payload)  # Parse data from serial port

    for type in files:
        files[type].close()


#  Define and create folder and filenames
folderString = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
os.mkdir(folderString)
files["Mics"] = open(folderString + "/mics.csv", "w")
files["Baros"] = open(folderString + "/baros.csv", "w")
files["Acc"] = open(folderString + "/acc.csv", "w")
files["Gyro"] = open(folderString + "/gyro.csv", "w")
files["Mag"] = open(folderString + "/mag.csv", "w")
files["Analog"] = open(folderString + "/analog.csv", "w")

ser = serial.Serial("COM9", BAUDRATE)  # open serial port
ser.set_buffer_size(rx_size=100000, tx_size=1280)

start_new_thread(read_packets, (ser,))  # thread that will parse serial data and write it to files

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
