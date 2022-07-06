# Works with python 3.6.5


import os
import sys
import time
from _thread import start_new_thread
from datetime import datetime

import serial


MICS_FREQ = 15625

MICS_BM = 0x3FF

BAROS_FREQ = 100

DIFF_BAROS_FREQ = 1000

BAROS_BM = 0x3FF

ACC_FREQ = 100

ACC_RANGE = 16

GYRO_FREQ = 100

GYRO_RANGE = 2000

ANALOG_FREQ = 16384

NUM_NODES = 6


# for interaction with the base station

mode = 0  # mode = 0: Linux, mode = 1: Windows


# for interaction with the aerosense debug

# mode=1


if mode == 0:

    BAUDRATE = 2300000

    PORT = "/dev/ttyACM0"

elif mode == 1:

    BAUDRATE = 2300000

    PORT = "COM12"


ENDIAN = "little"

MAX_TIMESTAMP_SLACK = 5e-3  # 5ms

MAX_PERIOD_DRIFT = 0.02  # 2% difference between IMU clock and CPU clock allowed


PACKET_KEY = 0xFE


PACKET_KEY_OFFSET = 0xF5


TYPE_HANDLE_DEF = 0xFF


handles = {
    34: "Abs. baros",
    36: "Diff. baros",
    38: "Mic 0",
    40: "Mic 1",
    42: "IMU Accel",
    44: "IMU Gyro",
    46: "IMU Magnetometer",
    48: "Analog1",
    50: "Analog2",
    52: "Constat",
    54: "Cmd Decline",
    56: "Sleep State",
    58: "Remote Info Message",
    60: "Timestamp Packet 0",
    62: "Timestamp Packet 1",
    64: "Local Info Message",
}


decline_reason = {
    0: "Bad block detection ongoing",
    1: "Task already registered, cannot register again",
    2: "Task is not registered, cannot de-register",
    3: "Connection parameter update unfinished",
    4: "Not ready to sleep",
    5: "Not in sleep",
}


sleep_state = {0: "Exiting sleep", 1: "Entering sleep"}


remote_info = {0: "Battery info"}


local_info = {
    0: "Synchronization not ready as not every sensor node is connected or connection parameters are not the desired ones",
    1: "Time synchronization info",
    2: "Time sync exception",
    4: "Time sync already in sync",
    8: "Time sync alignment error",
    16: "Time sync coarse data time diff error",
    32: "Device not connected",
    64: "select message destination successful",
    128: "Time sync success",
    129: "Coarse sync finish",
    130: "time sync msg sent",
}


def errPrint(s):

    print("***** " + s + " *****")


def parseHandleDef(payload):

    startHandle = int.from_bytes(payload[0:1], ENDIAN)

    endHandle = int.from_bytes(payload[2:3], ENDIAN)

    print(startHandle, endHandle)

    if endHandle - startHandle == 30:

        handles = {
            startHandle + 2: "Abs. baros",
            startHandle + 4: "Diff. baros",
            startHandle + 6: "Mic 0",
            startHandle + 8: "Mic 1",
            startHandle + 10: "IMU Accel",
            startHandle + 12: "IMU Gyro",
            startHandle + 14: "IMU Magnetometer",
            startHandle + 16: "Analog1",
            startHandle + 18: "Analog2",
            startHandle + 20: "Constat",
            startHandle + 22: "Cmd Decline",
            startHandle + 24: "Sleep State",
            startHandle + 26: "Remote Info Message",
            startHandle + 28: "Timestamp Packet 0",
            startHandle + 30: "Timestamp Packet 1",
            startHandle + 32: "Local Info Message",
        }

        print("Successfully updated the handles")

    else:

        errPrint("Handle error: " + str(startHandle) + " " + str(endHandle))


files = {}


MICS_SAMPLES_PER_PACKET = 8

BAROS_SAMPLES_PER_PACKET = 1

IMU_SAMPLES_PER_PACKET = int(240 / 2 / 3)

ANALOG_SAMPLES_PER_PACKET = 60

DIFF_BAROS_SAMPLES_PER_PACKET = 24


samplesPerPacket = {
    "Mics": MICS_SAMPLES_PER_PACKET,
    "Baros_P": BAROS_SAMPLES_PER_PACKET,
    "Baros_T": BAROS_SAMPLES_PER_PACKET,
    "Acc": IMU_SAMPLES_PER_PACKET,
    "Gyro": IMU_SAMPLES_PER_PACKET,
    "Mag": IMU_SAMPLES_PER_PACKET,
    "Analog": ANALOG_SAMPLES_PER_PACKET,
    "Diff_Baros": DIFF_BAROS_SAMPLES_PER_PACKET,
}


nMeasQty = {
    "Mics": 10,
    "Baros_P": 40,
    "Baros_T": 40,
    "Acc": 3,
    "Gyro": 3,
    "Mag": 3,
    "Analog": 2,
    "Diff_Baros": 5,
}


data = {
    "Mics": [([0] * samplesPerPacket["Mics"]) for i in range(nMeasQty["Mics"])],
    "Baros_P": [([0] * samplesPerPacket["Baros_P"]) for i in range(nMeasQty["Baros_P"])],
    "Baros_T": [([0] * samplesPerPacket["Baros_T"]) for i in range(nMeasQty["Baros_T"])],
    "Acc": [([0] * samplesPerPacket["Acc"]) for i in range(nMeasQty["Acc"])],
    "Gyro": [([0] * samplesPerPacket["Gyro"]) for i in range(nMeasQty["Gyro"])],
    "Mag": [([0] * samplesPerPacket["Mag"]) for i in range(nMeasQty["Mag"])],
    "Analog": [([0] * samplesPerPacket["Analog"]) for i in range(nMeasQty["Analog"])],
    "Diff_Baros": [([0] * samplesPerPacket["Diff_Baros"]) for i in range(nMeasQty["Diff_Baros"])],
}


period = {
    "Mics": 1 / MICS_FREQ,
    "Baros_P": 1 / BAROS_FREQ,
    "Baros_T": 1 / BAROS_FREQ,
    "Acc": 1 / ACC_FREQ,
    "Gyro": 1 / GYRO_FREQ,
    "Mag": 1 / 12.5,
    "Analog": 1 / ANALOG_FREQ,
    "Diff_Baros": 1 / DIFF_BAROS_FREQ,
}


currentTimestamp = {"Mics": 0, "Baros_P": 0, "Baros_T": 0, "Acc": 0, "Gyro": 0, "Mag": 0, "Analog": 0, "Diff_Baros": 0}

prevIdealTimestamp = {
    "Mics": 0,
    "Baros_P": 0,
    "Baros_T": 0,
    "Acc": 0,
    "Gyro": 0,
    "Mag": 0,
    "Analog": 0,
    "Diff_Baros": 0,
}


def writeData(type, timestamp, period, node=1):  # timestamp in s

    n = len(data[type][0])  # number of samples

    for i in range(len(data[type][0])):  # iterate through all sample times

        time = timestamp - (n - i) * period

        files[node][type].write(str(time) + ",")

        for meas in data[type]:  # iterate through all measured quantities

            files[node][type].write(str(meas[i]) + ",")

        files[node][type].write("\n")


# The sensor data arrive packets that contain n samples from some sensors of the same type, e.g. one barometer packet contains 40 samples from 4 barometers each.

# For each sensor type (e.g. baro), this function waits until the packets from all sensors have arrived. Then it writes those to the .csv file.

# Since timestamps only come at a packet level, this function also interpolates the within-packet-timestamps


def waitTillSetComplete(type, t, node=1):  # timestamp in 1/(2**16) s

    if type == "Mics" or type == "Baros_P" or type == "Baros_T" or type == "Diff_Baros" or type == "Analog":

        # For those measurement types, the samples are inherently synchronized to the CPU time already.

        # The timestamps may be slightly off, so it takes the first one as a reference and then uses the following ones only to check if a packet has been dropped

        # Also, for mics and baros, there exist packet sets: Several packets arrive with the same timestamp

        if currentTimestamp[type] != 0:

            idealNewTimestamp = prevIdealTimestamp[type] + samplesPerPacket[type] * period[type] * (2 ** 16)

            if abs(idealNewTimestamp - currentTimestamp[type]) > MAX_TIMESTAMP_SLACK * (
                2 ** 16
            ):  # If at least one set (= one packet per mic/baro group) of packets was lost

                if prevIdealTimestamp[type] != 0 and type != "Mics":

                    print(
                        "Lost set of "
                        + type
                        + " packets: "
                        + str((currentTimestamp[type] - idealNewTimestamp) / (2 ** 16) * 1000)
                        + "ms gap"
                    )

                if type != "Mics":

                    idealNewTimestamp = currentTimestamp[type]

            writeData(type, idealNewTimestamp / (2 ** 16), period[type], node)

            data[type] = [([0] * samplesPerPacket[type]) for i in range(nMeasQty[type])]

            prevIdealTimestamp[type] = idealNewTimestamp

            currentTimestamp[type] = t

        else:

            if type == "Mics":

                prevIdealTimestamp[type] = t

            currentTimestamp[type] = t

            print("Received first set of " + type + " packets")

    else:  # The IMU values are not synchronized to the CPU time, so we simply always take the timestamp we have

        if currentTimestamp[type] != 0:

            per = period[type]

            if (
                prevIdealTimestamp[type] != 0
            ):  # If there is a previous timestamp, calculate the actual sampling period from the difference to the current timestamp

                per = (currentTimestamp[type] - prevIdealTimestamp[type]) / samplesPerPacket[type] / (2 ** 16)

                if (
                    abs(per - period[type]) / period[type] < MAX_PERIOD_DRIFT
                ):  # If the calculated period is reasonable, accept it. If not, most likely a packet got lost

                    period[type] = per

                else:

                    print(
                        "Lost "
                        + type
                        + " packet: "
                        + str((currentTimestamp[type] - prevIdealTimestamp[type]) / (2 ** 16) * 1000)
                        + "ms gap"
                    )

            else:

                print("Received first " + type + " packet")

            writeData(type, t / (2 ** 16), period[type], node)

        prevIdealTimestamp[type] = currentTimestamp[type]

        currentTimestamp[type] = t


def parseSensorPacket(type, len, payload, node=1):

    global mic_cnt

    if not type in handles:

        print("Received packet with unknown type: ", type)

        print("Payload len: ", len)

        #        print("Payload: ", int.from_bytes(payload, ENDIAN))

        return

    t = int.from_bytes(payload[240:244], ENDIAN, signed=False)  # Read timestamp from packet

    if handles[type] == "Abs. baros":

        waitTillSetComplete("Baros_P", t, node)

        waitTillSetComplete("Baros_T", t, node)

        # Write the received payload to the data field

        for i in range(BAROS_SAMPLES_PER_PACKET):

            for j in range(nMeasQty["Baros_P"]):

                bps = 6  # bytes per sample

                data["Baros_P"][j][i] = int.from_bytes(
                    payload[(bps * j) : (bps * j + 4)], ENDIAN, signed=False
                )  # /4096

                data["Baros_T"][j][i] = int.from_bytes(
                    payload[(bps * j + 4) : (bps * j + 6)], ENDIAN, signed=True
                )  # /100

    elif handles[type] == "Diff. baros":

        waitTillSetComplete("Diff_Baros", t, node)

        # int_payload = [x for x in payload]

        # print(int_payload)

        # Write the received payload to the data field

        for i in range(DIFF_BAROS_SAMPLES_PER_PACKET):

            for j in range(nMeasQty["Diff_Baros"]):

                bps = 2  # bytes per sample

                # this result depends on the sensor (multiply with the sensor max value to get the scaled result)

                # data["Diff_Baros"][j][i] = (int.from_bytes(payload[(bps*(nMeasQty["Diff_Baros"]*i+j)) : (bps*(nMeasQty["Diff_Baros"]*i+j)+bps)], ENDIAN, signed=False) - 6553)/(58982-6553)

                data["Diff_Baros"][j][i] = int.from_bytes(
                    payload[(bps * (nMeasQty["Diff_Baros"] * i + j)) : (bps * (nMeasQty["Diff_Baros"] * i + j) + bps)],
                    ENDIAN,
                    signed=False,
                )

    elif handles[type] == "Mic 0":

        waitTillSetComplete("Mics", t, node)

        bps = 3  # bytes per sample

        for i in range(MICS_SAMPLES_PER_PACKET // 2):

            for j in range(5):

                data["Mics"][j][2 * i] = int.from_bytes(
                    payload[(bps * j + 20 * bps * i) : (bps * j + 20 * bps * i + 3)], "big", signed=True
                )

                data["Mics"][j][2 * i + 1] = int.from_bytes(
                    payload[(bps * j + 20 * bps * i + 5 * bps) : (bps * j + 20 * bps * i + 3 + 5 * bps)],
                    "big",
                    signed=True,
                )

                data["Mics"][j + 5][2 * i] = int.from_bytes(
                    payload[(bps * j + 20 * bps * i + 10 * bps) : (bps * j + 20 * bps * i + 3 + 10 * bps)],
                    "big",
                    signed=True,
                )

                data["Mics"][j + 5][2 * i + 1] = int.from_bytes(
                    payload[(bps * j + 20 * bps * i + 15 * bps) : (bps * j + 20 * bps * i + 3 + 15 * bps)],
                    "big",
                    signed=True,
                )

    elif handles[type] == "Mic 1":

        if payload[0] == 1:

            print("Sensor reading from flash done")  # print("Mics reading done")

        elif payload[0] == 2:

            print("Flash erasing done")  # print("Mics erasing done")

        elif payload[0] == 3:

            print("Sensor started")  # print("Mics and or (diff)baros started")

    elif handles[type].startswith("IMU Accel"):

        waitTillSetComplete("Acc", t, node)

        # Write the received payload to the data field

        for i in range(IMU_SAMPLES_PER_PACKET):

            data["Acc"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], ENDIAN, signed=True)

            data["Acc"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], ENDIAN, signed=True)

            data["Acc"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], ENDIAN, signed=True)

    elif handles[type] == "IMU Gyro":

        waitTillSetComplete("Gyro", t, node)

        # Write the received payload to the data field

        for i in range(IMU_SAMPLES_PER_PACKET):

            data["Gyro"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], ENDIAN, signed=True)

            data["Gyro"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], ENDIAN, signed=True)

            data["Gyro"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], ENDIAN, signed=True)

    elif handles[type] == "IMU Magnetometer":

        waitTillSetComplete("Mag", t, node)

        # Write the received payload to the data field

        for i in range(IMU_SAMPLES_PER_PACKET):

            data["Mag"][0][i] = int.from_bytes(payload[(6 * i) : (6 * i + 2)], ENDIAN, signed=True)

            data["Mag"][1][i] = int.from_bytes(payload[(6 * i + 2) : (6 * i + 4)], ENDIAN, signed=True)

            data["Mag"][2][i] = int.from_bytes(payload[(6 * i + 4) : (6 * i + 6)], ENDIAN, signed=True)

    # elif handles[type] == "Analog":

    #     waitTillSetComplete("Analog", t)

    #     def valToV(val):

    #         return (val << 6) / 1e6

    #     for i in range(ANALOG_SAMPLES_PER_PACKET):

    #         data["Analog"][0][i] = valToV(int.from_bytes(payload[(4*i):(4*i+2)], ENDIAN, signed=False))

    #         data["Analog"][1][i] = valToV(int.from_bytes(payload[(4*i+2):(4*i+4)], ENDIAN, signed=False))

    # print(data["Analog"][0][0])

    elif handles[type] == "Constat":

        print(f"Node: {node}, Constat packet: %d" % (t / (2 ** 16)))

    elif handles[type] == "Cmd Decline":

        reason_index = int.from_bytes(payload, ENDIAN, signed=False)

        print("Command declined, " + decline_reason[reason_index])

    elif handles[type] == "Sleep State":

        state_index = int.from_bytes(payload, ENDIAN, signed=False)

        print("\n" + sleep_state[state_index] + "\n")

    #     elif handles[type] == "Info Message":

    #         info_index = int.from_bytes(payload[0:1], ENDIAN, signed=False)

    #         print(info_index)

    #         if info_type[info_index] == "Battery info":

    #             voltage = int.from_bytes(payload[1:5], ENDIAN, signed=False)

    #             cycle = int.from_bytes(payload[5:9], ENDIAN, signed=False)

    #             stateOfCharge = int.from_bytes(payload[9:13], ENDIAN, signed=False)

    #             print(f"Node: {node} \n Voltage : {voltage/1000000} v \n Cycle count: {cycle/100} \n State of charge: {stateOfCharge/256}%")

    #######################################################################################

    elif handles[type] == "Remote Info Message":

        info_index = int.from_bytes(payload[0:1], ENDIAN, signed=False)

        print(remote_info[info_index])

        if remote_info[info_index] == "Battery info":

            voltage = int.from_bytes(payload[1:5], ENDIAN, signed=False)

            cycle = int.from_bytes(payload[5:9], ENDIAN, signed=False)

            stateOfCharge = int.from_bytes(payload[9:13], ENDIAN, signed=False)

            print(
                f"Node: {node} \n Voltage : {voltage/1000000} v \n Cycle count: {cycle/100} \n State of charge: {stateOfCharge/256}%"
            )

    elif handles[type] == "Local Info Message":

        info_index = int.from_bytes(payload[0:1], ENDIAN, signed=False)

        print(local_info[info_index])

        if info_index == 130:

            print(int.from_bytes(payload[1:3], ENDIAN, signed=False))

        if local_info[info_index] == "Time synchronization info":

            info_type = int.from_bytes(payload[1:5], ENDIAN, signed=False)

            if info_type == 0:

                print("seq data")

                for i in range(15):

                    seqDataFile.write(str(int.from_bytes(payload[5 + i * 4 : 9 + i * 4], ENDIAN, signed=False)) + ",")

                for i in range(15, 18):

                    seqDataFile.write(str(int.from_bytes(payload[5 + i * 4 : 9 + i * 4], ENDIAN, signed=True)) + ",")

                seqDataFile.close()

            elif info_type == 1:

                print("central data")

                for i in range(60):

                    centralDataFile.write(
                        str(int.from_bytes(payload[5 + i * 4 : 9 + i * 4], ENDIAN, signed=False)) + ","
                    )

                    centralCnt = centralCnt + 1

                    if centralCnt == 187:

                        centralDataFile.close()

                        break

            elif info_type == 2:

                print("perif 0 data")

                for i in range(61):

                    perif0DataFile.write(
                        str(int.from_bytes(payload[5 + i * 4 : 9 + i * 4], ENDIAN, signed=False)) + ","
                    )

                perif0DataFile.close()

            elif info_type == 3:

                print("perif 1 data")

                for i in range(61):

                    perif1DataFile.write(
                        str(int.from_bytes(payload[5 + i * 4 : 9 + i * 4], ENDIAN, signed=False)) + ","
                    )

                perif1DataFile.close()

            elif info_type == 4:

                print("perif 2 data")

                for i in range(61):

                    perif2DataFile.write(
                        str(int.from_bytes(payload[5 + i * 4 : 9 + i * 4], ENDIAN, signed=False)) + ","
                    )

                perif2DataFile.close()

    elif handles[type] == "Timestamp Packet 0":

        print("timestamp packet", int(len / 4), len)

        for i in range(int(len / 4)):

            files["ts" + str(packet_source)].write(
                str(int.from_bytes(payload[i * 4 : (i + 1) * 4], ENDIAN, signed=False)) + ","
            )

        # files["sampleElapse"+str(packet_source)].close()

    elif handles[type] == "Timestamp Packet 1":

        print("time elapse packet", int(len / 4), len)

        for i in range(int(len / 4)):

            files["sampleElapse" + str(packet_source)].write(
                str(int.from_bytes(payload[i * 4 : (i + 1) * 4], ENDIAN, signed=False)) + ","
            )


#     else:

#         print("unknown handle %d", type)

#######################################################################################


stop = False


def read_packets(ser):

    global stop

    while not stop:

        r = ser.read()

        if len(r) == 0:

            continue

        # print(f"Got packet key {r[0]}, key-PACKET_KEY_OFFSET = {r[0]-PACKET_KEY_OFFSET}")

        if (r[0] == PACKET_KEY) or (((r[0] - PACKET_KEY_OFFSET) <= 5) & ((r[0] - PACKET_KEY_OFFSET) >= 0)):

            pack_type = int.from_bytes(ser.read(), ENDIAN)

            length = int.from_bytes(ser.read(), ENDIAN)

            payload = ser.read(length)

            # print(f"{time.time()}:  Got packet type {pack_type}")

            if pack_type == TYPE_HANDLE_DEF:

                parseHandleDef(payload)

                nextPacketStart = 0

                packetCnt = 0

            else:

                parseSensorPacket(pack_type, length, payload, r[0] - PACKET_KEY_OFFSET)

    for type in files:

        for i in range(NUM_NODES):

            files[i][type].close()


def writeHeaders():

    for i in range(NUM_NODES):

        files[i]["Mics"].write("Time (s),x,y,z\n")

        files[i]["Baros_P"].write("time,x,y,z\n")

        files[i]["Baros_T"].write("time,x,y,z\n")

        files[i]["Diff_Baros"].write('"time","baro0","baro1","baro2","baro3","baro4"\n')

        files[i]["Acc"].write('"time","x","y","z"\n')

        files[i]["Gyro"].write('"time","x","y","z"\n')

        files[i]["Mag"].write('"time","x","y","z"\n')

        files[i]["Analog"].write("time,x,y,z\n")


folderString = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")

os.mkdir(folderString)

for i in range(NUM_NODES):

    files[i] = {}

    files[i]["Mics"] = open(folderString + "/" + str(i) + "_mics.csv", "w")

    files["ts" + str(i)] = open(folderString + "/ts" + str(i) + ".csv", "w")

    files["sampleElapse" + str(i)] = open(folderString + "/sampleElapse" + str(i) + ".csv", "w")

    files[i]["Baros_P"] = open(folderString + "/" + str(i) + "_baros_p.csv", "w")

    files[i]["Baros_T"] = open(folderString + "/" + str(i) + "_baros_T.csv", "w")

    files[i]["Diff_Baros"] = open(folderString + "/" + str(i) + "_diff_baros.csv", "w")

    files[i]["Acc"] = open(folderString + "/" + str(i) + "_acc.csv", "w")

    files[i]["Gyro"] = open(folderString + "/" + str(i) + "_gyro.csv", "w")

    files[i]["Mag"] = open(folderString + "/" + str(i) + "_mag.csv", "w")

    files[i]["Analog"] = open(folderString + "/" + str(i) + "_analog.csv", "w")


seqDataFile = open(folderString + "/seqData.csv", "w")

centralDataFile = open(folderString + "/centralData.csv", "w")

perif0DataFile = open(folderString + "/perif0Data.csv", "w")

perif1DataFile = open(folderString + "/perif1Data.csv", "w")

perif2DataFile = open(folderString + "/perif2Data.csv", "w")


writeHeaders()


ser = serial.Serial(PORT, BAUDRATE)  # open serial port

# ser.set_buffer_size(rx_size = 100000, tx_size = 1280)


start_new_thread(read_packets, (ser,))


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

    if line == "saveFinish\n":

        ser.write("syncSensorFinish\n".encode("utf_8"))

        print("----command syncSensorFinish issued----")

    for i in range(NUM_NODES):

        files["ts" + str(i)].close()

        files["sampleElapse" + str(i)].close()

    if line == "stop\n":

        stop = True

        break

    else:

        ser.write(line.encode("utf_8"))

        print("----command " + line[:-1] + " issued----")
