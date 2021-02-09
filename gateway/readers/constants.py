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

period = {
    "Mics": 1 / MICS_FREQ,
    "Baros": 1 / BAROS_FREQ,
    "Acc": 1 / ACC_FREQ,
    "Gyro": 1 / GYRO_FREQ,
    "Mag": 1 / 12.5,
    "Analog": 1 / ANALOG_FREQ,
}
