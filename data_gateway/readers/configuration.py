class Configuration:
    def __init__(
        self,
        mics_freq=5000,
        mics_bm=0x3FF,
        baros_freq=100,
        baros_bm=0x3FF,
        acc_freq=100,
        acc_range=16,
        gyro_freq=100,
        gyro_range=2000,
        analog_freq=16384,
        serial_port="COM9",
        serial_buffer_rx_size=100000,
        serial_buffer_tx_size=1280,
        baudrate=2300000,
        endian="little",
        max_timestamp_slack=5e-3,  # 5ms
        max_period_drift=0.02,  # 2% difference between IMU clock and CPU clock allowed
        packet_key=0xFE,
        type_handle_def=0xFF,
        mics_samples_per_packet=120,
        baros_packet_size=60,
        baros_group_size=4,
        imu_samples_per_packet=int(240 / 2 / 3),
        analog_samples_per_packet=60,
        baros_samples_per_packet=None,
        default_handles=None,
        samples_per_packet=None,
        n_meas_qty=None,
        period=None,
        user_data=None,
    ):
        self.mics_freq = mics_freq
        self.mics_bm = mics_bm
        self.baros_freq = baros_freq
        self.baros_bm = baros_bm
        self.acc_freq = acc_freq
        self.acc_range = acc_range
        self.gyro_freq = gyro_freq
        self.gyro_range = gyro_range
        self.analog_freq = analog_freq
        self.serial_port = serial_port
        self.serial_buffer_rx_size = serial_buffer_rx_size
        self.serial_buffer_tx_size = serial_buffer_tx_size
        self.baudrate = baudrate
        self.endian = endian
        self.max_timestamp_slack = max_timestamp_slack
        self.max_period_drift = max_period_drift
        self.packet_key = packet_key
        self.type_handle_def = type_handle_def
        self.mics_samples_per_packet = mics_samples_per_packet
        self.baros_packet_size = baros_packet_size
        self.baros_group_size = baros_group_size
        self.imu_samples_per_packet = imu_samples_per_packet
        self.analog_samples_per_packet = analog_samples_per_packet
        self.baros_samples_per_packet = baros_samples_per_packet or int(baros_packet_size / baros_group_size)

        self.default_handles = default_handles or {
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

        self.samples_per_packet = samples_per_packet or {
            "Mics": self.mics_samples_per_packet,
            "Baros": self.baros_samples_per_packet,
            "Acc": self.imu_samples_per_packet,
            "Gyro": self.imu_samples_per_packet,
            "Mag": self.imu_samples_per_packet,
            "Analog": self.analog_samples_per_packet,
        }

        self.n_meas_qty = n_meas_qty or {
            "Mics": 10,
            "Baros": 40,
            "Acc": 3,
            "Gyro": 3,
            "Mag": 3,
            "Analog": 2,
        }

        self.period = period or {
            "Mics": 1 / self.mics_freq,
            "Baros": 1 / self.baros_freq,
            "Acc": 1 / self.acc_freq,
            "Gyro": 1 / self.gyro_freq,
            "Mag": 1 / 12.5,
            "Analog": 1 / self.analog_freq,
        }

        self.user_data = user_data or {}

    def __getattr__(self, item):
        return vars(self)[item]

    @classmethod
    def from_dict(cls, dictionary):
        """Construct a configuration from a dictionary. Note that all the configuration values are required - the
        construction will fail if any are missing.

        :param dict dictionary:
        :return Configuration:
        """
        return cls(**{attribute_name: dictionary[attribute_name] for attribute_name in vars(Configuration())})
