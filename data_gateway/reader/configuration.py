class Configuration:
    """A data class containing the configuration values for the firmware and hardware used by the data gateway.

    :param float mics_freq: microphones sampling frequency
    :param float mics_bm: TODO
    :param float baros_freq: barometers sampling frequency
    :param float baros_bm: TODO
    :param float acc_freq: accelerometers sampling frequency
    :param float acc_range: TODO
    :param float gyro_freq: gyrometers sampling frequency
    :param float gyro_range: TODO
    :param float analog_freq: analog sensors sampling frequency
    :param float constat_period: period of incoming connection statistic parameters in ms
    :param str serial_port: name of the serial port
    :param int serial_buffer_rx_size: serial receiving buffer size in bytes
    :param int serial_buffer_tx_size: serial transmitting buffer size in bytes
    :param float baudrate: serial port baud rate
    :param str endian: one of "little" or "big"
    :param float max_timestamp_slack: TODO   # 5ms
    :param float max_period_drift: TODO   # 2% difference between IMU clock and CPU clock allowed
    :param int packet_key: TODO
    :param int type_handle_def: TODO
    :param int mics_samples_per_packet: number of samples per packet from microphones
    :param int baros_packet_size: TODO
    :param int baros_group_size: TODO
    :param int imu_samples_per_packet: TODO
    :param int analog_samples_per_packet: number of samples per packet from analog sensors
    :param int baros_samples_per_packet: number of samples per packet from barometers
    :param int constat_samples_per_packet: number of samples per packet from connection statistics
    :param dict|None default_handles: TODO
    :param dict|None samples_per_packet: TODO
    :param dict|None n_meas_qty: TODO
    :param dict|None period: TODO
    :param dict|None user_data: metadata about the current session of the gateway provided by the user
    :return None:
    """

    def __init__(
        self,
        mics_freq=15625,
        mics_bm=0x3FF,
        baros_freq=100,
        baros_bm=0x3FF,
        acc_freq=100,
        acc_range=16,
        gyro_freq=100,
        gyro_range=2000,
        analog_freq=10,
        constat_period=45,
        serial_port="COM9",
        serial_buffer_rx_size=100000,
        serial_buffer_tx_size=1280,
        baudrate=2300000,
        endian="little",
        max_timestamp_slack=5e-3,
        max_period_drift=0.02,
        packet_key=0xFE,
        type_handle_def=0xFF,
        mics_samples_per_packet=8,
        baros_samples_per_packet=1,
        imu_samples_per_packet=int(240 / 2 / 3),
        analog_samples_per_packet=60,
        constat_samples_per_packet=24,
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
        self.constat_period = constat_period
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
        self.imu_samples_per_packet = imu_samples_per_packet
        self.analog_samples_per_packet = analog_samples_per_packet
        self.baros_samples_per_packet = baros_samples_per_packet
        self.constat_samples_per_packet = constat_samples_per_packet

        self.default_handles = default_handles or {
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
        }

        self.samples_per_packet = samples_per_packet or {
            "Mics": self.mics_samples_per_packet,
            "Baros_P": self.baros_samples_per_packet,
            "Baros_T": self.baros_samples_per_packet,
            "Acc": self.imu_samples_per_packet,
            "Gyro": self.imu_samples_per_packet,
            "Mag": self.imu_samples_per_packet,
            "Analog Vbat": self.analog_samples_per_packet,
            "Constat": self.constat_samples_per_packet,
        }

        self.n_meas_qty = n_meas_qty or {
            "Mics": 10,
            "Baros_P": 40,
            "Baros_T": 40,
            "Acc": 3,
            "Gyro": 3,
            "Mag": 3,
            "Analog Vbat": 1,
            "Constat": 3,
        }

        self.period = period or {
            "Mics": 1 / self.mics_freq,
            "Baros_P": 1 / self.baros_freq,
            "Baros_T": 1 / self.baros_freq,
            "Acc": 1 / self.acc_freq,
            "Gyro": 1 / self.gyro_freq,
            "Mag": 1 / 12.5,
            "Analog Vbat": 1 / self.analog_freq,
            "Constat": self.constat_period / 1000,
        }

        self.user_data = user_data or {}

    @classmethod
    def from_dict(cls, dictionary):
        """Construct a configuration from a dictionary. Note that all the configuration values are required - the
        construction will fail if any are missing (i.e. default arguments are disabled for this alternative constructor)

        :param dict dictionary:
        :return Configuration:
        """
        return cls(**{attribute_name: dictionary[attribute_name] for attribute_name in vars(Configuration())})

    def to_dict(self):
        """Serialise the configuration to a dictionary."""
        return vars(self)
