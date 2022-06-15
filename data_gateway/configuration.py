DEFAULT_SENSOR_NAMES = (
    [
        "Mics",
        "Baros_P",
        "Baros_T",
        "Diff_Baros",
        "Acc",
        "Gyro",
        "Mag",
        "Analog Vbat",
        "Constat",
    ],
)

DEFAULT_DEFAULT_HANDLES = {
    "34": "Abs. baros",
    "36": "Diff. baros",
    "38": "Mic 0",
    "40": "Mic 1",
    "42": "IMU Accel",
    "44": "IMU Gyro",
    "46": "IMU Magnetometer",
    "48": "Analog1",
    "50": "Analog2",
    "52": "Constat",
    "54": "Cmd Decline",
    "56": "Sleep State",
    "58": "Info Message",
}

DEFAULT_DECLINE_REASONS = {
    "0": "Bad block detection ongoing",
    "1": "Task already registered, cannot register again",
    "2": "Task is not registered, cannot de-register",
    "3": "Connection Parameter update unfinished",
}

DEFAULT_SLEEP_STATES = {"0": "Exiting sleep", "1": "Entering sleep"}

DEFAULT_INFO_TYPES = {"0": "Battery info"}

DEFAULT_SAMPLES_PER_PACKET = {
    "Mics": 8,
    "Diff_Baros": 24,
    "Baros_P": 1,
    "Baros_T": 1,
    "Acc": 40,  # IMU, int(240 / 2 / 3)
    "Gyro": 40,  # IMU, int(240 / 2 / 3)
    "Mag": 40,  # IMU, int(240 / 2 / 3)
    "Analog Vbat": 60,
    "Constat": 24,
}


DEFAULT_SENSOR_CONVERSION_CONSTANTS = {
    "Mics": 1,
    "Diff_Baros": 1,
    "Baros_P": 40.96,
    "Baros_T": 100,
    "Acc": 1,
    "Gyro": 1,
    "Mag": 1,
    "Analog Vbat": 1,
    "Constat": 1,
}

DEFAULT_SENSOR_COMMANDS = {
    "start": ["startBaros", "startDiffBaros", "startIMU", "startMics"],
    "stop": ["stopBaros", "stopDiffBaros", "stopIMU", "stopMics"],
    "configuration": ["configBaros", "configAccel", "configGyro", "configMics"],
    "utilities": [
        "getBattery",
        "setConnInterval",
        "tpcBoostIncrease",
        "tpcBoostDecrease",
        "tpcBoostHeapMemThr1",
        "tpcBoostHeapMemThr2",
        "tpcBoostHeapMemThr4",
    ],
}

DEFAULT_NUMBER_OF_SENSORS = {
    "Mics": 10,
    "Baros_P": 40,
    "Baros_T": 40,
    "Diff_Baros": 5,
    "Acc": 3,
    "Gyro": 3,
    "Mag": 3,
    "Analog Vbat": 1,
    "Constat": 4,
}

DEFAULT_SESSION = {
    "label": None,
}


class GatewayConfiguration:
    """A data class containing configured/default values for the gateway receiver

    :param float baudrate: serial port baud rate
    :param Literal["little", "big"] endian: one of "little" or "big"
    :param str installation_reference: A unique reference (id) for the current installation
    :param float latitude: The latitude of the turbine in WGS84 coordinate system
    :param float longitude: The longitude of the turbine in WGS84 coordinate system
    :param int packet_key_offset: The value from which each node's packet key is calculated (packet_key = node_id + packet_key_offset)
    :param str receiver_firmware_version: The version ofthe firmware running on the gateway receiver, if known.
    :param int serial_buffer_rx_size: serial receiving buffer size in bytes
    :param int serial_buffer_tx_size: serial transmitting buffer size in bytes
    :param str turbine_id: A unique id for the turbine on which this is installed
    :return None:
    """

    def __init__(
        self,
        baudrate=2300000,
        endian="little",
        installation_reference="unknown",
        latitude=0,
        longitude=0,
        packet_key_offset=245,
        receiver_firmware_version="unknown",
        serial_buffer_rx_size=4095,
        serial_buffer_tx_size=1280,
        turbine_id="unknown",
    ):
        self.baudrate = baudrate
        self.endian = endian
        self.installation_reference = installation_reference
        self.latitude = latitude
        self.longitude = longitude
        self.serial_buffer_rx_size = serial_buffer_rx_size
        self.serial_buffer_tx_size = serial_buffer_tx_size
        self.turbine_id = turbine_id
        self.receiver_firmware_version = receiver_firmware_version
        self.packet_key_offset = packet_key_offset


class NodeConfiguration:
    """A data class containing configured/default values for a sensor node
    :param float acc_freq: accelerometers sampling frequency
    :param float acc_range: TODO nobody seems to know...
    :param float analog_freq: analog sensors sampling frequency
    :param float baros_freq: barometers sampling frequency
    :param float baros_bm: TODO nobody seems to know...
    :param float diff_baros_freq: differential barometers sampling frequency
    :param str blade_id: The id of the blade on which the node is mounted, if known
    :param float constat_period: period of incoming connection statistic parameters in ms
    :param float gyro_freq: gyrometers sampling frequency
    :param float gyro_range: TODO nobody seems to know...
    :param float max_period_drift: TODO   # 2% difference between IMU clock and CPU clock allowed
    :param float max_timestamp_slack: TODO   # 5ms
    :param float mics_bm: TODO nobody seems to know...
    :param float mics_freq: microphones sampling frequency
    :param str node_firmware_version: The verison of the firmware on the node, if known.
    :param int type_handle_def: TODO
    :param dict|None default_handles: Map of the default handles which a node will use to communicate packet type (the expected contents of packet payload). These are defaults, as they may be altered on the fly by a node.
    :param dict|None number_of_sensors: A map for each sensor, giving the number of samples expected from that sensor
    :param dict|None samples_per_packet: A map for each sensor, giving the number of samples sent in a packet from that sensor
    :param dict|None sensor_commands:
    :param list|None sensor_names: List of sensors present on the measurement node
    :return None:
    """

    def __init__(
        self,
        acc_freq=100,
        acc_range=16,
        analog_freq=16384,
        baros_bm=0x3FF,
        baros_freq=100,
        blade_id="unknown",
        constat_period=45,
        decline_reason=None,
        diff_baros_freq=1000,
        default_handles=None,
        gyro_freq=100,
        gyro_range=2000,
        info_type=None,
        mag_freq=12.5,
        mics_freq=15625,
        mics_bm=0x3FF,
        max_timestamp_slack=5e-3,
        max_period_drift=0.02,
        node_firmware_version="unknown",
        number_of_sensors=None,
        samples_per_packet=None,
        sensor_commands=None,
        sensor_conversion_constants=None,
        sensor_coordinates=None,
        sensor_names=None,
        sleep_state=None,
        type_handle_def=0xFF,
    ):
        # Set kwargs as attributes directly
        self.acc_freq = acc_freq
        self.acc_range = acc_range
        self.analog_freq = analog_freq
        self.baros_bm = baros_bm
        self.baros_freq = baros_freq
        self.blade_id = blade_id
        self.constat_period = constat_period
        self.diff_baros_freq = diff_baros_freq
        self.gyro_freq = gyro_freq
        self.gyro_range = gyro_range
        self.mag_freq = mag_freq
        self.max_timestamp_slack = max_timestamp_slack
        self.max_period_drift = max_period_drift
        self.mics_bm = mics_bm
        self.mics_freq = mics_freq
        self.node_firmware_version = node_firmware_version
        self.type_handle_def = type_handle_def

        # Set default dictionaries
        self.decline_reason = decline_reason or DEFAULT_DECLINE_REASONS
        self.default_handles = default_handles or DEFAULT_DEFAULT_HANDLES
        self.info_type = info_type or DEFAULT_INFO_TYPES
        self.number_of_sensors = number_of_sensors or DEFAULT_NUMBER_OF_SENSORS
        self.samples_per_packet = samples_per_packet or DEFAULT_SAMPLES_PER_PACKET
        self.sensor_commands = sensor_commands or DEFAULT_SENSOR_COMMANDS
        self.sensor_conversion_constants = sensor_conversion_constants or DEFAULT_SENSOR_CONVERSION_CONSTANTS
        self.sensor_names = sensor_names or DEFAULT_SENSOR_NAMES
        self.sleep_state = sleep_state or DEFAULT_SLEEP_STATES

        # Set calculated defaults
        self.sensor_coordinates = sensor_coordinates or self._get_default_sensor_coordinates()

        # Ensure conversion constants are consistent
        self._expand_sensor_conversion_constants()

        # Validate the final configuration
        self._check()

    def get_periods(self):
        """Return a dict of periods (in s) for each of the sensors, computed from the sensor frequencies"""
        return {
            "Mics": 1 / self.mics_freq,
            "Baros_P": 1 / self.baros_freq,
            "Baros_T": 1 / self.baros_freq,
            "Diff_Baros": 1 / self.diff_baros_freq,
            "Acc": 1 / self.acc_freq,
            "Gyro": 1 / self.gyro_freq,
            "Mag": 1 / self.mag_freq,
            "Analog Vbat": 1 / self.analog_freq,
            "Constat": self.constat_period / 1000,
        }

    def to_dict(self):
        """Serialise the configuration to a dictionary."""
        return vars(self)

    def _get_default_sensor_coordinates(self):
        return {
            sensor_name: [(0, 0, 0)] * number_of_sensors
            for sensor_name, number_of_sensors in self.number_of_sensors.items()
        }

    def _check(self):
        """Serialise self to JSON then make sure it matches a schema"""
        # NOT IMPLEMENTED YET
        # See https://github.com/aerosense-ai/data-gateway/issues/18

    def _expand_sensor_conversion_constants(self):
        """Expand the sensor conversion constant to arrays of the correct size

        This means that sensor conversion constants can be given as single values rather than lists.
        Passing a full list of adjusted values is still possible, to calibrate individual samples.
        """
        unconverted = self.sensor_conversion_constants
        converted = dict()
        for key, value in unconverted.items():
            number_of_samples = self.number_of_sensors[key]
            if isinstance(value, (int, float)):
                converted[key] = [value] * number_of_samples
            elif isinstance(value, list):
                if len(value) != number_of_samples:
                    raise ValueError(
                        f"If you give a list of conversion constants for {key}, it must be the same length as the number of samples you expect from that sensor (got length {len(value)}, require length {number_of_samples})"
                    )
                converted[key] = value
            else:
                raise ValueError(f"Unknown sensor conversion constant value {value}")

        self.sensor_conversion_constants = converted


class Configuration:
    """Configuration class for gateway, node and session configuration data.

    :param dict|None gateway: A dict of values used to customise the gateway configuration
    :param dict|None nodes: A dict of dicts, keyed by node_id, used to customise the configuration for each node
    :param dict|None session: A dict of metadata about the current session of the gateway provided by the user
    :return None:
    """

    def __init__(
        self,
        gateway=None,
        nodes=None,
        session=None,
        **kwargs,
    ):
        # Set up the gateway configuration
        gateway_configuration = gateway or {}
        self.gateway = GatewayConfiguration(**gateway_configuration)

        if len(kwargs) > 0:
            raise ValueError(
                "Properties other than 'gateway', 'nodes' and 'session' passed to Configuration. Are you using an "
                "old-format configuration file?"
            )

        # Set up a single-node default in the absence of any nodes at all
        self.nodes = {}

        if nodes is None:
            self.nodes[0] = NodeConfiguration()
        else:
            for key, value in nodes.items():
                node_id = int(key)
                self.nodes[node_id] = NodeConfiguration(**value)

        # Set up the session-specific data as empty
        self.session = session or DEFAULT_SESSION

    def get_packet_key(self, node_id, as_bytes=False):
        """Return the packet key for a given node, computed from the packet key offset
        :param int node_id: The node ID for which you want the packet key
        :param bool as_bytes: Convert the package key to the bytes representation
        :return int|bytes: The packet key for a given node_id
        """
        packet_key = self.gateway.packet_key_offset + node_id
        if as_bytes:
            packet_key = packet_key.to_bytes(1, self.gateway.endian)

        return packet_key

    @property
    def node_ids(self):
        """Access a list of node ids in the current configuration
        :return list: A list of node ids
        """
        return list(self.nodes)

    def to_dict(self):
        """Serialise the configuration to a dictionary."""
        return vars(self)
