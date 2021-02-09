import logging


logger = logging.getLogger(__name__)


DEFAULT_CONFIGURATION = {}

# TODO Document and configure all of these

# ser.set_buffer_size(rx_size=100000, tx_size=1280)
# MICS_FREQ = 5000
# MICS_BM = 0x3FF
# BAROS_FREQ = 100
# BAROS_BM = 0x3FF
# ACC_FREQ = 100
# ACC_RANGE = 16
# GYRO_FREQ = 100
# GYRO_RANGE = 2000
# ANALOG_FREQ = 16384
#
# BAUDRATE = 2300000
# ENDIAN = 'little'
# MAX_TIMESTAMP_SLACK = 5e-3  # 5ms
# MAX_PERIOD_DRIFT = 0.02     # 2% difference between IMU clock and CPU clock allowed
#
# PACKET_KEY = 0xFE
#
# TYPE_HANDLE_DEF = 0xFF
#
# handles = {
#     34: "Baro group 0",
#     36: "Baro group 1",
#     38: "Baro group 2",
#     40: "Baro group 3",
#     42: "Baro group 4",
#     44: "Baro group 5",
#     46: "Baro group 6",
#     48: "Baro group 7",
#     50: "Baro group 8",
#     52: "Baro group 9",
#     54: "Mic 0",
#     56: "Mic 1",
#     58: "Mic 2",
#     60: "Mic 3",
#     62: "Mic 4",
#     64: "Mic 5",
#     66: "Mic 6",
#     68: "Mic 7",
#     70: "Mic 8",
#     72: "Mic 9",
#     74: "IMU Accel",
#     76: "IMU Gyro",
#     78: "IMU Magnetometer",
#     80: "Analog"
# }


class Gateway:
    """Gateway process manager

    This'll get rendered in the auto generated docs. If Kanye could see this he'd rap about how great it is.

    """

    def __init__(self, configuration=None, **kwargs):
        """Instantiate and configure gateway server"""
        # TODO use the helpers in twined to load configurations from src, file or dict
        self.configuration = {**DEFAULT_CONFIGURATION, **configuration}

    def _connect(self):
        """Connect to the ingress service
        Establishes a persistent, reconnecting websocket connection to the ingress
        """
        pass

    def _dump(self):
        """Method to show how to handle an error"""

    def start(self):
        """Start the reader service and the uploader service"""


# TODO SAFE STOP() TO CLOSE SOCKETS AND FILES ON KEYBOARD INTERRUPT

# Start the gateway service on running this script
if __name__ == "__main__":

    gateway = Gateway()

    gateway.start()
