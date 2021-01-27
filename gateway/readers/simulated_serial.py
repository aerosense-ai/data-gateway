from dummy_serial import DummySerial, random_bytes


def generate_packet():
    """Generates packet data consistent with aerosense instrument (although actual values are meaningless)
    """
    # TODO
    return random_bytes(8)


class SimulatedSerial(DummySerial):
    """ A dummy serial port which generates mocked aerosense data

    Instantiate SimulatedSerial as you would a normal pyserial.Serial object:
    ```
    simulated = SimulatedSerial(
        port=123,
        baudrate=12345,
    )
    ```

    See pyserial.Serial for available constructor arguments.

    """

    def __init__(self, *args, **kwargs):
        """ Constructor for SimulatedSerial
        """
        super().__init__(*args, responses=self._generate_response, **kwargs)

    @staticmethod
    def _generate_response():
        return generate_packet()
