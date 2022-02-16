from . import exceptions


__all__ = ("exceptions",)
MICROPHONE_SENSOR_NAME = "Mics"


def stop_gateway(logger, stop_signal):
    """Stop the gateway's multiple processes and threads by sending the stop signal.

    :param logging.Logger logger: a logger to log that the stop signal has been sent
    :param multiprocessing.Value stop_signal: a value of 0 means don't stop; a value of 1 means stop
    :return None:
    """
    logger.info("Stopping gateway.")
    stop_signal.value = 1
