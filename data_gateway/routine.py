import multiprocessing
import sched
import time

from data_gateway import stop_gateway


logger = multiprocessing.get_logger()


class Routine:
    """A routine of string commands to give to the action after the given delays. A period can be given for repetition
    of the set of commands, as well as a time to stop repeating after. If no period is given, the commands are run once.
    All delays for commands are counted from when the `run` method is executed, and must each be less than the period
    if one is given.

    :param list(tuple(str, float)) commands: a list of command names and the delay in seconds after which to send them
    :param callable action: a callable taking the command name as a single argument
    :param float|None period: the period in seconds at which to repeat the set of commands
    :param float|None stop_after: the time in seconds to stop repeating the set of commands; if this is `None`, the commands will repeat periodically forever
    :return None:
    """

    def __init__(self, commands, action, period=None, stop_after=None):
        self.commands = commands
        self.action = action
        self.period = period
        self.stop_after = stop_after

        if self.period:
            for command, delay in self.commands:
                if delay > self.period:
                    raise ValueError("The delay for each command in the routine should be less than the period.")

            if self.stop_after:
                if self.stop_after < self.period:
                    raise ValueError("The 'stop after' time must be greater than the period.")

        else:
            if self.stop_after:
                logger.warning("The `stop_after` parameter is ignored unless `period` is also given.")

    def run(self, stop_signal):
        """Send the commands to the action after the given delays, repeating if a period was given. The routine will
        stop before the next run if the stop signal is received (i.e. if the `stop_signal.value` is set to 1 in another
        process).

        :param multiprocessing.Value stop_signal: a value of 0 means don't stop; a value of 1 means stop
        :return None:
        """
        try:
            scheduler = sched.scheduler(time.perf_counter)
            start_time = time.perf_counter()

            while stop_signal.value == 0:
                cycle_start_time = time.perf_counter()

                for command, delay in self.commands:
                    scheduler.enter(delay=delay, priority=1, action=self.action, argument=(command,))

                    # If a command is "stop", schedule stopping the gateway and then schedule no further commands.
                    if command == "stop":
                        scheduler.enter(delay=delay, priority=1, action=stop_gateway, argument=(logger, stop_signal))
                        break

                scheduler.run(blocking=True)

                if self.period is None:
                    logger.info("Non-periodic routine finished.")
                    return

                elapsed_time = time.perf_counter() - cycle_start_time
                time.sleep(self.period - elapsed_time)

                if self.stop_after:
                    if time.perf_counter() - start_time >= self.stop_after:
                        logger.info("Periodic routine stopped after given timeout of %ss.", self.stop_after)
                        return

        except Exception as e:
            stop_gateway(logger, stop_signal)
            raise e
