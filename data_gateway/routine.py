import multiprocessing
import sched
import time


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
        self.action = self._wrap_action_with_logger(action)
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
        """Send the commands to the action after the given delays, repeating if a period was given.

        :return None:
        """
        scheduler = sched.scheduler(time.perf_counter)
        start_time = time.perf_counter()

        while stop_signal.value == 0:
            cycle_start_time = time.perf_counter()

            for command, delay in self.commands:
                scheduler.enter(delay=delay, priority=1, action=self.action, argument=(command,))

            scheduler.run(blocking=True)

            if self.period is None:
                logger.info("Sending stop signal.")
                stop_signal.value = 1
                return

            elapsed_time = time.perf_counter() - cycle_start_time
            time.sleep(self.period - elapsed_time)

            if self.stop_after:
                if time.perf_counter() - start_time >= self.stop_after:
                    logger.info("Sending stop signal.")
                    stop_signal.value = 1
                    return

    def _wrap_action_with_logger(self, action):
        """Wrap the given action so that when it's run on a command, the command is logged.

        :param callable action: action for handling command
        :return callable: action with logging included
        """

        def action_with_logging(command):
            """Run the action on the command and log that it was run.

            :param str command:
            :return None:
            """
            action(command)
            logger.info("Routine ran %r command.", command)

        return action_with_logging
