import sched
import time


class Routine:
    """A routine of commands to give to the action after the given delays. A period can be given for repetition of the
    set of commands, as well as a time to stop repeating after. If no period is given, the commands are run once.

    :param list(dict(str, float)) commands: dictionaries with the command name as the key and the delay as the value
    :param callable action: a function or method taking the command name as a single argument
    :param float|None period: the time in seconds to repeat the set of commands
    :param float|None stop_after: the time in seconds to stop repeating the set of commands
    :return None:
    """

    def __init__(self, commands, action, period=None, stop_after=None):
        self.commands = commands
        self.action = action
        self.period = period
        self.stop_after = stop_after

        if self.period:
            for command, delay in self.commands.items():
                if delay > self.period:
                    raise ValueError("The delay for each command in the routine should be less than the period.")

    def run(self):
        """Send the commands to the action after the given delays, repeating if a period was given.

        :return None:
        """
        scheduler = sched.scheduler(time.perf_counter)
        start_time = time.perf_counter()

        while True:
            cycle_start_time = time.perf_counter()

            for command, delay in self.commands.items():
                scheduler.enter(delay=delay, priority=1, action=self.action, argument=(command,))

            scheduler.run(blocking=True)

            if self.period is None:
                break

            elapsed_time = time.perf_counter() - cycle_start_time
            time.sleep(self.period - elapsed_time)

            if self.stop_after:
                if time.perf_counter() - start_time > self.stop_after:
                    break
