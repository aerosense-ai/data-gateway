import multiprocessing
import time
from unittest import TestCase
from unittest.mock import patch

from data_gateway.routine import Routine


def create_record_commands_action():
    """Create a list in which commands will be recorded when the `record_commands` function is given as an action to
    the routine.

    :return (list, callable): the list that actions are recorded in, and the function that causes them to be recorded
    """
    recorded_commands = []

    def record_commands(command):
        recorded_commands.append((command, time.perf_counter()))

    return recorded_commands, record_commands


class TestRoutine(TestCase):
    def test_routine_with_no_period_runs_commands_once(self):
        """Test that commands can be scheduled to run once when a period isn't given."""
        recorded_commands, record_commands = create_record_commands_action()

        routine = Routine(
            commands=[("first-command", 0.1), ("second-command", 0.3)],
            action=record_commands,
        )

        start_time = time.perf_counter()
        routine.run(stop_signal=multiprocessing.Value("i", 0))

        self.assertEqual(recorded_commands[0][0], "first-command")
        self.assertAlmostEqual(recorded_commands[0][1], start_time + 0.1, delta=0.2)

        self.assertEqual(recorded_commands[1][0], "second-command")
        self.assertAlmostEqual(recorded_commands[1][1], start_time + 0.3, delta=0.2)

    def test_error_raised_if_any_delay_is_greater_than_period(self):
        """Test that an error is raised if any of the command delays is greater than the period."""
        with self.assertRaises(ValueError):
            Routine(
                commands=[("first-command", 10), ("second-command", 0.3)],
                action=None,
                period=1,
            )

    def test_error_raised_if_stop_after_time_is_less_than_period(self):
        """Test that an error is raised if the `stop_after` time is less than the period."""
        with self.assertRaises(ValueError):
            Routine(
                commands=[("first-command", 0.1), ("second-command", 0.3)],
                action=None,
                period=1,
                stop_after=0.5,
            )

    def test_warning_raised_if_stop_after_time_provided_without_a_period(self):
        """Test that a warning is raised if the `stop_after` time is provided without a period."""
        with patch("data_gateway.routine.logger") as mock_logger:
            Routine(
                commands=[("first-command", 10), ("second-command", 0.3)],
                action=None,
                stop_after=0.5,
            )

        self.assertEqual(
            mock_logger.warning.call_args_list[0].args[0],
            "The `stop_after` parameter is ignored unless `period` is also given.",
        )

    def test_routine_with_period(self):
        """Test that commands can be scheduled to repeat at the given period and then stop after a certain time."""
        recorded_commands, record_commands = create_record_commands_action()

        routine = Routine(
            commands=[("first-command", 0.1), ("second-command", 0.3)],
            action=record_commands,
            period=0.4,
            stop_after=1,
        )

        start_time = time.perf_counter()
        routine.run(stop_signal=multiprocessing.Value("i", 0))

        self.assertEqual(recorded_commands[0][0], "first-command")
        self.assertAlmostEqual(recorded_commands[0][1], start_time + 0.1, delta=0.2)

        self.assertEqual(recorded_commands[1][0], "second-command")
        self.assertAlmostEqual(recorded_commands[1][1], start_time + 0.3, delta=0.2)

        self.assertEqual(recorded_commands[2][0], "first-command")
        self.assertAlmostEqual(recorded_commands[2][1], start_time + 0.1 + routine.period, delta=0.2)

        self.assertEqual(recorded_commands[3][0], "second-command")
        self.assertAlmostEqual(recorded_commands[3][1], start_time + 0.3 + routine.period, delta=0.2)

    def test_routine_only_runs_until_stop_command(self):
        """Test that a routine only runs until the "stop" command is received."""
        recorded_commands, record_commands = create_record_commands_action()

        routine = Routine(
            commands=[("first-command", 0.1), ("stop", 0.3), ("command-after-stop", 0.5)],
            action=record_commands,
        )

        stop_signal = multiprocessing.Value("i", 0)
        start_time = time.perf_counter()

        routine.run(stop_signal=stop_signal)

        # Check that only the first two commands (i.e. up until the `stop` command) are scheduled and carried out.
        self.assertEqual(len(recorded_commands), 2)

        self.assertEqual(recorded_commands[0][0], "first-command")
        self.assertAlmostEqual(recorded_commands[0][1], start_time + 0.1, delta=0.2)

        self.assertEqual(recorded_commands[1][0], "stop")
        self.assertAlmostEqual(recorded_commands[1][1], start_time + 0.3, delta=0.2)

        # Check that the stop signal has been sent.
        self.assertEqual(stop_signal.value, 1)
