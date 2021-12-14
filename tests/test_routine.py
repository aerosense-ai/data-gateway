import time
from unittest import TestCase

from data_gateway.routine import Routine


class TestRoutine(TestCase):
    def test_routine_with_no_period(self):
        """Test that commands can be scheduled to run once when a period isn't given."""
        recorded_commands = []

        def record_commands(command):
            recorded_commands.append((command, time.perf_counter()))

        routine = Routine(commands=[("first-command", 0.1), ("second-command", 0.3)], action=record_commands)

        start_time = time.perf_counter()
        routine.run()

        self.assertEqual(recorded_commands[0][0], "first-command")
        self.assertAlmostEqual(recorded_commands[0][1], start_time + 0.1, delta=0.1)

        self.assertEqual(recorded_commands[1][0], "second-command")
        self.assertAlmostEqual(recorded_commands[1][1], start_time + 0.3, delta=0.1)

    def test_error_raised_if_any_delay_is_greater_than_period(self):
        """Test that an error is raised if any of the command delays is greater than the period."""
        with self.assertRaises(ValueError):
            Routine(commands=[("first-command", 10), ("second-command", 0.3)], action=None, period=1)

    def test_routine_with_period(self):
        """Test that commands can be scheduled to repeat at the given period."""
        recorded_commands = []

        def record_commands(command):
            recorded_commands.append((command, time.perf_counter()))

        routine = Routine(
            commands=[("first-command", 0.1), ("second-command", 0.3)],
            action=record_commands,
            period=0.4,
            stop_after=1,
        )

        start_time = time.perf_counter()
        routine.run()

        self.assertEqual(recorded_commands[0][0], "first-command")
        self.assertAlmostEqual(recorded_commands[0][1], start_time + 0.1, delta=0.1)

        self.assertEqual(recorded_commands[1][0], "second-command")
        self.assertAlmostEqual(recorded_commands[1][1], start_time + 0.3, delta=0.1)

        self.assertEqual(recorded_commands[2][0], "first-command")
        self.assertAlmostEqual(recorded_commands[2][1], start_time + 0.1 + routine.period, delta=0.1)

        self.assertEqual(recorded_commands[3][0], "second-command")
        self.assertAlmostEqual(recorded_commands[3][1], start_time + 0.3 + routine.period, delta=0.1)