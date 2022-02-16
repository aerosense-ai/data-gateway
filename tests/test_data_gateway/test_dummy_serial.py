import random
import unittest

from serial.serialutil import SerialException

from data_gateway.dummy_serial import DummySerial, constants, exceptions, random_bytes, random_string
from tests.base import BaseTestCase


class DummySerialTest(BaseTestCase):
    def setUp(self):
        """Set up  the test environment:
        1. Create a random serial port name.
        2. Create a random baud rate.
        """
        self.random_serial_port = random_string()
        self.random_baudrate = random_string(5, constants.NUMBERS)

    def test_write_closed_port(self):
        """Tests writing-to a closed DummySerial port."""
        rand_write_len1 = random.randint(0, 1024)
        rand_write_len2 = random.randint(0, 1024)
        rand_write_str1 = random_string(rand_write_len1).encode()
        rand_write_str2 = random_string(rand_write_len2).encode()

        ds_instance = DummySerial(
            port=self.random_serial_port, baudrate=self.random_baudrate, responses={rand_write_str1: rand_write_str2}
        )

        self.assertTrue(ds_instance._isOpen)  # pylint: disable=W0212
        ds_instance.close()
        self.assertFalse(ds_instance._isOpen)  # pylint: disable=W0212
        with self.assertRaises(SerialException):
            ds_instance.write(rand_write_str1)
        self.assertFalse(ds_instance._isOpen)  # pylint: disable=W0212

    def test_write_and_read_to_closed_port(self):
        """Tests writing-to and reading-from a closed DummySerial port."""
        rand_write_len1 = random.randint(0, 1024)
        rand_write_len2 = random.randint(0, 1024)
        rand_write_str1 = random_string(rand_write_len1).encode()
        rand_write_str2 = random_string(rand_write_len2).encode()

        ds_instance = DummySerial(
            port=self.random_serial_port, baudrate=self.random_baudrate, responses={rand_write_str1: rand_write_str2}
        )

        self.assertTrue(ds_instance._isOpen)  # pylint: disable=W0212
        ds_instance.write(rand_write_str1)
        ds_instance.close()
        self.assertFalse(ds_instance._isOpen)  # pylint: disable=W0212
        with self.assertRaises(SerialException):
            ds_instance.read(rand_write_len2)
        self.assertFalse(ds_instance._isOpen)  # pylint: disable=W0212

    def test_repr_port(self):
        """Tests describing a DummySerial port."""
        rand_write_len1 = random.randint(0, 1024)
        rand_write_len2 = random.randint(0, 1024)
        rand_write_str1 = random_string(rand_write_len1).encode()
        rand_write_str2 = random_string(rand_write_len2).encode()

        ds_instance = DummySerial(
            port=self.random_serial_port, baudrate=self.random_baudrate, responses={rand_write_str1: rand_write_str2}
        )

        self.assertTrue(self.random_serial_port in str(ds_instance))

    def test_open_port(self):
        """Tests opening an already-open DummySerial port."""
        rand_write_len1 = random.randint(0, 1024)
        rand_write_len2 = random.randint(0, 1024)
        rand_write_str1 = random_string(rand_write_len1).encode()
        rand_write_str2 = random_string(rand_write_len2).encode()

        ds_instance = DummySerial(
            port=self.random_serial_port, baudrate=self.random_baudrate, responses={rand_write_str1: rand_write_str2}
        )

        self.assertTrue(ds_instance._isOpen)  # pylint: disable=W0212
        with self.assertRaises(SerialException):
            ds_instance.open()
        ds_instance.close()
        self.assertFalse(ds_instance._isOpen)  # pylint: disable=W0212
        ds_instance.open()
        self.assertTrue(ds_instance._isOpen)  # pylint: disable=W0212

    def test_close(self):
        """Tests closing a DummySerial port."""
        rand_write_len1 = random.randint(0, 1024)
        rand_write_len2 = random.randint(0, 1024)
        rand_write_str1 = random_string(rand_write_len1).encode()
        rand_write_str2 = random_string(rand_write_len2).encode()

        ds_instance = DummySerial(
            port=self.random_serial_port, baudrate=self.random_baudrate, responses={rand_write_str1: rand_write_str2}
        )

        self.assertTrue(ds_instance._isOpen)  # pylint: disable=W0212
        self.assertFalse(ds_instance.close())
        self.assertFalse(ds_instance._isOpen)  # pylint: disable=W0212

    def test_write_and_read_no_data_present(self):  # pylint: disable=C0103
        """Tests writing and reading with an unspecified response."""
        rand_write_len1 = random.randint(256, 1024)
        rand_read_len2 = random.randint(1, 16)  # give it some order of magnitudes less
        rand_write_bytes1 = random_bytes(rand_write_len1)

        ds_instance = DummySerial(port=self.random_serial_port, baudrate=self.random_baudrate)

        ds_instance.write(rand_write_bytes1)

        while 1:
            ds_instance.read(rand_read_len2)  # discard this data
            if not ds_instance.in_waiting:
                empty_data = ds_instance.read(rand_read_len2)
                break

        self.assertEqual(constants.NO_DATA_PRESENT, empty_data)

    def test_writing_non_bytes_data_raises_type_error(self):
        """Ensures that errors are raised if attempting to write non-bytes data"""
        rand_write_len = random.randint(256, 1024)
        rand_write_string = random_string(rand_write_len)

        ds = DummySerial(port=self.random_serial_port, baudrate=self.random_baudrate)
        with self.assertRaises(TypeError):
            ds.write(rand_write_string)

    def test_negative_read_size(self):
        """Ensures that errors are raised if attempting to access more or less data than in the buffer"""
        rand_write_len = random.randint(256, 1024)
        rand_write_bytes = random_bytes(rand_write_len)

        ds = DummySerial(port=self.random_serial_port, baudrate=self.random_baudrate)
        ds.write(rand_write_bytes)

        with self.assertRaises(exceptions.DSIOError):
            ds.read(-1)

    def test_timeout_with_large_read_size(self):
        """Ensures that errors are raised if attempting to access more or less data than in the buffer"""
        rand_write_len = random.randint(256, 1024)
        rand_write_bytes = random_bytes(rand_write_len)

        ds = DummySerial(
            port=self.random_serial_port, baudrate=self.random_baudrate, responses={rand_write_bytes: rand_write_bytes}
        )
        ds.write(rand_write_bytes)

        result = ds.read(rand_write_len + 2)
        self.assertEqual(len(result), rand_write_len)

    def test_partial_read(self):
        """Ensures that errors are raised if attempting to access more or less data than in the buffer"""
        rand_write_len = random.randint(256, 1024)
        rand_write_bytes = random_bytes(rand_write_len)

        ds = DummySerial(
            port=self.random_serial_port, baudrate=self.random_baudrate, responses={rand_write_bytes: rand_write_bytes}
        )
        ds.write(rand_write_bytes)

        result = ds.read(rand_write_len - 2)
        self.assertEqual(len(result), rand_write_len - 2)
        self.assertEqual(ds.in_waiting, 2)

    def test_write_bytearray(self):
        """Ensures that errors are raised if attempting to access more or less data than in the buffer"""
        rand_write_len = random.randint(256, 1024)
        rand_write_bytearray = bytearray(random_bytes(rand_write_len))

        ds = DummySerial(
            port=self.random_serial_port,
            baudrate=self.random_baudrate,
        )
        ds.write(rand_write_bytearray)


if __name__ == "__main__":
    unittest.main()
