"""Tests for date utility functions.

Covers parse_yyyymmdd, resolve_dates, and validate_date_range with
comprehensive test cases including edge cases and error conditions.
"""

import unittest
from datetime import date
from unittest.mock import MagicMock

from src.core.date_utils import parse_yyyymmdd, resolve_dates, validate_date_range


class ParseYyyymmddTests(unittest.TestCase):
    """Tests for parse_yyyymmdd function."""

    def test_parse_yyyymmdd_valid_date(self) -> None:
        """Valid yyyyMMdd string parses to correct date."""
        result = parse_yyyymmdd("20200115")
        self.assertEqual(result, date(2020, 1, 15))

    def test_parse_yyyymmdd_start_of_year(self) -> None:
        """January 1st parses correctly."""
        result = parse_yyyymmdd("20200101")
        self.assertEqual(result, date(2020, 1, 1))

    def test_parse_yyyymmdd_end_of_year(self) -> None:
        """December 31st parses correctly."""
        result = parse_yyyymmdd("20201231")
        self.assertEqual(result, date(2020, 12, 31))

    def test_parse_yyyymmdd_invalid_format_iso_raises(self) -> None:
        """ISO format (yyyy-mm-dd) raises ValueError."""
        with self.assertRaises(ValueError):
            parse_yyyymmdd("2020-01-15")

    def test_parse_yyyymmdd_invalid_format_slash_raises(self) -> None:
        """Slash format (mm/dd/yyyy) raises ValueError."""
        with self.assertRaises(ValueError):
            parse_yyyymmdd("01/15/2020")

    def test_parse_yyyymmdd_invalid_day_32_raises(self) -> None:
        """Day 32 raises ValueError."""
        with self.assertRaises(ValueError):
            parse_yyyymmdd("20200132")

    def test_parse_yyyymmdd_invalid_day_00_raises(self) -> None:
        """Day 00 raises ValueError."""
        with self.assertRaises(ValueError):
            parse_yyyymmdd("20200100")

    def test_parse_yyyymmdd_invalid_month_13_raises(self) -> None:
        """Month 13 raises ValueError."""
        with self.assertRaises(ValueError):
            parse_yyyymmdd("20201301")

    def test_parse_yyyymmdd_invalid_month_00_raises(self) -> None:
        """Month 00 raises ValueError."""
        with self.assertRaises(ValueError):
            parse_yyyymmdd("20200001")

    def test_parse_yyyymmdd_leap_year_feb_29_valid(self) -> None:
        """February 29 in leap year (2020) parses correctly."""
        result = parse_yyyymmdd("20200229")
        self.assertEqual(result, date(2020, 2, 29))

    def test_parse_yyyymmdd_non_leap_year_feb_29_raises(self) -> None:
        """February 29 in non-leap year (2021) raises ValueError."""
        with self.assertRaises(ValueError):
            parse_yyyymmdd("20210229")

    def test_parse_yyyymmdd_empty_string_raises(self) -> None:
        """Empty string raises ValueError."""
        with self.assertRaises(ValueError):
            parse_yyyymmdd("")

    def test_parse_yyyymmdd_incomplete_format_raises(self) -> None:
        """Incomplete date format raises ValueError."""
        with self.assertRaises(ValueError):
            parse_yyyymmdd("202001")  # Missing day entirely

    def test_parse_yyyymmdd_too_long_raises(self) -> None:
        """String longer than 8 characters raises ValueError."""
        with self.assertRaises(ValueError):
            parse_yyyymmdd("202001151")

    def test_parse_yyyymmdd_non_numeric_raises(self) -> None:
        """Non-numeric characters raise ValueError."""
        with self.assertRaises(ValueError):
            parse_yyyymmdd("2020abcd")


class ResolveDatesTests(unittest.TestCase):
    """Tests for resolve_dates function."""

    def test_resolve_dates_valid_config(self) -> None:
        """Valid config returns parsed dates tuple."""
        config = MagicMock()
        config.process_start_date = "20200101"
        config.process_end_date = "20201231"

        start, end = resolve_dates(config)

        self.assertEqual(start, date(2020, 1, 1))
        self.assertEqual(end, date(2020, 12, 31))

    def test_resolve_dates_same_date(self) -> None:
        """Same start and end date is valid."""
        config = MagicMock()
        config.process_start_date = "20200615"
        config.process_end_date = "20200615"

        start, end = resolve_dates(config)

        self.assertEqual(start, date(2020, 6, 15))
        self.assertEqual(end, date(2020, 6, 15))

    def test_resolve_dates_empty_start_raises(self) -> None:
        """Empty start date raises ValueError."""
        config = MagicMock()
        config.process_start_date = ""
        config.process_end_date = "20201231"

        with self.assertRaises(ValueError) as ctx:
            resolve_dates(config)
        self.assertIn("required", str(ctx.exception))

    def test_resolve_dates_empty_end_raises(self) -> None:
        """Empty end date raises ValueError."""
        config = MagicMock()
        config.process_start_date = "20200101"
        config.process_end_date = ""

        with self.assertRaises(ValueError) as ctx:
            resolve_dates(config)
        self.assertIn("required", str(ctx.exception))

    def test_resolve_dates_none_start_raises(self) -> None:
        """None start date raises ValueError."""
        config = MagicMock()
        config.process_start_date = None
        config.process_end_date = "20201231"

        with self.assertRaises(ValueError):
            resolve_dates(config)

    def test_resolve_dates_none_end_raises(self) -> None:
        """None end date raises ValueError."""
        config = MagicMock()
        config.process_start_date = "20200101"
        config.process_end_date = None

        with self.assertRaises(ValueError):
            resolve_dates(config)

    def test_resolve_dates_invalid_format_start_raises(self) -> None:
        """Invalid start date format propagates ValueError from parse."""
        config = MagicMock()
        config.process_start_date = "2020-01-01"  # ISO format
        config.process_end_date = "20201231"

        with self.assertRaises(ValueError):
            resolve_dates(config)

    def test_resolve_dates_invalid_format_end_raises(self) -> None:
        """Invalid end date format propagates ValueError from parse."""
        config = MagicMock()
        config.process_start_date = "20200101"
        config.process_end_date = "2020-12-31"  # ISO format

        with self.assertRaises(ValueError):
            resolve_dates(config)


class ValidateDateRangeTests(unittest.TestCase):
    """Tests for validate_date_range function."""

    def test_validate_date_range_raises_on_reversed_dates(self) -> None:
        """End date before start date raises ValueError."""
        with self.assertRaises(ValueError):
            validate_date_range(date(2024, 2, 1), date(2024, 1, 1))

    def test_validate_date_range_equal_dates_valid(self) -> None:
        """Same start and end date is valid."""
        start = date(2020, 6, 15)
        end = date(2020, 6, 15)

        result_start, result_end = validate_date_range(start, end)

        self.assertEqual(result_start, start)
        self.assertEqual(result_end, end)

    def test_validate_date_range_forward_dates_valid(self) -> None:
        """End date after start date is valid."""
        start = date(2020, 1, 1)
        end = date(2020, 12, 31)

        result_start, result_end = validate_date_range(start, end)

        self.assertEqual(result_start, start)
        self.assertEqual(result_end, end)

    def test_validate_date_range_one_day_apart_valid(self) -> None:
        """Dates one day apart is valid."""
        start = date(2020, 6, 14)
        end = date(2020, 6, 15)

        result_start, result_end = validate_date_range(start, end)

        self.assertEqual(result_start, start)
        self.assertEqual(result_end, end)

    def test_validate_date_range_year_boundary_valid(self) -> None:
        """Dates spanning year boundary is valid."""
        start = date(2020, 12, 31)
        end = date(2021, 1, 1)

        result_start, result_end = validate_date_range(start, end)

        self.assertEqual(result_start, start)
        self.assertEqual(result_end, end)

    def test_validate_date_range_error_message_contains_dates(self) -> None:
        """Error message includes both dates for debugging."""
        with self.assertRaises(ValueError) as ctx:
            validate_date_range(date(2024, 2, 1), date(2024, 1, 1))

        error_msg = str(ctx.exception)
        self.assertIn("2024-02-01", error_msg)
        self.assertIn("2024-01-01", error_msg)


if __name__ == "__main__":
    unittest.main()
