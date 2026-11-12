from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from ledger.normalize import (ist_date, ist_iso, normalize_header, normalize_phone, parse_date,
                              parse_decimal, parse_timestamp, to_epoch_us)

IST = timezone(timedelta(hours=5, minutes=30))


@pytest.mark.parametrize("raw", [
    "9876543210", "+91 98765 43210", "098765 43210", "91-98765-43210", "(+91) 9876543210",
    "+919876543210", "  98765-43210 ", "6000000000", "7000000000", "8000000000"])
def test_phone_valid_formats(raw):
    assert normalize_phone(raw) == "+91" + "".join(c for c in raw if c.isdigit())[-10:]


@pytest.mark.parametrize("raw", [
    "987654321",            # 9 digits
    "5876543210",           # bad prefix
    "022-2345-6789",        # landline (11 digits starting 0 -> 10 digits starting 2)
    "N/A", "", "-",
    "+1 415 555 0134",      # US number
    "919876543210 1",       # too long
    "0919876543210",        # 13 digits: neither rule applies
    "91 5876543210",        # 12 digits starting 91 but remainder has a bad prefix
])
def test_phone_invalid(raw):
    assert normalize_phone(raw) is None


def test_phone_rules_are_exact_not_greedy():
    # a 10-digit number that merely *starts* with 91 is left alone
    assert normalize_phone("9123456789") == "+919123456789"
    # 11 digits starting with 0 -> drop the 0 only
    assert normalize_phone("09123456789") == "+919123456789"


def test_header_normalisation():
    assert normalize_header("  Customer_Name ") == "customer name"
    assert normalize_header("MOBILE   NO") == "mobile no"
    assert normalize_header("policy__no") == "policy no"
    assert normalize_header("Policy_ No") == "policy no"


@pytest.mark.parametrize("raw,expected", [
    ("2026-10-05", date(2026, 10, 5)),
    ("05/10/2026", date(2026, 10, 5)),        # day-first
    ("05-10-2026", date(2026, 10, 5)),
    ("01-Dec-2026", date(2026, 12, 1)),
    ("01-dec-2026", date(2026, 12, 1)),
    ("01-DEC-2026", date(2026, 12, 1)),
    ("29/02/2028", date(2028, 2, 29)),        # leap year
])
def test_dates_valid(raw, expected):
    assert parse_date(raw) == expected


@pytest.mark.parametrize("raw", [
    "31/02/2026", "2026-13-01", "soon", "TBD", "", "13/13/2026", "29/02/2026",
    "2026/10/05", "01-Foo-2026", "05/10-2026", "5 Oct 2026"])
def test_dates_invalid(raw):
    assert parse_date(raw) is None


@pytest.mark.parametrize("raw,expected", [
    ("12000", "12000.00"), ("12000.5", "12000.50"), ("₹12,000", "12000.00"),
    ("Rs. 4500", "4500.00"), ("rs 4500", "4500.00"), ("RS.4500", "4500.00"),
    ("INR 1234.5", "1234.50"), ("inr1,23,456.789", "123456.79"), ("0", "0.00"),
    ("₹ 1,000.005", "1000.01"),
])
def test_decimal_valid(raw, expected):
    assert parse_decimal(raw) == Decimal(expected)


@pytest.mark.parametrize("raw", ["N/A", "-", "twelve thousand", "-5", "₹-5", "", "USD 5", "1.2.3"])
def test_decimal_invalid(raw):
    assert parse_decimal(raw) is None


def test_timestamp_three_forms_all_mean_the_same_instant():
    utc = parse_timestamp("2026-10-03T04:31:00Z")
    off = parse_timestamp("2026-10-03T10:01:00+05:30")
    naive = parse_timestamp("2026-10-03 10:01:00")          # naive means IST
    assert to_epoch_us(utc) == to_epoch_us(off) == to_epoch_us(naive)
    assert naive.tzinfo is not None and naive.utcoffset() == timedelta(hours=5, minutes=30)


@pytest.mark.parametrize("bad", [None, "", "yesterday", 12345, "2026-13-45T00:00:00Z"])
def test_timestamp_invalid(bad):
    assert parse_timestamp(bad) is None


def test_ist_date_crosses_midnight():
    us = to_epoch_us(parse_timestamp("2026-10-01T23:00:00Z"))      # 04:30 IST next day
    assert ist_date(us) == date(2026, 10, 2)
    assert ist_iso(us) == "2026-10-02T04:30:00+05:30"
