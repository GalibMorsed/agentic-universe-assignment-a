"""Pure value-normalisation helpers. No I/O, no database: easy to unit test.

Every function implements one rule of the spec (assignment-a-data/README.md).
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Optional

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

_HEADER_SEP = re.compile(r"[_\s]+")


def normalize_header(h: str) -> str:
    """lower-case, trim, collapse every run of '_' / whitespace to one space."""
    return _HEADER_SEP.sub(" ", h.strip().lower()).strip()


# --------------------------------------------------------------------- phone
def normalize_phone(raw: str) -> Optional[str]:
    """Indian mobile -> '+91XXXXXXXXXX', or None when invalid."""
    digits = re.sub(r"[^0-9]", "", raw)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) == 10 and digits[0] in "6789":
        return "+91" + digits
    return None


# ---------------------------------------------------------------------- date
_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
_ISO = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})$")
_DMY = re.compile(r"^(\d{1,2})([/-])(\d{1,2})\2(\d{4})$")
_DMON = re.compile(r"^(\d{1,2})-([A-Za-z]{3})-(\d{4})$")


def parse_date(raw: str) -> Optional[date]:
    """YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, DD-Mon-YYYY. Day-first. None if invalid."""
    s = raw.strip()
    try:
        if m := _ISO.match(s):
            return date(int(m[1]), int(m[2]), int(m[3]))
        if m := _DMY.match(s):
            return date(int(m[4]), int(m[3]), int(m[1]))
        if m := _DMON.match(s):
            mon = _MONTHS.get(m[2].lower())
            return date(int(m[3]), mon, int(m[1])) if mon else None
    except ValueError:  # impossible dates such as 31/02
        return None
    return None


# ------------------------------------------------------------------- decimal
_DECIMAL = re.compile(r"^(?:₹|rs\.?|inr)?\s*(\d[\d,]*(?:\.\d+)?|\.\d+)$", re.IGNORECASE)


def parse_decimal(raw: str) -> Optional[Decimal]:
    """Optional currency prefix, comma thousands separators, >= 0, 2 places."""
    m = _DECIMAL.match(raw.strip())
    if not m:
        return None
    return Decimal(m[1].replace(",", "")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------- field glue
def parse_value(field: Any, raw: str) -> tuple[bool, Any]:
    """Parse one trimmed, non-empty cell by field type -> (ok, JSON-safe value)."""
    t = field.type
    if t == "string":
        return True, raw
    if t == "phone":
        p = normalize_phone(raw)
        return (p is not None), p
    if t == "date":
        d = parse_date(raw)
        return (d is not None), (d.isoformat() if d else None)
    if t == "decimal":
        v = parse_decimal(raw)
        return (v is not None), (str(v) if v is not None else None)
    if t == "enum":
        low = raw.lower()
        return (low in field.values), (low if low in field.values else None)
    raise ValueError(f"unsupported field type {t!r}")


# ---------------------------------------------------------------- timestamps
def parse_timestamp(raw: Any) -> Optional[datetime]:
    """UTC ('...Z'), offset ('+05:30') or naive (= IST) -> aware datetime, else None."""
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=IST)


def to_epoch_us(dt: datetime) -> int:
    d = dt.astimezone(UTC) - EPOCH
    return (d.days * 86400 + d.seconds) * 1_000_000 + d.microseconds


def from_epoch_us(us: int) -> datetime:
    return EPOCH + timedelta(microseconds=us)


def ist_date(us: int) -> date:
    return from_epoch_us(us).astimezone(IST).date()


def ist_iso(us: int) -> str:
    return from_epoch_us(us).astimezone(IST).isoformat(timespec="seconds")
