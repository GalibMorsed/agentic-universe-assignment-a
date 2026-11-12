"""Derived facts: effective disposition and lead bucket. Pure functions (no DB)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Optional

YET_TO_REACH_OUT = "yet_to_reach_out"
COMPLETED = "completed"
IN_PROGRESS_TELEPHONY_FAILED = "in_progress_telephony_failed"
IN_PROGRESS_DIALED = "in_progress_dialed"
BUCKETS = (YET_TO_REACH_OUT, IN_PROGRESS_TELEPHONY_FAILED, IN_PROGRESS_DIALED, COMPLETED)

# (occurred_us, event_id, code); event_id is only a deterministic tie-break
DispEvent = tuple[int, str, str]


@dataclass
class Call:
    call_id: str
    initiated_us: int
    connected: bool = False
    telephony_failed: bool = False
    disposition: Optional[str] = None      # effective disposition code


def effective_disposition(sets: Iterable[DispEvent], overrides: Iterable[DispEvent]) -> Optional[str]:
    """Latest override (by occurred_at) wins; else latest disposition.set; else None.

    Callers pass only events with catalogue-valid codes. Nothing is ever edited or deleted:
    an override simply outranks the original event.
    """
    overrides = list(overrides)
    pool = overrides if overrides else list(sets)
    return max(pool)[2] if pool else None


def lead_bucket(calls: list[Call], is_terminal: Callable[[str], bool]) -> str:
    """First match wins: no calls -> yet_to_reach_out; any terminal effective disposition ->
    completed; every call telephony-failed -> in_progress_telephony_failed; else in_progress_dialed."""
    if not calls:
        return YET_TO_REACH_OUT
    if any(c.disposition is not None and is_terminal(c.disposition) for c in calls):
        return COMPLETED
    if all(c.telephony_failed for c in calls):
        return IN_PROGRESS_TELEPHONY_FAILED
    return IN_PROGRESS_DIALED
