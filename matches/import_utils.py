from __future__ import annotations

from datetime import datetime, timedelta

from django.utils import timezone
from django.utils.dateparse import parse_datetime

DEFAULT_DEADLINE_HOURS_BEFORE_KICKOFF = 3.5


def ensure_aware(dt: datetime) -> datetime:
    """
    Ensure dt is timezone-aware in the current Django timezone (Europe/Berlin).
    OpenLigaDB timestamps can be naive.
    """
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def parse_openligadb_datetime(value: str) -> datetime:
    """
    Parse OpenLigaDB datetime string into a timezone-aware datetime.
    """
    dt = parse_datetime(value)
    if dt is None:
        raise ValueError(f"Could not parse datetime from OpenLigaDB value: {value!r}")
    return ensure_aware(dt)


def compute_deadline_before_kickoff(
    earliest_kickoff: datetime,
    hours_before: float = DEFAULT_DEADLINE_HOURS_BEFORE_KICKOFF,
) -> datetime:
    """
    Deadline rule:
    Deadline = hours_before (default 3.5) hours before the first kickoff of the matchday.
    """
    earliest_kickoff = ensure_aware(earliest_kickoff)
    return earliest_kickoff - timedelta(hours=hours_before)
