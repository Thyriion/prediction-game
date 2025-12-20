from datetime import datetime, time, timedelta

from django.utils import timezone
from django.utils.dateparse import parse_datetime

def parse_openligadb_datetime(value: str) -> datetime:
    """
    Parse a datetime string from OpenLigaDB API into a timezone-aware datetime object.
    """
    dt = parse_datetime(value)
    if dt is None:
        raise ValueError(f"Could not parse datetime from OpenLigaDB value: {value!r}")
    
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone.get_current_timezone())
    return dt

def compute_deadline_before_kickoff(
    earliest_kickoff: datetime,
    hours_before: float = 3.5
) -> datetime:
    """
    Compute the tipping deadline as Friday 17:00 (5 PM) before the given earliest kickoff datetime.
    """
    if timezone.is_naive(earliest_kickoff):
        earliest_kickoff = timezone.make_aware(earliest_kickoff, timezone.get_current_timezone())

    delta = timedelta(hours=hours_before)
    deadline = earliest_kickoff - delta

    return deadline