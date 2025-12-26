from __future__ import annotations
from dataclasses import dataclass
from django.utils import timezone
from matches.models import Season, Matchday, Match

@dataclass(frozen=True)
class SeasonStatus:
    active_matchday: Matchday | None
    state: str  # "live" | "upcoming" | "idle"
    next_kickoff_at: timezone.datetime | None

def get_season_status(*, season: Season, now=None) -> SeasonStatus:
    now = now or timezone.now()

    live_matchday = (
        Matchday.objects
        .filter(season=season, matches__kickoff_at__lte=now, matches__is_finished=False)
        .distinct()
        .order_by("-order_id")
        .first()
    )
    if live_matchday:
        return SeasonStatus(active_matchday=live_matchday, state="live", next_kickoff_at=None)

    next_match = (
        Match.objects
        .filter(matchday__season=season, kickoff_at__gt=now)
        .select_related("matchday")
        .order_by("kickoff_at")
        .first()
    )
    if next_match:
        return SeasonStatus(active_matchday=next_match.matchday, state="upcoming", next_kickoff_at=next_match.kickoff_at)

    return SeasonStatus(active_matchday=None, state="idle", next_kickoff_at=None)
