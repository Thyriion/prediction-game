from dataclasses import dataclass
from typing import Any

from django.utils import timezone

from matches.import_utils import parse_openligadb_datetime, compute_deadline_before_kickoff
from matches.openligadb_client import OpenLigaDbClient

@dataclass
class ImportSummary:
    league: str
    season: int

    groups_total: int = 0
    groups_with_matches: int = 0

    matches_total: int = 0

    teams_created: int = 0
    teams_updated: int = 0
    matches_created: int = 0
    matches_updated: int = 0
    results_created: int = 0
    results_updated: int = 0

    groups_imported: int = 0

def _group_order_id(match_json: dict[str, Any]) -> int:
    group = match_json.get("group")
    if isinstance(group, dict):
        order_id = group.get("groupOrderID")
        if isinstance(order_id, int):
            return order_id

    raise KeyError(f"Missing group/groupOrderID for matchID={match_json.get('matchID')}")


def _kickoff_at(match_json: dict[str, Any]) -> timezone.datetime:
    value = match_json.get("matchDateTime")
    if not isinstance(value, str) or not value:
        raise KeyError(f"Missing matchDateTime for matchID={match_json.get('matchID')}")
    return parse_openligadb_datetime(value)

def bootstrap_season(
    *,
    client: OpenLigaDbClient,
    league_shortcut: str,
    season_year: int,
    dry_run: bool = False,
) -> ImportSummary:
    """
    Bootstrap the season by fetching matches and computing tipping deadlines.
    1. Fetch all matches for the given league and season.
    2. Determine the earliest kickoff time for each matchday (group).
    3. Compute the tipping deadline as Friday 17:00 before the earliest kickoff.
    4. Return an ImportSummary with the results.
    """
    summary = ImportSummary(league=league_shortcut, season=season_year)

    matches = client.fetch_matches_season(league_shortcut, season_year)
    summary.matches_total = len(matches)

    earliest_by_matchday: dict[int, timezone.datetime] = {}

    for match in matches:
        try:
            group_id = _group_order_id(match)
            kickoff = _kickoff_at(match)
        except KeyError as e:
            print(f"[bootstrap] skip match due to missing data: {e}")
            continue

        current = earliest_by_matchday.get(group_id)
        if current is None or kickoff < current:
            earliest_by_matchday[group_id] = kickoff

    groups = client.fetch_available_groups(league_shortcut, season_year)
    summary.groups_total = len(groups)

    deadlines: dict[int, timezone.datetime] = {}
    for group in groups:
        group_id = group.get("groupOrderID") or group.get("groupOrderId")
        if not isinstance(group_id, int):
            continue

        earliest = earliest_by_matchday.get(group_id)
        if earliest is None:
            continue

        deadlines[group_id] = compute_deadline_before_kickoff(earliest)

    summary.groups_with_matches = len(deadlines)

    if dry_run:

        sample = sorted(deadlines.items(), key=lambda item: item[0])[:5]
        print(f"[bootstrap] league={league_shortcut} season={season_year}")
        print(f"groups: {summary.groups_total}, with matches: {summary.groups_with_matches}")
        print(f"matches: {summary.matches_total}")
        if sample:
            print("sample deadlines (matchday -> deadline_at):")
            for group_id, deadline in sample:
                print(f"  {group_id:>2} -> {deadline.isoformat()}")

    return summary

def update_season_smart(
    *,
    client: OpenLigaDbClient,
    league_shortcut: str,
    season_year: int,
    dry_run: bool = False,
) -> ImportSummary:
    summary = ImportSummary(league=league_shortcut, season=season_year)

    groups = client.fetch_available_groups(league_shortcut, season_year)
    summary.groups_total = len(groups)

    changed_samples: list[tuple[int, str]] = []

    for group in groups:
        group_id = group.get("groupOrderID") or group.get("groupOrderId")
        if not isinstance(group_id, int):
            continue

        last_changed = client.fetch_last_changed(league_shortcut, season_year, group_id)
        changed_samples.append((group_id, last_changed))

    if dry_run:
        print(f"[update] league={league_shortcut} season={season_year}")
        print(f"groups checked: {len(changed_samples)}")

        for group_id, last_changed in sorted(changed_samples)[:5]:
            print(f"  group {group_id:>2} last changed at {last_changed}")

    return summary