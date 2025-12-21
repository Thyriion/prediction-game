from dataclasses import dataclass
from typing import Any

from django.utils import timezone

from matches.import_utils import parse_openligadb_datetime, compute_deadline_before_kickoff
from matches.openligadb_client import OpenLigaDbClient
from matches.models import League, Season, Matchday, Team, Match, MatchResult

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

def _extract_team(team_json: dict[str, Any]) -> tuple[int, str, str, str]:
    team_id = team_json.get("teamId")
    if not isinstance(team_id, int):
        raise KeyError(f"Missing teamId in team JSON: keys={list(team_json.keys())}")
    
    name = (team_json.get("teamName") or "").strip()
    short_name = (team_json.get("shortName") or "").strip()
    icon_url = (team_json.get("teamIconUrl") or "").strip()

    return team_id, name, short_name, icon_url

def _upsert_team(
    *,
    team_id: int,
    name: str,
    short_name: str,
    icon_url: str,
) -> tuple[Team, bool, bool]:
    """
    Returns (team, created, updated)
    """
    team, created = Team.objects.get_or_create(
        openligadb_team_id=team_id,
        defaults={
            "name": name,
            "short_name": short_name,
            "icon_url": icon_url,
        }
    )

    updated = False

    if not created:
        if name and team.name != name:
            team.name = name
            updated = True
        if team.short_name != short_name:
            team.short_name = short_name
            updated = True
        if team.icon_url != icon_url:
            team.icon_url = icon_url
            updated = True

        if updated:
            team.save(update_fields=["name", "short_name", "icon_url"])

    return team, created, updated

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
    3. Compute tipping deadlines as 3.5 hours before the earliest kickoff.
    4. Return an ImportSummary with the results.
    """
    summary = ImportSummary(league=league_shortcut, season=season_year)

    matches = client.fetch_matches_season(league_shortcut, season_year)
    summary.matches_total = len(matches)

    league_name = ""
    if matches:
        league_name = matches[0].get("leagueName") or ""
    
    league_obj = season_obj = None
    if not dry_run:
        league_obj, created = League.objects.get_or_create(shortcut=league_shortcut, defaults={"name": league_name})

        if not created and league_name and league_obj.name != league_name:
            league_obj.name = league_name
            league_obj.save(update_fields=["name"])

        season_obj, _ = Season.objects.get_or_create(league=league_obj, year=season_year)

        seen_team_ids: set[int] = set()

        for match in matches:
            for key in ("team1", "team2"):
                team_json = match.get(key)
                if not isinstance(team_json, dict):
                    continue
                
                team_id, name, short_name, icon_url = _extract_team(team_json)

                if team_id in seen_team_ids:
                    continue
                seen_team_ids.add(team_id)

                _, created, updated = _upsert_team(
                    team_id=team_id,
                    name=name,
                    short_name=short_name,
                    icon_url=icon_url,
                )
                if created:
                    summary.teams_created += 1
                elif updated:
                    summary.teams_updated += 1

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

        deadline = compute_deadline_before_kickoff(earliest)
        deadlines[group_id] = deadline
        if not dry_run:
            matchday_obj, created = Matchday.objects.get_or_create(
                season=season_obj,
                order_id=group_id,
                defaults={
                    "name": group.get("groupName") or "",
                    "deadline_at": deadline,
                }
            )
            if not created:
                new_name = group.get("groupName") or ""
                if matchday_obj.name != new_name:
                    matchday_obj.name = new_name
                    matchday_obj.save(update_fields=["name"])
            else:
                summary.groups_imported += 1

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