# matches/importer.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from django.db import transaction
from django.utils import timezone

from matches.import_utils import parse_openligadb_datetime, compute_deadline_before_kickoff
from matches.openligadb_client import OpenLigaDbClient
from matches.models import League, Season, Matchday, Team, Match, MatchResult
from concurrent.futures import ThreadPoolExecutor, as_completed


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

def _require_int(value: Any, *, err: str) -> int:
    if not isinstance(value, int):
        raise KeyError(err)
    return value

def _require_str(value: Any, *, err: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise KeyError(err)
    return value

def _match_id(match_json: dict[str, Any]) -> int:
    return _require_int(
        match_json.get("matchID"),
        err=f"Missing matchID in match JSON: keys={list(match_json.keys())}",
    )

def _group_order_id(match_json: dict[str, Any]) -> int:
    group = match_json.get("group")
    if not isinstance(group, dict):
        raise KeyError(f"Missing group for matchID={match_json.get('matchID')}")
    return _require_int(
        group.get("groupOrderID"),
        err=f"Missing group/groupOrderID for matchID={match_json.get('matchID')}",
    )

def _kickoff_at(match_json: dict[str, Any]) -> timezone.datetime:
    raw = _require_str(
        match_json.get("matchDateTime"),
        err=f"Missing matchDateTime for matchID={match_json.get('matchID')}",
    )
    return parse_openligadb_datetime(raw)

def _is_finished(match_json: dict[str, Any]) -> bool:
    return match_json.get("matchIsFinished") is True

def _extract_team_fields(team_json: dict[str, Any]) -> tuple[int, str, str, str]:
    team_id = _require_int(
        team_json.get("teamId"),
        err=f"Missing teamId in team JSON: keys={list(team_json.keys())}",
    )

    name = (team_json.get("teamName") or "").strip()
    short_name = (team_json.get("shortName") or "").strip()
    icon_url = (team_json.get("teamIconUrl") or "").strip()

    return team_id, name, short_name, icon_url

def _extract_current_score(match_json: dict[str, Any]) -> tuple[int, int] | None:
    """
    Returns (home_goals, away_goals) if any score is available, else None.

    Priority:
    1) goals (live progression) -> last goal has scoreTeam1/scoreTeam2
    2) matchResults -> prefer final-ish resultName (end/final), else highest resultOrderID
    """
    goals = match_json.get("goals") or []
    if isinstance(goals, list) and goals:
        last = goals[-1]
        if isinstance(last, dict):
            home = last.get("scoreTeam1")
            away = last.get("scoreTeam2")
            if isinstance(home, int) and isinstance(away, int):
                return home, away

    results = match_json.get("matchResults") or []
    if not isinstance(results, list) or not results:
        return None

    def _int(d: dict[str, Any], key: str) -> int | None:
        v = d.get(key)
        return v if isinstance(v, int) else None

    finals: list[tuple[int, int, int]] = []
    for r in results:
        if not isinstance(r, dict):
            continue
        name = (r.get("resultName") or "").strip().lower()
        if any(k in name for k in ("end", "ende", "final")):
            h = _int(r, "pointsTeam1")
            a = _int(r, "pointsTeam2")
            oid = r.get("resultOrderID")
            oid_int = oid if isinstance(oid, int) else -1
            if h is not None and a is not None:
                finals.append((h, a, oid_int))

    if finals:
        finals.sort(key=lambda t: t[2])
        h, a, _ = finals[-1]
        return h, a

    best: dict[str, Any] | None = None
    best_oid = -1
    for r in results:
        if not isinstance(r, dict):
            continue
        oid = r.get("resultOrderID")
        oid_int = oid if isinstance(oid, int) else -1
        if oid_int >= best_oid:
            best_oid = oid_int
            best = r

    if not best:
        return None

    h = _int(best, "pointsTeam1")
    a = _int(best, "pointsTeam2")
    if h is None or a is None:
        return None

    return h, a

def _extract_first_goal_minute_for_match(match_json: dict[str, Any]) -> int | None:
    """
    Returns the minute of the FIRST goal in THIS match, if available.
    We use this to compute the FIRST goal of the whole matchday (chronological).
    """
    goals = match_json.get("goals")
    if not isinstance(goals, list) or not goals:
        return None

    for g in goals:
        if not isinstance(g, dict):
            continue
        minute = g.get("matchMinute")
        if isinstance(minute, int) and 0 <= minute <= 130:
            return minute

    return None


def _compute_matchday_first_goal(
    *,
    matchday_obj: Matchday,
    match_rows: list[tuple[Match, dict[str, Any]]],
) -> None:
    """
    Computes and stores:
      - matchday.first_goal_at     (absolute timestamp)
      - matchday.first_goal_match  (FK to Match)
      - matchday.first_goal_minute (minute within that match)

    "First goal of matchday" means: chronologically first goal across all matches of that matchday.
    We approximate absolute time as kickoff_at + matchMinute.
    """
    best: tuple[timezone.datetime, Match, int] | None = None  

    for match_obj, match_json in match_rows:
        minute = _extract_first_goal_minute_for_match(match_json)
        if minute is None:
            continue

        goal_at = match_obj.kickoff_at + timedelta(minutes=minute)

        if best is None or goal_at < best[0]:
            best = (goal_at, match_obj, minute)

    if best is None:
        if (
            matchday_obj.first_goal_at is not None
            or matchday_obj.first_goal_match is not None
            or matchday_obj.first_goal_minute is not None
        ):
            matchday_obj.first_goal_at = None
            matchday_obj.first_goal_match = None
            matchday_obj.first_goal_minute = None
            matchday_obj.save(update_fields=["first_goal_at", "first_goal_match", "first_goal_minute"])
        return

    goal_at, match_obj, minute = best

    current_match_pk = matchday_obj.first_goal_match.pk if matchday_obj.first_goal_match else None

    if (
        matchday_obj.first_goal_at != goal_at
        or current_match_pk != match_obj.pk
        or matchday_obj.first_goal_minute != minute
    ):
        matchday_obj.first_goal_at = goal_at
        matchday_obj.first_goal_match = match_obj # type: ignore[type-arg]
        matchday_obj.first_goal_minute = minute
        matchday_obj.save(update_fields=["first_goal_at", "first_goal_match", "first_goal_minute"])


def _upsert_team(*, team_id: int, name: str, short_name: str, icon_url: str) -> tuple[Team, bool, bool]:
    """
    Returns (team, created, updated).
    """
    team, created = Team.objects.get_or_create(
        openligadb_team_id=team_id,
        defaults={"name": name, "short_name": short_name, "icon_url": icon_url},
    )

    if created:
        return team, True, False

    updated_fields: list[str] = []

    if name and team.name != name:
        team.name = name
        updated_fields.append("name")

    if team.short_name != short_name:
        team.short_name = short_name
        updated_fields.append("short_name")

    if team.icon_url != icon_url:
        team.icon_url = icon_url
        updated_fields.append("icon_url")

    if updated_fields:
        team.save(update_fields=updated_fields)
        return team, False, True

    return team, False, False


@transaction.atomic
def _upsert_match_result(*, match_obj: Match, match_json: dict[str, Any], summary: ImportSummary | None = None) -> None:
    """
    Invariant: MatchResult exists only if a score exists.
    Honest counting:
    - results_created increments only on first create
    - results_updated increments only when FT score actually changed
    """
    score = _extract_current_score(match_json)

    if score is None:
        MatchResult.objects.filter(match=match_obj).delete()
        return

    home_ft, away_ft = score

    existing = (
        MatchResult.objects
        .filter(match=match_obj)
        .only("home_goals_ft", "away_goals_ft")
        .first()
    )

    if existing is not None:
        if existing.home_goals_ft == home_ft and existing.away_goals_ft == away_ft:
            return

        existing.home_goals_ft = home_ft
        existing.away_goals_ft = away_ft
        existing.save(update_fields=["home_goals_ft", "away_goals_ft"])

        if summary:
            summary.results_updated += 1
        return

    MatchResult.objects.create(
        match=match_obj,
        home_goals_ft=home_ft,
        away_goals_ft=away_ft,
    )

    if summary:
        summary.results_created += 1

def _compute_earliest_kickoffs(matches: list[dict[str, Any]]) -> dict[int, timezone.datetime]:
    """
    Return earliest kickoff per matchday order_id.
    """
    earliest_by_matchday: dict[int, timezone.datetime] = {}

    for match in matches:
        if not isinstance(match, dict):
            continue
        try:
            md_order = _group_order_id(match)
            kickoff = _kickoff_at(match)
        except KeyError:
            continue

        current = earliest_by_matchday.get(md_order)
        if current is None or kickoff < current:
            earliest_by_matchday[md_order] = kickoff

    return earliest_by_matchday

def bootstrap_season(*, client: OpenLigaDbClient, league_shortcut: str, season_year: int, dry_run: bool = False) -> ImportSummary:
    summary = ImportSummary(league=league_shortcut, season=season_year)

    matches = client.fetch_matches_season(league_shortcut, season_year)
    summary.matches_total = len(matches)

    league_name = (matches[0].get("leagueName") or "") if matches else ""

    league_obj = season_obj = None
    seen_team_ids: set[int] = set()

    if not dry_run:
        league_obj, created = League.objects.get_or_create(
            shortcut=league_shortcut,
            defaults={"name": league_name},
        )
        if not created and league_name and league_obj.name != league_name:
            league_obj.name = league_name
            league_obj.save(update_fields=["name"])

        season_obj, _ = Season.objects.get_or_create(league=league_obj, year=season_year)

        for match in matches:
            for key in ("team1", "team2"):
                team_json = match.get(key)
                if not isinstance(team_json, dict):
                    continue

                team_id, name, short_name, icon_url = _extract_team_fields(team_json)
                if team_id in seen_team_ids:
                    continue
                seen_team_ids.add(team_id)

                _, t_created, t_updated = _upsert_team(
                    team_id=team_id,
                    name=name,
                    short_name=short_name,
                    icon_url=icon_url,
                )
                if t_created:
                    summary.teams_created += 1
                elif t_updated:
                    summary.teams_updated += 1

    earliest_by_matchday = _compute_earliest_kickoffs(matches)

    groups = client.fetch_available_groups(league_shortcut, season_year)
    summary.groups_total = len(groups)

    deadlines: dict[int, timezone.datetime] = {}

    for group in groups:
        md_order = group.get("groupOrderID") or group.get("groupOrderId")
        if not isinstance(md_order, int):
            continue

        earliest = earliest_by_matchday.get(md_order)
        if earliest is None:
            continue

        deadline = compute_deadline_before_kickoff(earliest)
        deadlines[md_order] = deadline

        if dry_run:
            continue

        assert season_obj is not None
        matchday_obj, created = Matchday.objects.get_or_create(
            season=season_obj,
            order_id=md_order,
            defaults={
                "name": group.get("groupName") or "",
                "deadline_at": deadline,
            },
        )

        new_name = group.get("groupName") or ""
        if not created and matchday_obj.name != new_name:
            matchday_obj.name = new_name
            matchday_obj.save(update_fields=["name"])

        if created:
            summary.groups_imported += 1

    summary.groups_with_matches = len(deadlines)

    if dry_run:
        sample = sorted(deadlines.items(), key=lambda item: item[0])[:5]
        print(f"[bootstrap] league={league_shortcut} season={season_year}")
        print(f"groups: {summary.groups_total}, with matches: {summary.groups_with_matches}")
        print(f"matches: {summary.matches_total}")
        if sample:
            print("sample deadlines (matchday -> deadline_at):")
            for md_order, deadline in sample:
                print(f"  {md_order:>2} -> {deadline.isoformat()}")
        return summary

    assert season_obj is not None

    matchday_by_order: dict[int, Matchday] = {md.order_id: md for md in Matchday.objects.filter(season=season_obj)}
    team_by_id: dict[int, Team] = {
        team.openligadb_team_id: team
        for team in Team.objects.filter(openligadb_team_id__in=seen_team_ids)
    }

    matchday_rows: dict[int, list[tuple[Match, dict[str, Any]]]] = {}

    for match in matches:
        if not isinstance(match, dict):
            continue

        try:
            openligadb_match_id = _match_id(match)
            md_order = _group_order_id(match)
            kickoff_at = _kickoff_at(match)

            team1 = match.get("team1")
            team2 = match.get("team2")
            if not isinstance(team1, dict) or not isinstance(team2, dict):
                raise KeyError("Missing team1/team2 data")

            home_id, *_ = _extract_team_fields(team1)
            away_id, *_ = _extract_team_fields(team2)

            matchday_obj = matchday_by_order[md_order]
            home_team = team_by_id[home_id]
            away_team = team_by_id[away_id]
        except Exception as e:
            print(f"[bootstrap] skip match due to missing data: {e}")
            continue

        match_obj, created = Match.objects.update_or_create(
            openligadb_match_id=openligadb_match_id,
            defaults={
                "matchday": matchday_obj,
                "kickoff_at": kickoff_at,
                "home_team": home_team,
                "away_team": away_team,
                "is_finished": _is_finished(match),
            },
        )

        if created:
            summary.matches_created += 1
        else:
            summary.matches_updated += 1

        _upsert_match_result(match_obj=match_obj, match_json=match, summary=summary)

        matchday_rows.setdefault(md_order, []).append((match_obj, match))

    for md_order, rows in matchday_rows.items():
        md_obj = matchday_by_order.get(md_order)
        if md_obj is None:
            continue
        _compute_matchday_first_goal(matchday_obj=md_obj, match_rows=rows)

    return summary


def _parse_last_changed(value: str) -> timezone.datetime:
    return parse_openligadb_datetime(value)


def _get_season_or_raise(*, league_shortcut: str, season_year: int) -> Season:
    try:
        league = League.objects.get(shortcut=league_shortcut)
    except League.DoesNotExist as e:
        raise RuntimeError(f"League '{league_shortcut}' not found. Run bootstrap_season first.") from e

    try:
        return Season.objects.get(league=league, year=season_year)
    except Season.DoesNotExist as e:
        raise RuntimeError(f"Season {league_shortcut} {season_year} not found. Run bootstrap_season first.") from e


def _compute_earliest_kickoff(matches: list[dict[str, Any]]) -> timezone.datetime | None:
    earliest: timezone.datetime | None = None
    for m in matches:
        if not isinstance(m, dict):
            continue
        try:
            kickoff = _kickoff_at(m)
        except Exception:
            continue
        if earliest is None or kickoff < earliest:
            earliest = kickoff
    return earliest


def _ensure_matchday(
    *,
    season: Season,
    group_json: dict[str, Any],
    matches_in_group: list[dict[str, Any]],
    summary: ImportSummary,
) -> Matchday | None:
    group_id = group_json.get("groupOrderID") or group_json.get("groupOrderId")
    if not isinstance(group_id, int):
        return None

    earliest = _compute_earliest_kickoff(matches_in_group)
    if earliest is None:
        return None

    deadline = compute_deadline_before_kickoff(earliest)
    name = (group_json.get("groupName") or "").strip()

    matchday, created = Matchday.objects.get_or_create(
        season=season,
        order_id=group_id,
        defaults={
            "name": name,
            "deadline_at": deadline,
        },
    )

    if created:
        summary.groups_imported += 1
    else:
        if name and matchday.name != name:
            matchday.name = name
            matchday.save(update_fields=["name"])

    return matchday


@transaction.atomic
def _import_one_matchday(
    *,
    season: Season,
    group_json: dict[str, Any],
    matches_in_group: list[dict[str, Any]],
    last_changed_at: timezone.datetime,
    summary: ImportSummary,
) -> None:
    matchday = _ensure_matchday(
        season=season,
        group_json=group_json,
        matches_in_group=matches_in_group,
        summary=summary,
    )
    if matchday is None:
        return

    matchday = Matchday.objects.select_for_update().get(pk=matchday.pk)

    seen_team_ids: set[int] = set()

    for m in matches_in_group:
        if not isinstance(m, dict):
            continue
        for key in ("team1", "team2"):
            team_json = m.get(key)
            if not isinstance(team_json, dict):
                continue

            team_id, name, short_name, icon_url = _extract_team_fields(team_json)
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

    team_by_id: dict[int, Team] = {
        t.openligadb_team_id: t
        for t in Team.objects.filter(openligadb_team_id__in=seen_team_ids)
    }

    match_rows: list[tuple[Match, dict[str, Any]]] = []

    for m in matches_in_group:
        if not isinstance(m, dict):
            continue

        try:
            openligadb_match_id = _match_id(m)
            kickoff_at = _kickoff_at(m)

            team1 = m.get("team1")
            team2 = m.get("team2")
            if not isinstance(team1, dict) or not isinstance(team2, dict):
                raise KeyError("Missing team1/team2")

            home_id, *_ = _extract_team_fields(team1)
            away_id, *_ = _extract_team_fields(team2)

            home_team = team_by_id[home_id]
            away_team = team_by_id[away_id]
        except Exception as e:
            print(f"[update] skip match due to missing data: {e}")
            continue

        match_obj, created = Match.objects.update_or_create(
            openligadb_match_id=openligadb_match_id,
            defaults={
                "matchday": matchday,
                "kickoff_at": kickoff_at,
                "home_team": home_team,
                "away_team": away_team,
                "is_finished": _is_finished(m),
            },
        )

        if created:
            summary.matches_created += 1
        else:
            summary.matches_updated += 1

        _upsert_match_result(match_obj=match_obj, match_json=m, summary=summary)

        match_rows.append((match_obj, m))

    _compute_matchday_first_goal(matchday_obj=matchday, match_rows=match_rows)

    matchday.openligadb_last_changed_at = last_changed_at
    matchday.save(update_fields=["openligadb_last_changed_at"])

def update_season_smart(
    *,
    client: OpenLigaDbClient,
    league_shortcut: str,
    season_year: int,
    dry_run: bool = False,
    last_changed_workers: int = 10,
) -> ImportSummary:
    """
    Smart update of season data by checking lastChanged timestamps of matchdays.
    Only matchdays with changed data are re-imported.
    """
    summary = ImportSummary(league=league_shortcut, season=season_year)
    season = _get_season_or_raise(league_shortcut=league_shortcut, season_year=season_year)

    groups = client.fetch_available_groups(league_shortcut, season_year)
    summary.groups_total = len(groups)

    def _log(msg: str) -> None:
        print(msg, flush=True)

    _log(
        f"[update] league={league_shortcut} season={season_year} "
        f"groups={summary.groups_total} dry_run={dry_run} workers={last_changed_workers}"
    )

    group_ids: list[int] = []
    group_by_id: dict[int, dict[str, Any]] = {}
    for idx, g in enumerate(groups, start=1):
        gid = g.get("groupOrderID") or g.get("groupOrderId")
        if not isinstance(gid, int):
            _log(f"[update] skip group #{idx}: missing groupOrderID/groupOrderId")
            continue
        group_ids.append(gid)
        group_by_id[gid] = g

    def _fetch_last_changed(gid: int) -> tuple[int, str, timezone.datetime]:
        raw = client.fetch_last_changed(league_shortcut, season_year, gid)
        dt = _parse_last_changed(raw)
        return gid, raw, dt

    last_changed_map: dict[int, tuple[str, timezone.datetime]] = {}
    errors: list[tuple[int, str]] = []

    with ThreadPoolExecutor(max_workers=last_changed_workers) as ex:
        futures = {ex.submit(_fetch_last_changed, gid): gid for gid in group_ids}
        for fut in as_completed(futures):
            gid = futures[fut]
            try:
                _gid, raw, dt = fut.result()
                last_changed_map[_gid] = (raw, dt)
            except Exception as e:
                errors.append((gid, str(e)))

    if errors:
        for gid, err in errors[:10]:
            _log(f"[update] lastChanged error group {gid}: {err}")
        if len(errors) > 10:
            _log(f"[update] ... {len(errors) - 10} more lastChanged errors")

    planned: list[tuple[int, str, timezone.datetime]] = []
    for gid, (raw, dt) in last_changed_map.items():
        md = (
            Matchday.objects
            .filter(season=season, order_id=gid)
            .only("openligadb_last_changed_at")
            .first()
        )
        db_last_changed = md.openligadb_last_changed_at if md else None
        has_changed = (db_last_changed is None) or (dt > db_last_changed)
        if has_changed:
            planned.append((gid, raw, dt))

    planned.sort(key=lambda t: t[0])

    summary.groups_with_matches = len(planned)

    if dry_run:
        _log(f"[update] planned changed groups: {len(planned)} (dry-run)")
        for gid, raw, _ in planned[:10]:
            _log(f"[update]   group {gid:>2} changed at {raw}")
        return summary

    matches_seen = 0

    for idx, (gid, raw, dt) in enumerate(planned, start=1):
        _log(f"[update] importing group {gid:>2} ({idx}/{len(planned)}) lastChanged={raw}")

        try:
            matches_in_group = client.fetch_matches_matchday(league_shortcut, season_year, gid)
        except Exception as e:
            _log(f"[update] group {gid}: fetch_matches_matchday failed: {e}")
            continue

        matches_seen += len(matches_in_group)

        _import_one_matchday(
            season=season,
            group_json=group_by_id[gid],
            matches_in_group=matches_in_group,
            last_changed_at=dt,
            summary=summary,
        )

    summary.matches_total = matches_seen

    _log(
        f"[update] done: changed_groups={summary.groups_with_matches}, matches_fetched={matches_seen}, "
        f"teams(created={summary.teams_created}, updated={summary.teams_updated}), "
        f"matches(created={summary.matches_created}, updated={summary.matches_updated}), "
        f"results(created={summary.results_created}, updated={summary.results_updated})"
    )

    return summary
