from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandParser, CommandError
from django.utils import timezone

from matches.import_utils import parse_openligadb_datetime
from matches.openligadb_client import OpenLigaDbClient
from matches.importer import bootstrap_season, update_season_smart
from matches.models import Season

from leaderboard.services import recompute_leaderboard_for_season

def _group_id(group_json: dict[str, Any]) -> int | None:
    """
    Extract groupOrderID from OpenLigaDB group JSON.
    """
    gid = group_json.get("groupOrderID") or group_json.get("groupOrderId")
    return gid if isinstance(gid, int) else None

def _iter_kickoffs_from_matchday_payload(matches: list[dict[str, Any]]):
    """
    Yield kickoff datetimes from OpenLigaDB matchday match payload.
    """
    for match in matches:
        if not isinstance(match, dict):
            continue

        raw = match.get("matchDateTime")
        if not isinstance(raw, str) or not raw:
            continue

        try:
            yield parse_openligadb_datetime(raw)
        except Exception:
            continue

def determine_active_season(
    *,
    client: OpenLigaDbClient,
    league_shortcut: str,
    candidate_years: list[int],
    now: timezone.datetime | None = None,
) -> int:
    """
    Determine the active season year for the given league by inspecting match kickoff times.
    """
    now = now or timezone.now()

    best_upcoming: tuple[timezone.datetime, int] | None = None
    best_recent: tuple[timezone.datetime, int] | None = None

    for year in sorted(candidate_years, reverse=True):
        try:
            groups = client.fetch_available_groups(league_shortcut, year)
        except Exception:
            continue

        for group in groups:
            if not isinstance(group, dict):
                continue

            gid = _group_id(group)
            if gid is None:
                continue

            try:
                matchday_matches = client.fetch_matches_matchday(league_shortcut, year, gid)
            except Exception:
                continue

            for kickoff in _iter_kickoffs_from_matchday_payload(matchday_matches):
                if kickoff >= now:
                    if best_upcoming is None or kickoff < best_upcoming[0]:
                        best_upcoming = (kickoff, year)
                else:
                    if best_recent is None or kickoff > best_recent[0]:
                        best_recent = (kickoff, year)

    if best_upcoming is not None:
        return best_upcoming[1]
    if best_recent is not None:
        return best_recent[1]

    raise RuntimeError(
        f"Could not determine active season year for league '{league_shortcut}'. "
        f"Tried candidate years: {sorted(candidate_years, reverse=True)}"
    )

def _resolve_season_year(
    *,
    league_shortcut: str,
    requested: int | None,
    client: OpenLigaDbClient,
) -> tuple[int, str]:
    """
    Resolve the season year to import for the given league.
    """
    if requested is not None:
        return int(requested), "cli"

    db_year = (
        Season.objects
        .filter(league__shortcut=league_shortcut)
        .order_by("-year")
        .values_list("year", flat=True)
        .first()
    )
    if db_year is not None:
        return int(db_year), "db"

    now = timezone.now()
    candidate_years = [now.year + 1, now.year, now.year - 1]
    api_year = determine_active_season(
        client=client,
        league_shortcut=league_shortcut,
        candidate_years=candidate_years,
        now=now,
    )
    return int(api_year), "api"

class Command(BaseCommand):
    help = "Imports match data from OpenLigaDB"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--league",
            required=True,
            choices=["bl1", "bl2"],
            help="League shortcut to import (e.g. bl1, bl2).",
        )

        parser.add_argument(
            "--season",
            type=int,
            default=None,
            help=(
                "Season year to import (e.g. 2025). Optional. "
                "If omitted, we use DB latest season (fast) and fall back to OpenLigaDB detection (slow)."
            ),
        )

        parser.add_argument(
            "--mode",
            required=True,
            choices=["bootstrap", "smart"],
            help="Import mode: 'bootstrap' for full import, 'smart' for incremental updates.",
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="If set, do not write to DB. Only print what would happen (where supported).",
        )

        parser.add_argument(
            "--timeout",
            type=int,
            default=10,
            help="Timeout in seconds for API requests (default: 10).",
        )

    def handle(self, *args, **options):
        league: str = str(options["league"]).strip().lower()
        mode: str = str(options["mode"]).strip().lower()
        dry_run: bool = bool(options["dry_run"])
        timeout: int = int(options["timeout"])

        self.stdout.write(
            self.style.NOTICE(
                f"[import_openligadb] starting league={league} mode={mode} dry_run={dry_run} timeout={timeout}s"
            )
        )
        self.stdout.flush()

        client = OpenLigaDbClient(timeout_seconds=timeout)

        try:
            season_year, season_source = _resolve_season_year(
                league_shortcut=league,
                requested=options.get("season"),
                client=client,
            )
        except Exception as e:
            raise CommandError(f"Failed to resolve season year: {e}") from e

        if season_source == "api":
            self.stdout.write(self.style.NOTICE("[import_openligadb] season auto-detected via API (slow path)"))
            self.stdout.flush()

        self.stdout.write(
            self.style.NOTICE(
                f"[import_openligadb] league={league} season={season_year} season_source={season_source} mode={mode} dry_run={dry_run}"
            )
        )
        self.stdout.flush()

        try:
            if mode == "bootstrap":
                summary = bootstrap_season(
                    league_shortcut=league,
                    season_year=season_year,
                    client=client,
                    dry_run=dry_run,
                )
            elif mode == "smart":
                summary = update_season_smart(
                    league_shortcut=league,
                    season_year=season_year,
                    client=client,
                    dry_run=dry_run,
                )
            else:
                raise CommandError(f"Unknown import mode: {mode}")
        except Exception as e:
            raise CommandError(str(e)) from e


        if not dry_run:
            should_recompute = (not dry_run) and (
                    mode == "bootstrap"
                    or summary.results_created > 0
                    or summary.results_updated > 0
                    or summary.groups_with_matches > 0
                )
            if should_recompute:
                try:
                    season = (Season.objects.select_related("league").get(league__shortcut=league, year=season_year))
                except Season.DoesNotExist:
                    self.stdout.write(
                        self.style.WARNING(
                            f"[import_openligadb] cannot recompute leaderboard: season not found in DB after import: league={league} year={season_year}"
                        )
                    )
                else:
                    updated = recompute_leaderboard_for_season(season=season)
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"[import_openligadb] recomputed leaderboard for season: league={league} year={season_year} updated_entries={updated}"
                        )
                    )
        self.stdout.write(self.style.SUCCESS("[import_openligadb] done"))
        self.stdout.write(str(summary))
