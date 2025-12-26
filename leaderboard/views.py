from __future__ import annotations

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.utils import timezone

from matches.models import League, Season
from matches.services import get_season_status

from leaderboard.services import compute_leaderboard_for_season


def _get_latest_season_for_league(*, league_shortcut: str) -> Season | None:
    """
    Returns the latest season we have in DB for a league shortcut.
    This is fast and avoids any API calls.
    """
    return (
        Season.objects
        .filter(league__shortcut=league_shortcut)
        .select_related("league")
        .order_by("-year")
        .first()
    )

def season_leaderboard_bl1_partial(request: HttpRequest) -> HttpResponse:
    season = _get_latest_season_for_league(league_shortcut="bl1")
    if season is None:
        return render(request, "leaderboard/_table.html", {"rows": [], "season": None})

    rows = compute_leaderboard_for_season(season=season)
    status = get_season_status(season=season)

    return render(
        request,
        "leaderboard/_table.html",
        {
            "rows": rows,
            "season": season,
            "status": status,
        },
    )


def season_leaderboard_bl1(request: HttpRequest) -> HttpResponse:
    """
    Leaderboard page for Bundesliga 1 (bl1).
    Uses the persisted SeasonLeaderboardEntry rows.
    """
    season = _get_latest_season_for_league(league_shortcut="bl1")
    if season is None:
        return render(
            request,
            "leaderboard/season.html",
            {
                "league_shortcut": "bl1",
                "season": None,
                "rows": [],
                "generated_at": timezone.now(),
                "empty_reason": "No season found in DB. Run the OpenLigaDB import first.",
            },
            status=200,
        )

    rows = compute_leaderboard_for_season(season=season)
    status = get_season_status(season=season)

    return render(
        request,
        "leaderboard/season.html",
        {
            "league_shortcut": "bl1",
            "season": season,
            "rows": rows,
            "status": status,
            "generated_at": timezone.now(),
            "empty_reason": None,
        },
    )
