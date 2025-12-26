from __future__ import annotations

from dataclasses import dataclass

from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models import F, Value
from django.db.models.functions import Coalesce

from matches.models import Season
from leaderboard.models import SeasonLeaderboardEntry
from leaderboard.scoring import score_tip, score_matchday_bonus
from tips.models import Tip, MatchdayBonusTip


@dataclass(frozen=True)
class LeaderboardRow:
    user_id: int
    display_name: str
    points: int
    tips_points: int
    bonus_points: int


def _display_name(user) -> str:
    for attr in ("username", "email"):
        val = getattr(user, attr, None)
        if isinstance(val, str) and val.strip():
            return val
    return str(user.pk)


def compute_leaderboard_for_season(*, season: Season, include_zero: bool = False) -> list[LeaderboardRow]:
    """
    Compute the leaderboard for the given season.
    """
    qs = (
        SeasonLeaderboardEntry.objects
        .select_related("user")
        .filter(season=season)
        .annotate(
            total_points_db=(
                Coalesce(F("tips_points"), Value(0)) + Coalesce(F("bonus_points"), Value(0))
            )
        )
        .order_by("-total_points_db", "-tips_points", "user_id")
    )

    rows: list[LeaderboardRow] = []
    for entry in qs:
        user = entry.user
        rows.append(
            LeaderboardRow(
                user_id=user.pk,
                display_name=_display_name(user),
                points=int(entry.total_points_db),
                tips_points=int(entry.tips_points),
                bonus_points=int(entry.bonus_points),
            )
        )

    return rows


@transaction.atomic
def recompute_leaderboard_for_season(*, season: Season) -> int:
    """
    Recompute the leaderboard entries for the given season.
    """
    User = get_user_model()

    tips = (
        Tip.objects
        .select_related(
            "user",
            "match",
            "match__result",
            "match__matchday",
            "match__matchday__season",
        )
        .filter(match__matchday__season=season)
        .only(
            "id",
            "user_id",
            "home_goals_predicted",
            "away_goals_predicted",
            "match_id",
        )
    )

    bonus_tips = (
        MatchdayBonusTip.objects
        .select_related("user", "matchday", "matchday__season")
        .filter(matchday__season=season)
        .only("id", "user_id", "matchday_id", "first_goal_minute_predicted")
    )

    tips_points_by_user: dict[int, int] = {}
    bonus_points_by_user: dict[int, int] = {}

    user_ids: set[int] = set()

    for tip in tips:
        user_ids.add(int(tip.user_id))
        tips_points_by_user[int(tip.user_id)] = tips_points_by_user.get(int(tip.user_id), 0) + score_tip(
            tip=tip,
            match=tip.match,
        )

    for bt in bonus_tips:
        user_ids.add(int(bt.user_id))
        bonus_points_by_user[int(bt.user_id)] = bonus_points_by_user.get(int(bt.user_id), 0) + score_matchday_bonus(
            bonus_tip=bt,
            matchday=bt.matchday,
        )

    if not user_ids:
        return 0

    existing_user_ids = set(
        User.objects.filter(id__in=user_ids).values_list("id", flat=True)
    )
    if not existing_user_ids:
        return 0

    existing_entries = {
        (e.user_id): e
        for e in SeasonLeaderboardEntry.objects
        .select_for_update()
        .filter(season=season, user_id__in=existing_user_ids)
    }

    to_create: list[SeasonLeaderboardEntry] = []
    to_update: list[SeasonLeaderboardEntry] = []

    for uid in existing_user_ids:
        t_pts = int(tips_points_by_user.get(uid, 0))
        b_pts = int(bonus_points_by_user.get(uid, 0))

        entry = existing_entries.get(uid)
        if entry is None:
            to_create.append(
                SeasonLeaderboardEntry(
                    season=season,
                    user_id=uid,
                    tips_points=t_pts,
                    bonus_points=b_pts,
                )
            )
        else:
            if int(entry.tips_points) != t_pts or int(entry.bonus_points) != b_pts:
                entry.tips_points = t_pts
                entry.bonus_points = b_pts
                to_update.append(entry)

    if to_create:
        SeasonLeaderboardEntry.objects.bulk_create(to_create, batch_size=500)

    if to_update:
        SeasonLeaderboardEntry.objects.bulk_update(to_update, ["tips_points", "bonus_points"], batch_size=500)

    return len(to_create) + len(to_update)
