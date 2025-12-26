from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

from matches.models import Match, Matchday
from tips.models import MatchdayBonusTip, Tip


@dataclass(frozen=True)
class TipUpsertResult:
    tip: Tip
    created: bool
    updated: bool


def _ensure_aware(dt: datetime) -> datetime:
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _validate_tip_input(
    *,
    user,
    match: Match,
    home: int,
    away: int,
    at: datetime | None = None,
) -> None:
    if user is None:
        raise ValidationError("User is required to create a tip.")
    if match is None:
        raise ValidationError("Match is required to create a tip.")

    now = _ensure_aware(at) if at is not None else timezone.now()

    if home < 0 or away < 0:
        raise ValidationError("Predicted goals must be non-negative integers.")

    if now > match.matchday.deadline_at:
        raise ValidationError("Deadline passed: tip can no longer be created or changed.")


@transaction.atomic
def upsert_tip(
    *,
    user,
    match: Match,
    home_goals_predicted: int,
    away_goals_predicted: int,
    at: datetime | None = None,
) -> TipUpsertResult:
    _validate_tip_input(
        user=user,
        match=match,
        home=home_goals_predicted,
        away=away_goals_predicted,
        at=at,
    )

    existing = Tip.objects.filter(user=user, match=match).first()
    if existing is not None:
        if (
            existing.home_goals_predicted == home_goals_predicted
            and existing.away_goals_predicted == away_goals_predicted
        ):
            return TipUpsertResult(tip=existing, created=False, updated=False)

    tip, created = Tip.objects.update_or_create(
        user=user,
        match=match,
        defaults={
            "home_goals_predicted": home_goals_predicted,
            "away_goals_predicted": away_goals_predicted,
        },
    )

    updated = (existing is not None) and (not created)
    return TipUpsertResult(tip=tip, created=created, updated=updated)

def _validate_bonus_tip_input(
    *,
    user,
    matchday: Matchday,
    minute: int,
    at: datetime | None = None,
) -> None:
    if user is None:
        raise ValidationError("User is required to create a bonus tip.")
    if matchday is None:
        raise ValidationError("Matchday is required to create a bonus tip.")

    now = _ensure_aware(at) if at is not None else timezone.now()

    if minute < 0 or minute > 130:
        raise ValidationError("First goal minute must be between 0 and 130.")

    if now > matchday.deadline_at:
        raise ValidationError("Deadline passed: bonus tip can no longer be created or changed.")
    
@transaction.atomic
def upsert_matchday_bonus_tip(
    *,
    user,
    matchday: Matchday,
    first_goal_minute_predicted: int,
    at: datetime | None = None,
)-> tuple[MatchdayBonusTip, bool, bool]:
    _validate_bonus_tip_input(
        user=user,
        matchday=matchday,
        minute=first_goal_minute_predicted,
        at=at,
    )

    existing = MatchdayBonusTip.objects.filter(user=user, matchday=matchday).first()
    if existing is not None and existing.first_goal_minute_predicted == first_goal_minute_predicted:
        return existing, False, False
    
    bonus_tip, created = MatchdayBonusTip.objects.update_or_create(
        user=user,
        matchday=matchday,
        defaults={
            "first_goal_minute_predicted": first_goal_minute_predicted,
        },
    )
    updated = (existing is not None) and (not created)
    return bonus_tip, created, updated