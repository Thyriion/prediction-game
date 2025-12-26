from datetime import datetime
from typing import Any

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone


class Tip(models.Model):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="tips",
    )

    match = models.ForeignKey(
        "matches.Match",
        on_delete=models.CASCADE,
        related_name="tips",
    )

    home_goals_predicted = models.PositiveSmallIntegerField()
    away_goals_predicted = models.PositiveSmallIntegerField()

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["user", "match"], name="uniq_tip_user_match"),
            models.CheckConstraint(
                condition=models.Q(home_goals_predicted__gte=0) & models.Q(away_goals_predicted__gte=0),
                name="chk_tip_goals_non_negative",
            ),
        ]
        indexes = [
            models.Index(fields=["match", "user"]),
            models.Index(fields=["user", "created_at"]),
        ]
        ordering = ["-updated_at", "-id"]

    def __str__(self) -> str:
        return (
            f"Tip(user={getattr(self.user, 'pk', None)}, match={getattr(self.match, 'pk', None)}, "
            f"predicted={self.home_goals_predicted}-{self.away_goals_predicted})"
        )

    def is_editable(self, at: datetime | None = None) -> bool:
        now = at or timezone.now()
        return now <= self.match.matchday.deadline_at

    def clean(self) -> None:
        errors: dict[str, Any] = {}

        if self.home_goals_predicted is None:
            errors["home_goals_predicted"] = ["Required."]
        if self.away_goals_predicted is None:
            errors["away_goals_predicted"] = ["Required."]

        if self.home_goals_predicted is not None and self.home_goals_predicted < 0:
            errors.setdefault("home_goals_predicted", []).append("Must be a non-negative integer.")
        if self.away_goals_predicted is not None and self.away_goals_predicted < 0:
            errors.setdefault("away_goals_predicted", []).append("Must be a non-negative integer.")

        if self.match_id is not None:
            try:
                if not self.is_editable():
                    errors["__all__"] = ["Deadline passed: tip can no longer be created or changed."]
            except Exception:
                pass

        if errors:
            raise ValidationError(errors)

class MatchdayBonusTip(models.Model):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="matchday_bonus_tips",
    )

    matchday = models.ForeignKey(
        "matches.Matchday",
        on_delete=models.CASCADE,
        related_name="bonus_tips",
    )

    first_goal_minute_predicted = models.PositiveSmallIntegerField()

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["user", "matchday"],
                name="uniq_matchday_bonus_tip_user_matchday",
            ),
            models.CheckConstraint(
                condition=models.Q(first_goal_minute_predicted__gte=0)
                & models.Q(first_goal_minute_predicted__lte=130),
                name="chk_matchday_bonus_tip_minute_range",
            ),
        ]
        indexes = [
            models.Index(fields=["matchday", "user"]),
            models.Index(fields=["user", "created_at"]),
        ]
        ordering = ["-updated_at", "-id"]
    
    def __str__(self) -> str:
        return (
            f"MatchdayBonusTip(user={getattr(self.user, 'pk', None)}, "
            f"matchday={getattr(self.matchday, 'pk', None)}, "
            f"minute={self.first_goal_minute_predicted})"
        )
    
    def is_editable(self, at: datetime | None = None) -> bool:
        now = at or timezone.now()
        return now <= self.matchday.deadline_at