from django.db import models
from django.conf import settings

# Create your models here.

class SeasonLeaderboardEntry(models.Model):
    season = models.ForeignKey(
        "matches.Season",
        on_delete=models.CASCADE,
        related_name="leaderboard_entries",
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="season_leaderboard_entries",
    )

    tips_points = models.PositiveIntegerField(default=0)
    bonus_points = models.PositiveIntegerField(default=0)

    computed_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["season", "user"], name="uniq_season_user_leaderboard_entry"),
        ]
        indexes = [
            models.Index(fields=["season", "-tips_points", "-bonus_points", "-computed_at"]),
        ]

    @property
    def total_points(self) -> int:
        return self.tips_points + self.bonus_points
    
    def __str__(self) -> str:
        return f"SeasonLeaderboardEntry(season={self.season_id}, user={self.user_id}, total={self.total_points})"