from django.db import models
from django.utils import timezone
from datetime import datetime

# Create your models here.

class League(models.Model):
    """
    Represents a sports league.
    """
    shortcut = models.CharField(max_length=10, unique=True)
    name = models.CharField(max_length=255, blank=True)

    def __str__(self):
        return self.name or self.shortcut
    
class Season(models.Model):
    """
    Represents a season within a league.
    """
    league = models.ForeignKey(League, on_delete=models.CASCADE, related_name="seasons")
    year = models.PositiveIntegerField()

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["league", "year"], name="uniq_league_year")
        ]
        ordering = ["league__shortcut", "year"]

    def __str__(self):
        return f"{self.league.shortcut} {self.year}"
    
class Matchday(models.Model):
    """
    Represents a matchday within a season.
    """
    season = models.ForeignKey(Season, on_delete=models.CASCADE, related_name="matchdays")
    order_id = models.PositiveSmallIntegerField()
    name = models.CharField(max_length=200, blank=True)
    deadline_at = models.DateTimeField(help_text="Deadline for placing/changing tips (Europe/Berlin)")
    openligadb_last_changed_at = models.DateTimeField(null=True, blank=True)
    first_goal_match = models.ForeignKey(
        "Match",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="+",
        help_text="The match in which the first goal of the matchday was scored.",
    )
    first_goal_minute = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        help_text="The minute in which the first goal of the matchday was scored.",
    )
    first_goal_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="The timestamp when the first goal of the matchday was scored.",
    )

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["season", "order_id"], name="uniq_season_matchday_order")
        ]
        ordering = ["season__year", "order_id"]

    def __str__(self):
        label = self.name or f"Matchday {self.order_id}"
        return f"{self.season} - {label}"
    
    def is_open_for_tipping(self, at: datetime | None = None):
        now = at or timezone.now()
        return now <= self.deadline_at
    
class Team(models.Model):
    """
    Represents a team in the league.
    """
    openligadb_team_id = models.PositiveIntegerField(unique=True)
    name = models.CharField(max_length=200)
    short_name = models.CharField(max_length=50, blank=True)
    icon_url = models.URLField(blank=True)

    def __str__(self):
        return self.name
    
class Match(models.Model):
    """
    Represents a match between two teams.
    """
    openligadb_match_id = models.PositiveIntegerField(unique=True)
    matchday = models.ForeignKey(Matchday, on_delete=models.CASCADE, related_name="matches")
    kickoff_at = models.DateTimeField()

    home_team = models.ForeignKey(Team, on_delete=models.PROTECT, related_name="home_matches")
    away_team = models.ForeignKey(Team, on_delete=models.PROTECT, related_name="away_matches")

    is_finished = models.BooleanField(default=False)

    class Meta:
        ordering = ["kickoff_at", "id"]
        indexes = [
            models.Index(fields=["matchday", "kickoff_at"]),
            models.Index(fields=["kickoff_at"])
        ]

    def __str__(self):
        return f"{self.home_team} vs {self.away_team} ({self.kickoff_at:%Y-%m-%d %H:%M})"
    
    @property
    def has_full_time_result(self):
        result = getattr(self, "result", None)
        return (
            result is not None and
            result.home_goals is not None and
            result.away_goals is not None
        )
    
class MatchResult(models.Model):
    """
    Represents the CURRENT live score of a match.
    When match.is_finished == True, this is the final result.
    """
    match = models.OneToOneField(Match, on_delete=models.CASCADE, related_name="result")
    home_goals = models.SmallIntegerField(null=True, blank=True)
    away_goals = models.SmallIntegerField(null=True, blank=True)

    class Meta:
        ordering = ["match__kickoff_at", "match__id"]

    @property
    def is_final(self) -> bool:
        return self.match.is_finished


    def __str__(self):
        return f"Result for {self.match}: {self.home_goals}:{self.away_goals}"