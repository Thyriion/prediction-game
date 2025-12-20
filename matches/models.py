from django.db import models
from django.utils import timezone

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

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["season", "order_id"], name="uniq_season_matchday_order")
        ]
        ordering = ["season__year", "order_id"]

    def __str__(self):
        label = self.name or f"Matchday {self.order_id}"
        return f"{self.season} - {label}"
    
    def is_open_for_tipping(self, at: timezone.datetime | None = None):
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
            result.home_goals_ft is not None and
            result.away_goals_ft is not None
        )
    
class MatchResult(models.Model):
    """
    Represents the result of a match.
    """
    match = models.OneToOneField(Match, on_delete=models.CASCADE, related_name="result")
    home_goals_ft = models.SmallIntegerField(null=True, blank=True)
    away_goals_ft = models.SmallIntegerField(null=True, blank=True)

    class Meta:
        ordering = ["match__kickoff_at", "match__id"]

    def __str__(self):
        return f"Result for {self.match}: {self.home_goals_ft}:{self.away_goals_ft}"