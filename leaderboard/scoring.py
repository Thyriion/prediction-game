from matches.models import Match, MatchResult, Matchday
from tips.models import Tip, MatchdayBonusTip

from dataclasses import dataclass
from typing import Iterable

EXACT_SCORE_POINTS = 5
TENDENCY_POINTS = 2
FIRST_GOAL_MINUTE_POINTS = 3

def _tendency(home: int, away: int) -> int:
    diff = home - away
    if diff > 0:
        return 1
    elif diff < 0:
        return -1
    
    return 0

def score_tip(
    *,
    tip: Tip,
    match: Match,
) -> int:
    result: MatchResult | None = getattr(match, "result", None)
    if result is None:
        return 0
    if result.home_goals is None or result.away_goals is None:
        return 0
    
    actual_home = int(result.home_goals)
    actual_away = int(result.away_goals)
    predicted_home = int(tip.home_goals_predicted)
    predicted_away = int(tip.away_goals_predicted)

    if predicted_home == actual_home and predicted_away == actual_away:
        return EXACT_SCORE_POINTS
    
    if _tendency(predicted_home, predicted_away) == _tendency(actual_home, actual_away):
        return TENDENCY_POINTS
    
    return 0

def score_matchday_bonus(
    *,
    bonus_tip: MatchdayBonusTip,
    matchday: Matchday,
) -> int:
    if matchday.first_goal_minute is None:
        return 0
    
    if int(bonus_tip.first_goal_minute_predicted) == int(matchday.first_goal_minute):
        return FIRST_GOAL_MINUTE_POINTS
    
    return 0

@dataclass(frozen=True)
class UserScoreBreakdown:
    tips_points: int
    bonus_points: int

    @property
    def total_points(self) -> int:
        return self.tips_points + self.bonus_points
    
def compute_user_score_for_season(
    *,
    user,
    season,
    tips: Iterable[Tip] | None = None,
    bonus_tips: Iterable[MatchdayBonusTip] | None = None,
) -> UserScoreBreakdown:
    if tips is None:
        tips = (
            Tip.objects
            .select_related("match", "match__result", "match__matchday", "match__matchday__season")
            .filter(user=user, match__matchday__season=season)
        )
    if bonus_tips is None:
        bonus_tips = (
            MatchdayBonusTip.objects
            .select_related("matchday", "matchday__season")
            .filter(user=user, matchday__season=season)
        )
    
    tips_points = 0
    for tip in tips:
        tips_points += score_tip(tip=tip, match=tip.match)

    bonus_points = 0
    for bonus_tip in bonus_tips:
        bonus_points += score_matchday_bonus(bonus_tip=bonus_tip, matchday=bonus_tip.matchday)

    return UserScoreBreakdown(
        tips_points=tips_points,
        bonus_points=bonus_points,
    )