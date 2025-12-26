from django.urls import path

from leaderboard.views import season_leaderboard_bl1, season_leaderboard_bl1_partial

app_name = "leaderboard"

urlpatterns = [
    path("", season_leaderboard_bl1, name="season_bl1"),
    path("partial/", season_leaderboard_bl1_partial, name="season_bl1_partial"),
]
