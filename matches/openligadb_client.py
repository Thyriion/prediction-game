from dataclasses import dataclass
from typing import Any

import requests

@dataclass(frozen=True)
class OpenLigaDbClient:
    base_url: str = "https://api.openligadb.de"
    timeout_seconds: int = 10

    def _get_json(self, endpoint: str) -> Any:
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        try:
            response = requests.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
            return response.json()
        except requests.Timeout as e:
            raise RuntimeError(f"Request to {url} timed out") from e
        except requests.HTTPError as e:
            raise RuntimeError(f"HTTP error occurred while requesting {url}: {e}") from e
        except ValueError as e:
            raise RuntimeError(f"Invalid JSON response from {url}") from e
        
    def fetch_available_groups(self, league_shortcut: str, season_year: int) -> list[dict[str, Any]]:
        # GET /getavailablegroups/{leagueShortcut}/{leagueSeason}
        endpoint = f"getavailablegroups/{league_shortcut}/{season_year}"
        data = self._get_json(endpoint)
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected data format for available groups: {data}")
        return data
    
    def fetch_matches_season(self, league_shortcut: str, season_year: int) -> list[dict[str, Any]]:
        # GET /getmatchdata/{leagueShortcut}/{leagueSeason}
        endpoint = f"getmatchdata/{league_shortcut}/{season_year}"
        data = self._get_json(endpoint)
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected data format for matches: {data}")
        return data
    
    def fetch_matches_matchday(self, league_shortcut: str, season_year: int, group_order_id: int) -> list[dict[str, Any]]:
        # GET /getmatchdata/{leagueShortcut}/{leagueSeason}/{groupOrderID}
        endpoint = f"getmatchdata/{league_shortcut}/{season_year}/{group_order_id}"
        data = self._get_json(endpoint)
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected data format for matchday matches: {data}")
        return data
    
    def fetch_last_changed(self, league_shortcut: str, season_year: int, group_order_id: int) -> str:
        # GET /getlastchangedate/{leagueShortcut}/{leagueSeason}/{groupOrderID}
        endpoint = f"getlastchangedate/{league_shortcut}/{season_year}/{group_order_id}"
        data = self._get_json(endpoint)
        if not isinstance(data, str):
            raise RuntimeError(f"Unexpected data format for last changed date: {data}")
        return data