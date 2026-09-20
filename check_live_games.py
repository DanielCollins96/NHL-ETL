#!/usr/bin/env python3
"""Exit 0 if NHL games are in progress, else 1.

Uses https://api-web.nhle.com/v1/schedule/now. LIVE and CRIT are in-progress
states; FINAL/OFF/FUT/PRE are not.
"""

import json
import sys
import urllib.error
import urllib.request

SCHEDULE_NOW_URL = "https://api-web.nhle.com/v1/schedule/now"
LIVE_STATES = {"LIVE", "CRIT"}


def fetch_schedule_now():
    request = urllib.request.Request(
        SCHEDULE_NOW_URL,
        headers={"User-Agent": "nhl-etl-live-check/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode())


def live_games(payload):
    games = []
    for day in payload.get("gameWeek") or []:
        for game in day.get("games") or []:
            if game.get("gameState") in LIVE_STATES:
                games.append(
                    {
                        "id": game.get("id"),
                        "date": day.get("date"),
                        "state": game.get("gameState"),
                        "start": game.get("startTimeUTC"),
                        "away": (game.get("awayTeam") or {}).get("abbrev"),
                        "home": (game.get("homeTeam") or {}).get("abbrev"),
                    }
                )
    return games


def main():
    try:
        payload = fetch_schedule_now()
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"Could not read /schedule/now: {exc}", file=sys.stderr)
        return 1

    current = live_games(payload)
    if not current:
        print("No in-progress NHL games")
        return 1

    print(f"{len(current)} in-progress NHL game(s):")
    for game in current:
        print(
            f"  {game['id']} {game['away']}@{game['home']} "
            f"state={game['state']} start={game['start']}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
