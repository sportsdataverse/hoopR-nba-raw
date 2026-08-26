"""One-off: re-scrape games whose committed final.json carries a stale boxscore.

Context (hoopR#163 / #164 follow-through): 112 games' final.json snapshots were
captured before ESPN finalized the box score -- 51 games of the 2021 season ship
an all-zero player boxscore (e.g. 401307882: every player minutes==0 while the
live summary endpoint has real minutes) plus a long tail of games flagged by the
player-sum-vs-team-box credibility gate in hoopR-nba-data. Re-scraping through
the SAME path as python/espn_nba_02_pbp_scrape.py (espn_nba_pbp raw ->
nba/json/raw -> nba_pbp_disk + helper_nba_pbp -> nba/json/final) refreshes both
trees byte-compatibly; games whose live payload is unchanged produce no git
diff.

Run from the repo root (sequential, ~1 req/sec -- ESPN 403s under load):

    PYTHONUNBUFFERED=1 uv run python ops/oneoff/2026-08-26_rescrape_stale_box_games.py \
        >> logs/rescrape_stale_box_2026-08-26.log 2>&1; echo EXIT=$?

Watch live:  tail -f logs/rescrape_stale_box_2026-08-26.log
Rate tuning: RESCRAPE_SLEEP (seconds between games, default 1.0).
Resumable: re-running skips nothing by design (idempotent overwrite); to resume
after an interrupt just re-run -- already-refreshed games rewrite identically.
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import sportsdataverse as sdv

PATH_TO_RAW = "nba/json/raw"
PATH_TO_FINAL = "nba/json/final"

# Detected 2026-08-26 from hoopR-nba-data committed parquets: union of
# (a) games where every player row has minutes 0/NA, (b) games with
# minutes==0-while-played rows, (c) games failing the player-sum vs team-box
# credibility gate (points/FGM).
GAME_IDS = {
    2021: [
        401266795,
        401267209,
        401267250,
        401267411,
        401307571,
        401307584,
        401307588,
        401307601,
        401307606,
        401307613,
        401307614,
        401307616,
        401307628,
        401307630,
        401307643,
        401307646,
        401307653,
        401307658,
        401307666,
        401307667,
        401307676,
        401307679,
        401307692,
        401307695,
        401307707,
        401307708,
        401307724,
        401307727,
        401307733,
        401307740,
        401307743,
        401307750,
        401307757,
        401307762,
        401307765,
        401307780,
        401307782,
        401307790,
        401307802,
        401307809,
        401307810,
        401307828,
        401307831,
        401307836,
        401307845,
        401307854,
        401307862,
        401307873,
        401307874,
        401307882,
        401307886,
        401313155,
        401326988,
        401326989,
        401326990,
        401326991,
        401326993,
        401326994,
        401327894,
        401333062,
        401337336,
    ],
    2022: [
        401358774,
        401359968,
        401360673,
        401360737,
        401360768,
        401360954,
        401360967,
        401430235,
        401438132,
    ],
    2023: [
        401468202,
        401468306,
        401468346,
        401468531,
        401468853,
        401469258,
        401469294,
        401469380,
        401524696,
        401544848,
    ],
    2024: [
        401585016,
        401585353,
        401585387,
        401585443,
        401585466,
        401585535,
        401616460,
        401658192,
    ],
    2025: [
        401704652,
        401704749,
        401704804,
        401704873,
        401704929,
        401705162,
        401705335,
        401705338,
        401705508,
        401705513,
        401705552,
        401705558,
        401705616,
        401705746,
    ],
    2026: [
        401810150,
        401810183,
        401810372,
        401810547,
        401810560,
        401810577,
        401810633,
        401810698,
        401810756,
        401810760,
    ],
}


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {msg}", flush=True)


def rescrape(game_id: int) -> str:
    """Same capture + process shape as espn_nba_02_pbp_scrape.download_game."""
    Path(PATH_TO_RAW).mkdir(parents=True, exist_ok=True)
    Path(PATH_TO_FINAL).mkdir(parents=True, exist_ok=True)
    g = sdv.nba.espn_nba_pbp(game_id=game_id, raw=True)
    with open(f"{PATH_TO_RAW}/{game_id}.json", "w") as f:
        json.dump(g, f, indent=0, sort_keys=False)
    processed = sdv.nba.nba_pbp_disk(game_id=game_id, path_to_json=PATH_TO_RAW)
    result = sdv.nba.helper_nba_pbp(game_id=game_id, pbp_txt=processed)
    with open(f"{PATH_TO_FINAL}/{game_id}.json", "w") as f:
        json.dump(result, f, indent=0, sort_keys=False)
    return "ok"


def main() -> int:
    sleep_s = float(os.environ.get("RESCRAPE_SLEEP", "1.0"))
    total = sum(len(v) for v in GAME_IDS.values())
    done = failed = 0
    log(f"re-scraping {total} games sequentially (sleep={sleep_s}s)")
    for season in sorted(GAME_IDS):
        for gid in GAME_IDS[season]:
            try:
                rescrape(gid)
                done += 1
                log(f"[{done + failed}/{total}] season {season} game {gid} ok")
            except Exception as e:  # keep going; report at the end
                failed += 1
                log(f"[{done + failed}/{total}] season {season} game {gid} FAILED: {e!r}")
            time.sleep(sleep_s)
    log(f"COMPLETE ok={done} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
