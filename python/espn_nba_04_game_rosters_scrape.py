
"""Scrape ESPN NBA per-game rosters.

Output: ``nba/game_rosters/json/{game_id}.json`` -- raw ESPN response.
The downstream R parser in ``hoopR-nba-data`` reads these JSONs and
navigates ``boxscore.players`` to build the per-game tidy roster frame
(one row per athlete-team-game).

Endpoint: ``site.api.espn.com/apis/site/v2/sports/basketball/nba/summary``
This is the same comprehensive game-summary endpoint espn_nba_02_pbp_scrape.py
hits for play-by-play; it includes ``boxscore.players`` with per-team
roster + per-athlete stats / starter / DNP / ejected flags.

We deliberately hit ESPN directly instead of routing through
``sportsdataverse.nba.espn_nba_game_rosters`` because that SDK helper
ignores its own ``raw=True`` flag and always returns a polars DataFrame;
combined with ``json.dump(..., default=str)`` it produced a stringified
DataFrame repr on disk that no downstream parser could read.

Game ids are sourced from the season's schedule parquet
(``nba/schedules/parquet/nba_schedule_{year}.parquet``). If the parquet
is missing, falls back to a fresh ``sdv.nba.espn_nba_schedule`` call.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import gc
import json
import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd
import sportsdataverse as sdv
from tqdm import tqdm

# Bypass the broken espn_nba_game_rosters helper (see module docstring);
# call ESPN's summary endpoint via the SDK's HTTP helper instead.
from sportsdataverse.dl_utils import download
from sportsdataverse.scrape.espn.persist import write_payload


logging.basicConfig(
    level=logging.INFO,
    filename="hoopR_nba_raw_game_rosters_logfile.txt",
)
logger = logging.getLogger(__name__)

PATH_TO_OUTPUT = "nba/game_rosters/json"
PATH_TO_SCHEDULES = "nba/schedules/parquet"
SUMMARY_URL = "http://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={game_id}"
MAX_THREADS = 8


def fetch_game_ids_for_season(season: int) -> list[int]:
    """Pull every completed game id ESPN exposes for a given NBA season.

    Reads from the season's schedule parquet first (the canonical source
    used by every other per-game scraper in this repo). Falls back to a
    fresh ``espn_nba_schedule`` call when the parquet is missing.
    """
    schedule_path = Path(f"{PATH_TO_SCHEDULES}/nba_schedule_{season}.parquet")
    if schedule_path.exists():
        df = pd.read_parquet(schedule_path)
        if "status_type_completed" in df.columns:
            df = df[df["status_type_completed"] == True]  # noqa: E712
        ids = pd.to_numeric(df["game_id"], errors="coerce").dropna().astype(int)
        return sorted(ids.unique().tolist())

    logger.warning(
        f"No schedule parquet at {schedule_path}; falling back to espn_nba_schedule()"
    )
    sched = sdv.nba.espn_nba_schedule(season=season)
    if hasattr(sched, "to_pandas"):
        sched = sched.to_pandas()
    if "status_type_completed" in sched.columns:
        sched = sched[sched["status_type_completed"] == True]  # noqa: E712
    ids = pd.to_numeric(sched["game_id"], errors="coerce").dropna().astype(int)
    return sorted(ids.unique().tolist())


def download_game_rosters_batch(
    season: int,
    game_ids: list[int],
    output_dir: Path,
    rerun_existing: bool,
    cores: int,
) -> None:
    threads = min(cores, max(1, len(game_ids)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futs = {
            executor.submit(
                download_game_rosters, gid, output_dir, rerun_existing
            ): gid
            for gid in game_ids
        }
        for fut in tqdm(
            concurrent.futures.as_completed(futs),
            total=len(futs),
            desc=f"NBA game rosters {season}",
        ):
            fut.result()


def download_game_rosters(
    game_id: int, output_dir: Path, rerun_existing: bool
) -> str:
    out_path = Path(output_dir) / f"{game_id}.json"
    if out_path.exists() and not rerun_existing:
        return f"skip {game_id}"
    try:
        url = SUMMARY_URL.format(game_id=int(game_id))
        raw: dict[str, Any] = download(url).json()
        if not write_payload(out_path, raw, indent=0):
            logger.warning(f"refused error/empty payload: {out_path}")
            return f"refused {out_path.stem}"
        return f"ok {game_id}"
    except Exception as e:
        # Per-game tolerance: 404s, schema drift, transient ESPN failures
        # must NOT abort the season's run.
        logger.warning(f"game_id={game_id} failed: {e!r}")
        return f"err {game_id}: {e}"


def scrape_season(
    season: int, cores: int, rerun_existing: bool, base_output_dir: str
) -> None:
    output_dir = Path(base_output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    game_ids = fetch_game_ids_for_season(season)
    logger.info(f"season={season} games={len(game_ids)}")
    if not game_ids:
        logger.info(f"No game ids for {season}; skipping")
        return
    t0 = time.time()
    download_game_rosters_batch(
        season, game_ids, output_dir, rerun_existing, cores
    )
    t1 = time.time()
    logger.info(
        f"{(t1 - t0) / 60:.2f} minutes to download {len(game_ids)} game rosters for {season}."
    )


def main() -> None:
    if args.start_year < 2002:
        start_year = 2002
    else:
        start_year = args.start_year
    end_year = args.end_year if args.end_year is not None else start_year
    cores = args.cores if args.cores is not None else MAX_THREADS
    base_output_dir = args.output_dir or PATH_TO_OUTPUT
    rerun_existing = args.rerun_existing or args.force

    for season in range(start_year, end_year + 1):
        scrape_season(season, cores, rerun_existing, base_output_dir)

    gc.collect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_year",
        "--start-year",
        "-s",
        dest="start_year",
        type=int,
        required=True,
        help="Start year of NBA game-roster scrape (YYYY), e.g. 2025",
    )
    parser.add_argument(
        "--end_year",
        "--end-year",
        "-e",
        dest="end_year",
        type=int,
        help="End year of NBA game-roster scrape (YYYY)",
    )
    parser.add_argument(
        "--cores",
        "--workers",
        "-c",
        dest="cores",
        type=int,
        default=MAX_THREADS,
        help="Concurrent worker threads (default 8).",
    )
    parser.add_argument(
        "--output-dir",
        "--output_dir",
        dest="output_dir",
        type=str,
        default=None,
        help=f"Override base output directory (default {PATH_TO_OUTPUT}).",
    )
    parser.add_argument(
        "--rerun_existing",
        "-r",
        nargs="?",
        const=True,
        default=False,
        type=lambda v: str(v).lower() in ("true", "1", "yes", "y", "t"),
        help="Re-scrape rosters even when the output file already exists. Accepts a true/false value (e.g. `-r true`) for compat with the legacy umbrella workflow; bare `-r` defaults to True.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Alias for --rerun_existing.",
    )
    args = parser.parse_args()

    main()
