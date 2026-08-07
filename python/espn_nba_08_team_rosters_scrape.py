"""Scrape ESPN NBA team rosters per (season, team_id).

Output: ``nba/team_rosters/json/{season}/{team_id}.json`` -- raw ESPN
``/teams/{team_id}/roster`` response. The downstream R parser in
``hoopR-nba-data`` (``espn_nba_04_rosters_creation.R``) reads these JSONs to
build the per-season tidy roster frame.

NOTE: ESPN's ``/teams/{id}/roster`` endpoint **ignores** the ``season``
query param and returns the team's CURRENT roster. So a season-keyed scrape
captures a snapshot-as-of-scrape-time; only the in-progress season is truly
"this season's roster". The daily flow keeps the current season fresh; older
season files reflect whatever the roster was when scraped.

Team ids are sourced from this season's NBA schedule parquet
(``home_id`` / ``away_id``) committed by ``espn_nba_01_schedules_scrape.py``.

Requirements:
    Uses the generic cross-league ``_espn_basketball_team_roster`` helper in
    sportsdataverse-py (``sportsdataverse/wbb/wbb_team_roster.py``), invoked
    with ``league="nba"``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import gc
import json
import logging
import time
from pathlib import Path

from sportsdataverse.scrape.espn.persist import write_payload
from sportsdataverse.wbb.wbb_team_roster import _espn_basketball_team_roster
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    filename="hoopR_nba_raw_team_rosters_logfile.txt",
)
logger = logging.getLogger(__name__)

PATH_TO_OUTPUT = "nba/team_rosters/json"
PATH_TO_SCHEDULES = "nba/schedules/parquet"
DEFAULT_THREADS = 8


def fetch_team_ids_for_season(season: int) -> list[int]:
    schedule_path = Path(f"{PATH_TO_SCHEDULES}/nba_schedule_{season}.parquet")
    if not schedule_path.exists():
        logger.warning(
            f"No schedule parquet at {schedule_path}; cannot resolve team ids."
        )
        return []
    try:
        import pandas as pd

        df = pd.read_parquet(schedule_path, columns=["home_id", "away_id"])
        ids = pd.concat([df["home_id"], df["away_id"]], ignore_index=True)
        ids = (
            pd.to_numeric(ids, errors="coerce")
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )
        return sorted(set(ids))
    except Exception as e:  # noqa: BLE001
        logger.warning(f"could not read team ids for {season}: {e!r}")
        return []


def download_team_roster(
    season: int, team_id: int, output_dir: Path, rerun_existing: bool
) -> str:
    out_path = Path(output_dir) / f"{team_id}.json"
    if out_path.exists() and not rerun_existing:
        return f"skip {team_id}"
    try:
        raw = _espn_basketball_team_roster(
            league="nba", team_id=int(team_id), season=int(season), raw=True
        )
        if isinstance(raw, (bytes, str)):
            raw = json.loads(raw)
        if not write_payload(out_path, raw, indent=0):
            logger.warning(f"refused error/empty payload: {out_path}")
            return f"refused {out_path.stem}"
        return f"ok {team_id}"
    except Exception as e:  # noqa: BLE001
        logger.warning(f"season={season} team_id={team_id} failed: {e!r}")
        return f"err {team_id}: {e}"


def download_team_rosters_batch(
    season: int, team_ids: list[int], output_dir: Path, rerun_existing: bool, cores: int
) -> None:
    threads = min(cores, max(1, len(team_ids)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futs = {
            executor.submit(
                download_team_roster, season, tid, output_dir, rerun_existing
            ): tid
            for tid in team_ids
        }
        for fut in tqdm(
            concurrent.futures.as_completed(futs),
            total=len(futs),
            desc=f"NBA rosters {season}",
        ):
            fut.result()


def scrape_season(season: int, cores: int, rerun_existing: bool) -> None:
    output_dir = Path(f"{PATH_TO_OUTPUT}/{season}")
    output_dir.mkdir(parents=True, exist_ok=True)
    team_ids = fetch_team_ids_for_season(season)
    logger.info(f"season={season} teams={len(team_ids)}")
    if not team_ids:
        logger.info(f"No team ids for {season}; skipping")
        return
    t0 = time.time()
    download_team_rosters_batch(season, team_ids, output_dir, rerun_existing, cores)
    t1 = time.time()
    logger.info(
        f"{(t1 - t0) / 60:.2f} minutes to download {len(team_ids)} rosters for {season}."
    )


def main() -> None:
    start_year = 2002 if args.start_year < 2002 else args.start_year
    end_year = args.end_year if args.end_year is not None else start_year
    cores = args.cores if args.cores is not None else DEFAULT_THREADS

    for season in range(start_year, end_year + 1):
        scrape_season(season, cores, args.rerun_existing)

    gc.collect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_year", "--start-year", "-s", dest="start_year",
        type=int, required=True,
        help="Start year of NBA roster scrape (YYYY)",
    )
    parser.add_argument(
        "--end_year", "--end-year", "-e", dest="end_year", type=int,
        help="End year of NBA roster scrape (YYYY)",
    )
    parser.add_argument(
        "--cores", "--workers", "-c", dest="cores", type=int,
        default=DEFAULT_THREADS,
        help="Concurrent worker threads (default 8).",
    )
    parser.add_argument(
        "--rerun_existing", "-r", nargs="?", const=True, default=False,
        type=lambda v: str(v).lower() in ("true", "1", "yes", "y", "t"),
        help="Re-scrape rosters even when the output file already exists.",
    )
    args = parser.parse_args()

    main()
