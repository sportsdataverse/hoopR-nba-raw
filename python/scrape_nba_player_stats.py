"""Scrape ESPN NBA athlete season stats.

Output: ``nba/player_season_stats/json/{athlete_id}.json`` -- the raw ESPN
``/athletes/{id}/stats`` response. The downstream R parser in
``hoopR-nba-data`` (``espn_nba_05_player_season_stats_creation.R``) reads
these JSONs to build the per-season tidy player-stats frame.

Key difference from the WNBA sibling (``scrape_wnba_player_stats.py``):
this output is **athlete-keyed**, not ``{season}/{athlete_id}``. ESPN's
``/athletes/{id}/stats?season=YYYY`` endpoint ignores the ``season`` query
param and always returns the athlete's *entire* career -- the per-season
breakdown lives inside ``categories[].statistics[]`` (one entry per
season-team stint, plus a "YYYY-YY Totals" row for traded players). So one
fetch per athlete returns every season they played; storing per-season
would duplicate the same payload N times. The R parser slices the season
it wants out of ``statistics[]``.

Athlete ids are sourced from the ``espn_nba_player_boxscores`` release on
sportsdataverse-data -- the union of every athlete who appeared in a box
score across the requested season range. That release is the authoritative
"who played in season Y" list (the ESPN current-only team-roster endpoint
cannot supply historical rosters).

Requirements:
    Uses ``espn_nba_player_stats_v3`` from sportsdataverse-py, which is the
    site.web.api ``common/v3 .../athletes/{id}/stats`` career endpoint this
    output has always been built from.

    NOT ``espn_nba_player_stats`` -- despite the name, that is the core-v2
    ``/athletes/{id}/statistics`` endpoint: a different API returning a
    different, season-scoped payload (``$ref``/``season``/``athlete``/
    ``splits``) that the downstream parser cannot read.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import gc
import io
import json
import logging
import time
from pathlib import Path

from tqdm import tqdm

# _v3 == the site.web.api common/v3 .../athletes/{id}/stats career endpoint.
# The unsuffixed espn_nba_player_stats is core-v2 and returns a different
# payload -- see the module docstring. Do not "simplify" this import.
from sportsdataverse.nba import espn_nba_player_stats_v3
from sportsdataverse.dl_utils import download


logging.basicConfig(
    level=logging.INFO,
    filename="hoopR_nba_raw_player_stats_logfile.txt",
)
logger = logging.getLogger(__name__)

PATH_TO_OUTPUT = "nba/player_season_stats/json"
PLAYER_BOX_RELEASE = (
    "https://github.com/sportsdataverse/sportsdataverse-data/releases/"
    "download/espn_nba_player_boxscores/player_box_{season}.parquet"
)
# ESPN rate-limits the per-athlete stats endpoint more aggressively than the
# per-game endpoints, so keep concurrency modest.
DEFAULT_THREADS = 4


def _athlete_ids_for_season(season: int) -> list[int]:
    """Read the espn_nba_player_boxscores release parquet for ``season`` and
    return the unique integer athlete ids that appeared that year."""
    url = PLAYER_BOX_RELEASE.format(season=int(season))
    try:
        import pandas as pd

        resp = download(url)
        content = resp.content if hasattr(resp, "content") else resp
        df = pd.read_parquet(io.BytesIO(content), columns=["athlete_id"])
        ids = sorted(
            {
                int(x)
                for x in df["athlete_id"].dropna().unique()
                if str(x).strip().isdigit() or isinstance(x, (int, float))
            }
        )
        return ids
    except Exception as e:  # noqa: BLE001
        logger.warning(f"could not list player_box athletes for {season}: {e!r}")
        return []


def _athlete_ids_for_range(start_year: int, end_year: int) -> list[int]:
    """Union of athlete ids across the requested season range."""
    seen: set[int] = set()
    for season in range(start_year, end_year + 1):
        ids = _athlete_ids_for_season(season)
        logger.info(f"season={season} player_box athletes={len(ids)}")
        seen.update(ids)
    return sorted(seen)


def download_player_stats(athlete_id: int, season: int, rerun_existing: bool) -> str:
    out_path = Path(PATH_TO_OUTPUT) / f"{athlete_id}.json"
    if out_path.exists() and not rerun_existing:
        return f"skip {athlete_id}"
    try:
        # season is forwarded for API symmetry but ESPN ignores it -- the
        # payload always carries the full career in categories[].statistics[].
        raw = espn_nba_player_stats_v3(
            athlete_id=int(athlete_id), season=int(season), return_parsed=False
        )
        if isinstance(raw, (bytes, str)):
            raw = json.loads(raw)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(raw, f, indent=0, sort_keys=False)
        return f"ok {athlete_id}"
    except Exception as e:  # noqa: BLE001
        logger.warning(f"athlete_id={athlete_id} failed: {e!r}")
        return f"err {athlete_id}: {e}"


def download_player_stats_batch(
    athlete_ids: list[int], season: int, rerun_existing: bool, cores: int
) -> None:
    threads = min(cores, max(1, len(athlete_ids)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futs = {
            executor.submit(download_player_stats, aid, season, rerun_existing): aid
            for aid in athlete_ids
        }
        for fut in tqdm(
            concurrent.futures.as_completed(futs),
            total=len(futs),
            desc="NBA player stats",
        ):
            fut.result()


def main() -> None:
    # ESPN NBA athlete-stats coverage tracks the box-score release, which
    # starts in 2002.
    start_year = 2002 if args.start_year < 2002 else args.start_year
    end_year = args.end_year if args.end_year is not None else start_year
    cores = args.cores if args.cores is not None else DEFAULT_THREADS

    Path(PATH_TO_OUTPUT).mkdir(parents=True, exist_ok=True)

    athlete_ids = _athlete_ids_for_range(start_year, end_year)
    logger.info(f"range={start_year}-{end_year} unique athletes={len(athlete_ids)}")
    if not athlete_ids:
        logger.info("No athlete ids resolved; skipping")
        return

    t0 = time.time()
    # Pass end_year as the nominal season arg (ignored by ESPN; kept for the
    # signature). One fetch per athlete returns all of their seasons.
    download_player_stats_batch(athlete_ids, end_year, args.rerun_existing, cores)
    t1 = time.time()
    logger.info(
        f"{(t1 - t0) / 60:.2f} minutes to download {len(athlete_ids)} player-stat payloads."
    )

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
        help="Start year of NBA player-stats scrape (YYYY)",
    )
    parser.add_argument(
        "--end_year",
        "--end-year",
        "-e",
        dest="end_year",
        type=int,
        help="End year of NBA player-stats scrape (YYYY)",
    )
    parser.add_argument(
        "--cores",
        "--workers",
        "-c",
        dest="cores",
        type=int,
        default=DEFAULT_THREADS,
        help="Concurrent worker threads (default 4 -- ESPN rate-limits the per-athlete endpoint aggressively).",
    )
    parser.add_argument(
        "--rerun_existing",
        "-r",
        nargs="?",
        const=True,
        default=False,
        type=lambda v: str(v).lower() in ("true", "1", "yes", "y", "t"),
        help="Re-scrape stats even when the output file already exists. "
        "Accepts a true/false value (e.g. `-r true`) for compat with the "
        "legacy umbrella workflow; bare `-r` defaults to True.",
    )
    args = parser.parse_args()

    main()
