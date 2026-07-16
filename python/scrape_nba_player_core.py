"""Scrape ESPN NBA athlete core records (identity + bio).

Output: ``nba/player_core/json/{athlete_id}.json`` -- raw ESPN core-v2
``/athletes/{id}`` response.

Why this dataset exists: the player_season_stats payload carries NO athlete
identity at all -- no name, no bio, not even the athlete id (the only carrier
of the id is the *filename*). Its ``"height"`` keys are team-logo pixel heights
and its ``"fullName"`` keys are arena names. So identity/bio has to come from
somewhere, and this is the cheapest complete source: one request per athlete,
resolving ~100% in every era sampled (2005/2015/2024), where the season-stats
endpoint 404s constantly.

Output is athlete-keyed and FLAT, not season-keyed: a core record is per-athlete
state, not per-season, so ids are deduped across the requested season range and
each athlete is fetched exactly once. The endpoint takes no season param.

Everything worth having is inline in one response -- fullName, height (numeric
inches) + displayHeight, weight, jersey, position, headshot{href}, birthPlace
{city,state,country}, dateOfBirth, experience{years}, active. ``team`` and
``college`` are ``{"$ref": url}`` only; DO NOT hydrate them (that triples the
request count for the whole athlete universe). Their ids are embedded in the
ref URL and are parsed out downstream for free.

CAUTION -- ``team`` is the athlete's CURRENT team (its ref is literally
``/seasons/{current}/teams/{id}``), NOT their team in any past season. Season
team belongs to player_season_stats' ``statistics[].teamId`` or to player_box.
Likewise bio is a CURRENT snapshot that ESPN overwrites -- era-correct height /
jersey / weight is not obtainable from this or any other ESPN endpoint.

Athlete ids come from the ``espn_nba_player_boxscores`` release on
sportsdataverse-data -- the union of every athlete who appeared in a box score
across the requested seasons, which costs zero extra HTTP. (ESPN's core-v2
``/seasons/{y}/athletes`` index is NOT a shortcut: it pages 100 ``{"$ref"}``
links at a time -- 78 pages for one season -- and hydrates nothing. Its only
advantage is listing roster-only players who never appeared in a game.)

Requirements:
    Uses ``espn_nba_player_core`` from sportsdataverse-py.
"""

import argparse
import concurrent.futures
import gc
import io
import json
import logging
import time
from pathlib import Path

from tqdm import tqdm

from sportsdataverse.nba import espn_nba_player_core
from sportsdataverse.dl_utils import download


logging.basicConfig(
    level=logging.INFO,
    filename="hoopR_nba_raw_player_core_logfile.txt",
)
logger = logging.getLogger(__name__)

PATH_TO_OUTPUT = "nba/player_core/json"
PLAYER_BOX_RELEASE = (
    "https://github.com/sportsdataverse/sportsdataverse-data/releases/"
    "download/espn_nba_player_boxscores/player_box_{season}.parquet"
)
# Per-athlete endpoint; ESPN rate-limits these harder than per-team ones.
DEFAULT_THREADS = 4


def _athlete_ids_for_season(season):
    """Unique integer athlete ids that appeared in ``season``'s box scores.

    Never raises -- on any failure (network, missing release asset, schema
    drift) logs a warning and returns ``[]`` so one bad season can't abort a
    multi-season run.
    """
    url = PLAYER_BOX_RELEASE.format(season=int(season))
    try:
        import pandas as pd

        resp = download(url)
        content = resp.content if hasattr(resp, "content") else resp
        df = pd.read_parquet(io.BytesIO(content), columns=["athlete_id"])
        return sorted(
            {
                int(x)
                for x in df["athlete_id"].dropna().unique()
                if str(x).strip().isdigit() or isinstance(x, (int, float))
            }
        )
    except Exception as e:  # noqa: BLE001
        logger.warning(f"could not list player_box athletes for {season}: {e!r}")
        return []


def _athlete_ids_for_range(start_year, end_year):
    """Union of athlete ids across the requested season range.

    A core record is per-athlete, so an athlete spanning ten seasons is
    fetched once, not ten times.
    """
    seen = set()
    for season in range(start_year, end_year + 1):
        ids = _athlete_ids_for_season(season)
        logger.info(f"season={season} player_box athletes={len(ids)}")
        seen.update(ids)
    return sorted(seen)


def download_player_core(athlete_id, rerun_existing):
    out_path = Path(PATH_TO_OUTPUT) / f"{athlete_id}.json"
    if out_path.exists() and not rerun_existing:
        return f"skip {athlete_id}"
    try:
        raw = espn_nba_player_core(athlete_id=int(athlete_id), return_parsed=False)
        if isinstance(raw, (bytes, str)):
            raw = json.loads(raw)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(raw, f, indent=0, sort_keys=False)
        return f"ok {athlete_id}"
    except Exception as e:  # noqa: BLE001
        logger.warning(f"athlete_id={athlete_id} failed: {e!r}")
        return f"err {athlete_id}: {e}"


def download_player_core_batch(athlete_ids, rerun_existing, cores):
    threads = min(cores, max(1, len(athlete_ids)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futs = {
            executor.submit(download_player_core, aid, rerun_existing): aid
            for aid in athlete_ids
        }
        for fut in tqdm(
            concurrent.futures.as_completed(futs),
            total=len(futs),
            desc="NBA player core",
        ):
            fut.result()


def main():
    start_year = max(args.start_year, 2002)  # player_box release starts 2002
    end_year = args.end_year if args.end_year is not None else start_year
    cores = args.cores if args.cores is not None else DEFAULT_THREADS

    Path(PATH_TO_OUTPUT).mkdir(parents=True, exist_ok=True)
    athlete_ids = _athlete_ids_for_range(start_year, end_year)
    logger.info(f"range={start_year}-{end_year} unique athletes={len(athlete_ids)}")
    if not athlete_ids:
        logger.info("No athlete ids resolved; nothing to scrape.")
        return

    t0 = time.time()
    download_player_core_batch(athlete_ids, args.rerun_existing, cores)
    logger.info(
        f"{(time.time() - t0) / 60:.2f} minutes to download {len(athlete_ids)} core records."
    )
    gc.collect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_year",
        "-s",
        type=int,
        required=True,
        help="First season whose box scores are mined for athlete ids (YYYY).",
    )
    parser.add_argument(
        "--end_year",
        "-e",
        type=int,
        help="Last season whose box scores are mined for athlete ids (YYYY).",
    )
    parser.add_argument(
        "--cores",
        "-c",
        type=int,
        default=DEFAULT_THREADS,
        help="Concurrent worker threads (default 4 -- ESPN rate-limits per-athlete more aggressively).",
    )
    parser.add_argument(
        "--rerun_existing",
        "-r",
        nargs="?",
        const=True,
        default=False,
        type=lambda v: str(v).lower() in ("true", "1", "yes", "y", "t"),
        help="Re-scrape a core record even when the output file already exists. Bio is a current-state snapshot, so a periodic -r refresh is how it stays current.",
    )
    args = parser.parse_args()

    main()
