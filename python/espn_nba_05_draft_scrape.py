"""Scrape ESPN NBA draft results per year.

Output: ``nba/standings/json/{season}.json`` -- raw ESPN response. The
downstream R parser in ``hoopR-nba-data`` reads these JSONs to build the
per-season tidy standings frame and upload it to the
``espn_nba_standings`` release tag on ``sportsdataverse-data``.

Standings is one HTTP call per season -- there is no per-team iteration,
so concurrency only helps when scraping a multi-season backfill.

Requirements:
    Uses the unified cross-league ``espn_nba_standings`` endpoint in
    ``sportsdataverse-py`` (sportsdataverse/_common_espn.py, re-exported
    from ``sportsdataverse.nba``). The new generic signature returns the
    raw ESPN payload by default (``return_parsed=False``) -- no ``raw=``
    kwarg like the older per-sport helpers. ``requirements.txt`` should
    pin a sportsdataverse-py version that exports it; until that release
    lands, install sdv-py from source (``pip install -e <path>/sdv-py``).
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

from sportsdataverse.dl_utils import download
from sportsdataverse.scrape.espn.persist import write_payload
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    filename="hoopR_nba_raw_draft_logfile.txt",
)
logger = logging.getLogger(__name__)

PATH_TO_OUTPUT = "nba/draft/json"
DEFAULT_THREADS = 1

# CORE-API season-draft athletes collection (returned in draft order). The
# site-API draft picks carry no athlete, so we walk this to supply the drafted
# players.
DRAFT_PROSPECTS_REF = (
    "http://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/"
    "seasons/{season}/draft/athletes"
)


def _resp_json(resp: Any) -> Any:
    return resp.json() if hasattr(resp, "json") else json.loads(
        getattr(resp, "text", resp)
    )


def _draft_prospects_in_order(season: int) -> list:
    """Walk the CORE-API season-draft athletes (paginated $refs) and return the
    prospect entities in draft order. The first len(picks) map 1:1 to overall
    picks 1..N; later prospects are undrafted. Verified against 2025 (Cooper
    Flagg #1, Dylan Harper #2, ...)."""
    out: list = []
    base = DRAFT_PROSPECTS_REF.format(season=int(season))
    page = 1
    try:
        while True:
            sep = "&" if "?" in base else "?"
            pg = _resp_json(download(f"{base}{sep}page={page}&limit=100"))
            for it in pg.get("items", []):
                ref = it.get("$ref") if isinstance(it, dict) else None
                if not ref:
                    continue
                try:
                    out.append(_resp_json(download(ref)))
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        f"draft {season}: prospect fetch failed: {e!r}"
                    )
            if page >= pg.get("pageCount", 1):
                break
            page += 1
            if page > 20:
                break
    except Exception as e:  # noqa: BLE001
        logger.warning(f"draft {season}: prospect walk failed: {e!r}")
    return out


def download_draft(
    season: int, output_dir: Path, rerun_existing: bool
) -> str:
    out_path = Path(output_dir) / f"{season}.json"
    if out_path.exists() and not rerun_existing:
        return f"skip {season}"
    try:
        # The site-API draft endpoint takes a year param and returns full
        # round/pick data (rounds may be an int count with picks[] at top
        # level). Hit the URL directly -- espn_nba_draft() does not thread a
        # year kwarg to the request.
        url = (
            "http://site.api.espn.com/apis/site/v2/sports/basketball/nba/"
            f"draft?year={int(season)}"
        )
        raw = _resp_json(download(url))
        # Inject drafted players: site-API picks have no athlete, so map the
        # core-API prospects (draft order) onto picks[i]["athlete"] -- the
        # downstream R parser (espn_nba_08_draft_creation.R) reads pick.athlete.
        prospects = _draft_prospects_in_order(int(season))
        picks = raw.get("picks") or []
        for idx, pk in enumerate(picks):
            if idx < len(prospects) and isinstance(pk, dict):
                pk["athlete"] = prospects[idx]
        if not write_payload(out_path, raw, indent=0):
            logger.warning(f"refused error/empty payload: {out_path}")
            return f"refused {out_path.stem}"
        return f"ok {season} ({len(prospects)} prospects -> {len(picks)} picks)"
    except Exception as e:
        logger.warning(f"season={season} failed: {e!r}")
        return f"err {season}: {e}"


def download_draft_batch(
    seasons: list[int],
    output_dir: Path,
    rerun_existing: bool,
    cores: int,
) -> None:
    threads = min(cores, max(1, len(seasons)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futs = {
            executor.submit(
                download_draft, s, output_dir, rerun_existing
            ): s
            for s in seasons
        }
        for fut in tqdm(
            concurrent.futures.as_completed(futs),
            total=len(futs),
            desc="NBA draft",
        ):
            fut.result()


def main() -> None:
    # ESPN NBA draft coverage starts in the 2002 season.
    if args.start_year < 2003:
        start_year = 2003
    else:
        start_year = args.start_year
    end_year = args.end_year if args.end_year is not None else start_year
    cores = args.cores if args.cores is not None else DEFAULT_THREADS
    base_output_dir = Path(args.output_dir or PATH_TO_OUTPUT)
    base_output_dir.mkdir(parents=True, exist_ok=True)
    rerun_existing = args.rerun_existing or args.force

    seasons = list(range(start_year, end_year + 1))
    logger.info(f"seasons={seasons}")
    if not seasons:
        return
    t0 = time.time()
    download_draft_batch(seasons, base_output_dir, rerun_existing, cores)
    t1 = time.time()
    logger.info(
        f"{(t1 - t0) / 60:.2f} minutes to download {len(seasons)} draft payloads."
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
        help="Start year of NBA draft scrape (YYYY), e.g. 2025 for the 2024-25 season",
    )
    parser.add_argument(
        "--end_year",
        "--end-year",
        "-e",
        dest="end_year",
        type=int,
        help="End year of NBA draft scrape (YYYY)",
    )
    parser.add_argument(
        "--cores",
        "--workers",
        "-c",
        dest="cores",
        type=int,
        default=DEFAULT_THREADS,
        help="Concurrent worker threads (default 1 -- one HTTP call per season).",
    )
    parser.add_argument(
        "--output-dir",
        "--output_dir",
        dest="output_dir",
        type=str,
        default=None,
        help=f"Override output directory (default {PATH_TO_OUTPUT}).",
    )
    parser.add_argument(
        "--rerun_existing",
        "-r",
        nargs="?",
        const=True,
        default=False,
        type=lambda v: str(v).lower() in ("true", "1", "yes", "y", "t"),
        help="Re-scrape standings even when the output file already exists. Accepts a true/false value (e.g. `-r true`) for compat with the legacy umbrella workflow; bare `-r` defaults to True.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Alias for --rerun_existing.",
    )
    args = parser.parse_args()

    main()
