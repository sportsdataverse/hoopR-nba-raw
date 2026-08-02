import argparse
import concurrent.futures
import logging
import os
import pyreadr
import pandas as pd
import sportsdataverse as sdv
import time
import gc
from itertools import repeat
from pathlib import Path

logging.basicConfig(level=logging.INFO, filename="hoopR_nba_raw_logfile.txt")
logger = logging.getLogger(__name__)

path_to_schedules = "nba/schedules"
final_file_name = "nba/nba_schedule_master.parquet"
MAX_THREADS = 30


def download_game_schedules(seasons, path_to_schedules):
    threads = min(MAX_THREADS, len(seasons))

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        result = list(executor.map(download_schedule, seasons, repeat(path_to_schedules)))
        return result


def download_schedule(season, path_to_schedules=None):
    logger.info(f"Scraping NBA schedules for year {season}...")
    df = sdv.nba.espn_nba_calendar(season, ondays=True, return_as_pandas=True)

    calendar = df["dateURL"].str.replace("-", "").tolist()
    ev = pd.DataFrame()
    for d in calendar:
        date_schedule = sdv.nba.espn_nba_schedule(dates=d, return_as_pandas=True)
        ev = pd.concat([ev, date_schedule], axis=0, ignore_index=True)
    ev = ev[ev["season_type"].isin([2, 3, 5])]

    ev = ev.drop_duplicates(subset=["game_id"], ignore_index=True)

    Path(f"{path_to_schedules}/parquet").mkdir(parents=True, exist_ok=True)
    Path(f"{path_to_schedules}/rds").mkdir(parents=True, exist_ok=True)
    if path_to_schedules is not None:
        ev.to_parquet(f"{path_to_schedules}/parquet/nba_schedule_{season}.parquet", index=False)
        pyreadr.write_rds(f"{path_to_schedules}/rds/nba_schedule_{season}.rds", ev, compress="gzip")


def main():
    if args.start_year < 2002:
        start_year = 2002
    else:
        start_year = args.start_year
    if args.end_year is None:
        end_year = start_year
    else:
        end_year = args.end_year
    years_arr = range(start_year, end_year + 1)

    if args.rescrape == True:
        t0 = time.time()
        download_game_schedules(years_arr, path_to_schedules)
        t1 = time.time()
        logger.info(f"{(t1 - t0) / 60} minutes to download {len(years_arr)} years of season schedules.")

    parquet_files = [
        pos_parquet.replace(".parquet", "")
        for pos_parquet in os.listdir(path_to_schedules + "/parquet")
        if pos_parquet.endswith(".parquet")
    ]
    glued_data = pd.DataFrame()
    for index, js in enumerate(parquet_files):
        x = pd.read_parquet(f"{path_to_schedules}/parquet/{js}.parquet", engine="auto", columns=None)
        glued_data = pd.concat([glued_data, x], axis=0)
    glued_data["status_display_clock"] = glued_data["status_display_clock"].astype(str)
    glued_data.to_parquet(final_file_name, index=False)
    gcol = gc.collect()


def str2bool(value):
    """Parse a shell-supplied boolean flag.

    argparse's ``type=bool`` is a trap: ``bool("false")`` is ``True``, so
    ``-r false`` silently forced a full re-scrape. Mirrors the wehoop-wbb-raw
    house converter -- unrecognised text parses as False, because the
    expensive mistake is re-scraping the archive, not skipping a run.
    """
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "t", "yes", "y", "on")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", "-s", type=int, required=True, help="Start year of NBA Schedule period (YYYY)")
    parser.add_argument("--end_year", "-e", type=int, help="End year of NBA Schedule period (YYYY)")
    parser.add_argument(
        "--rescrape", "-r", type=str2bool, default=True, help="Rescrape all games in the schedule period"
    )
    args = parser.parse_args()

    main()
