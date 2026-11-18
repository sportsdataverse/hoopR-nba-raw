import argparse
import concurrent.futures
import gc
import logging
import os
import time
from itertools import repeat
from pathlib import Path

import pandas as pd
import pyreadr
import sportsdataverse as sdv
from sportsdataverse.scrape.espn.cli import str2bool

logging.basicConfig(level=logging.INFO, filename="hoopR_nba_raw_logfile.txt")
logger = logging.getLogger(__name__)

path_to_schedules = "nba/schedules"
final_file_name = "nba/nba_schedule_master.parquet"
MAX_THREADS = 30


#: Every ``id`` / ``*_id`` column of a schedule frame is published as Int64 (R integer in the rds), whatever
#: sportsdataverse-py returns: text ids before its Int64 id contract (#1003), nullable Int64 after it.
#: ``game_id`` and ``tournament_id`` included.
def _is_id(col):
    return col == "id" or col.endswith("_id")


_INT_TEXT = r"^-?\d+(\.0+)?$"


def cast_ids_int64(df):
    """Cast every ``id`` / ``*_id`` column to pandas' nullable Int64, in place, and return the frame.

    Strict: text is stripped and ``""`` is null; a whole-number float is accepted and NaN is null; any other
    value (``"abc"``, ``1.5``) raises ValueError naming the column instead of being nulled.
    """
    for col in [c for c in df.columns if _is_id(c)]:
        s = df[col]
        if pd.api.types.is_integer_dtype(s):
            df[col] = s.astype("Int64")
            continue
        if pd.api.types.is_float_dtype(s):
            num = s
        else:
            text = s.astype("string").str.strip()
            text = text.mask(text == "", pd.NA)
            bad = text.notna() & ~text.fillna("0").str.fullmatch(_INT_TEXT)
            if bad.any():
                raise ValueError(f"{col}: not an integer id: {text[bad].unique()[:5].tolist()}")
            num = pd.to_numeric(text)
        if (num.notna() & (num % 1 != 0)).any():
            raise ValueError(f"{col}: not an integer id: {num[num % 1 != 0].unique()[:5].tolist()}")
        df[col] = num.astype("Int64")
    return df


#: ESPN moved to 40xxxxxxx game ids with the 2012-13 season; before it, a 4002xxxxx id is ESPN's alternate copy
#: of a game it already lists under its old id (every Chicago Bulls game of 1998-99 and 1999-2000).
_ALT_ID_FLOOR = 400_000_000
_ALT_ID_LAST_SEASON = 2012


def clean_schedule(ev, season):
    """Keep only ``season``'s games, each once.

    ESPN files the 1998-99 lockout games under both the 1999 and the 2000 calendars and keeps a second,
    4002xxxxx-id copy of every Bulls game of those two seasons (some with off-canon opponent ids, so a
    (date, home, away) key does not catch them). Three rules, applied in order:

    1. Before 2012-13, drop an alternate id that shares its US Eastern date and a team with an old-id game
       (an alternate with no such twin is a real game and stays).
    2. Drop rows ESPN labels with another season.
    3. Drop rows dated outside 1 July of ``season - 1`` to 31 October of ``season`` (the 2020 bubble ended in
       October; a lockout game labeled 2000 is dated February-May 1999).

    Every other season's committed schedule is unchanged by these rules (tests/test_clean_schedule.py).
    """
    day = pd.to_datetime(ev["date"], utc=True).dt.tz_convert("America/New_York").dt.date
    gid = pd.to_numeric(ev["game_id"])
    alt = gid >= _ALT_ID_FLOOR
    if season <= _ALT_ID_LAST_SEASON and alt.any():
        old = ev[~alt]
        seen = {
            (d, t) for d, h, a in zip(day[~alt], old["home_id"], old["away_id"]) for t in (h, a)
        }
        twin = pd.Series(
            [
                alt.iloc[i] and ((d, h) in seen or (d, a) in seen)
                for i, (d, h, a) in enumerate(zip(day, ev["home_id"], ev["away_id"]))
            ],
            index=ev.index,
        )
        keep = ~twin
    else:
        keep = pd.Series(True, index=ev.index)
    keep &= pd.to_numeric(ev["season"]) == season
    start, end = pd.Timestamp(f"{season - 1}-07-01").date(), pd.Timestamp(f"{season}-10-31").date()
    keep &= (day >= start) & (day <= end)
    return ev[keep].reset_index(drop=True)


def write_schedule(df, parquet_path, rds_path=None):
    """Cast the ids, then write the parquet (Int64) and the rds.

    The rds goes through pyreadr, which cannot write a pandas nullable Int64 holding a missing value
    (``sportsdataverse._rds.write_rds`` keeps NA but rejects the nested schedule columns). So the ids go
    to the rds as nullable Int32, an R integer with NA (every ESPN id fits in 32 bits; a larger one
    raises), and any other Int64 column as a double.
    """
    df = cast_ids_int64(df)
    df.to_parquet(parquet_path, index=False)
    if rds_path is not None:
        rds = df.copy()
        for col in rds.columns:
            if _is_id(col):
                if (rds[col].abs() > 2**31 - 1).any():  # astype("Int32") would wrap silently
                    raise ValueError(f"{col}: an id does not fit an R integer")
                rds[col] = rds[col].astype("Int32")
            elif str(rds[col].dtype) == "Int64":
                rds[col] = rds[col].astype("float64")
        pyreadr.write_rds(rds_path, rds, compress="gzip")
    return df


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
    ev = clean_schedule(ev, season)

    Path(f"{path_to_schedules}/parquet").mkdir(parents=True, exist_ok=True)
    Path(f"{path_to_schedules}/rds").mkdir(parents=True, exist_ok=True)
    if path_to_schedules is not None:
        write_schedule(
            ev,
            f"{path_to_schedules}/parquet/nba_schedule_{season}.parquet",
            f"{path_to_schedules}/rds/nba_schedule_{season}.rds",
        )


def main():
    if args.start_year < 1994:
        start_year = 1994
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
        logger.info(
            f"{(t1 - t0) / 60} minutes to download {len(years_arr)} years of season schedules."
        )

    parquet_files = [
        pos_parquet.replace(".parquet", "")
        for pos_parquet in os.listdir(path_to_schedules + "/parquet")
        if pos_parquet.endswith(".parquet")
    ]
    glued_data = pd.DataFrame()
    for index, js in enumerate(parquet_files):
        x = pd.read_parquet(
            f"{path_to_schedules}/parquet/{js}.parquet", engine="auto", columns=None
        )
        glued_data = pd.concat([glued_data, cast_ids_int64(x)], axis=0)
    glued_data["status_display_clock"] = glued_data["status_display_clock"].astype(str)
    glued_data.to_parquet(final_file_name, index=False)
    gc.collect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_year",
        "-s",
        type=int,
        required=True,
        help="Start year of NBA Schedule period (YYYY)",
    )
    parser.add_argument("--end_year", "-e", type=int, help="End year of NBA Schedule period (YYYY)")
    parser.add_argument(
        "--rescrape",
        "-r",
        type=str2bool,
        default=False,
        help="Rescrape all games in the schedule period",
    )
    args = parser.parse_args()

    main()
