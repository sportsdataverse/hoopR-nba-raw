
import argparse
import concurrent.futures
import gc
import json
import http
import logging
import numpy as np
import os
import pyreadr
import pyarrow as pa
import pandas as pd
import re
import sportsdataverse as sdv
import time
import traceback
import urllib.request
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap, repeat
from pathlib import Path
from tqdm import tqdm


logging.basicConfig(level=logging.info, filename='hoopR_nba_raw_logfile.txt')
logger = logging.getLogger(__name__)

path_to_raw = "nba/json/raw"
path_to_final = "nba/json/final"
path_to_errors = "nba/errors"
run_processing = True
rescrape_all = False
MAX_THREADS = 30

def download_game_pbps(games, process, path_to_raw, path_to_final):
    threads = min(MAX_THREADS, len(games))

    with concurrent.futures.ThreadPoolExecutor(max_workers = threads) as executor:
        result = list(executor.map(download_game, games, repeat(process), repeat(path_to_raw), repeat(path_to_final)))
        return result

def download_game(game, process, path_to_raw, path_to_final):

    # this finds our json files
    path_to_raw_json = f"{path_to_raw}/"
    path_to_final_json = f"{path_to_final}/"
    Path(path_to_raw_json).mkdir(parents = True, exist_ok = True)
    Path(path_to_final_json).mkdir(parents = True, exist_ok = True)
    try:
        g = sdv.nba.espn_nba_pbp(game_id = game, raw = True)
        with open(f"{path_to_raw_json}{game}.json", "w") as f:
            json.dump(g, f, indent = 0, sort_keys = False)
    except (TypeError) as e:
        logger.info(f"TypeError: game_id = {game}\n {traceback.format_exc()}")
        pass
    except (IndexError) as e:
        logger.info(f"IndexError:  game_id = {game}\n {traceback.format_exc()}")
        pass
    except (KeyError) as e:
        logger.info(f"KeyError: game_id =  game_id = {game}\n {traceback.format_exc()}")
        pass
    except (ValueError) as e:
        logger.info(f"DecodeError: game_id = {game}\n {traceback.format_exc()}")
        pass
    except (AttributeError) as e:
        logger.info(f"AttributeError: game_id = {game}\n {traceback.format_exc()}")
        pass
    if process == True:
        try:
            processed_data = sdv.nba.nba_pbp_disk(
                game_id = game,
                path_to_json = path_to_raw
            )

            result = sdv.nba.helper_nba_pbp(
                game_id = game,
                pbp_txt = processed_data
            )
            fp = f"{path_to_final_json}{game}.json"
            with open(fp, "w") as f:
                json.dump(result, f, indent = 0, sort_keys = False)
        except (FileNotFoundError) as e:
            logger.info(f"FileNotFoundError: game_id = {game}\n {traceback.format_exc()}")
            pass
        except (TypeError) as e:
            logger.info(f"TypeError: game_id = {game}\n {traceback.format_exc()}")
            pass
        except (IndexError) as e:
            logger.info(f"IndexError:  game_id = {game}\n {traceback.format_exc()}")
            pass
        except (KeyError) as e:
            logger.info(f"KeyError: game_id =  game_id = {game}\n {traceback.format_exc()}")
            pass
        except (ValueError) as e:
            logger.info(f"DecodeError: game_id = {game}\n {traceback.format_exc()}")
            pass
        except (AttributeError) as e:
            logger.info(f"AttributeError: game_id = {game}\n {traceback.format_exc()}")
            pass

    time.sleep(0.5)

def add_game_to_schedule(schedule):
    game_files = [int(game_file.replace(".json", "")) for game_file in os.listdir(path_to_final)]
    schedule["game_json"] = schedule["game_id"].astype(int).isin(game_files)
    schedule["game_json_url"] = np.where(
        schedule["game_json"] == True,
        schedule["game_id"].apply(lambda x: f"https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-raw/main/nba/json/final/{x}.json"),
        None
    )
    schedule.to_parquet(f"nba/schedules/parquet/nba_schedule_{year}.parquet", index = None)
    pyreadr.write_rds(f"nba/schedules/rds/nba_schedule_{year}.rds", schedule, compress = "gzip")
    return

def main():

    if args.start_year < 2002:
        start_year = 2002
    else:
        start_year = args.start_year
    if args.end_year is None:
        end_year = start_year
    else:
        end_year = args.end_year
    process = args.process
    years_arr = range(start_year, end_year + 1)

    for year in years_arr:
        schedule = pd.read_parquet(f"nba/schedules/parquet/nba_schedule_{year}.parquet", engine = "auto", columns = None)
        schedule = schedule.sort_values(by = ["season", "season_type"], ascending = True)
        schedule["game_id"] = schedule["game_id"].astype(int)
        schedule = schedule[schedule["status_type_completed"] == True]
        if args.rescrape == False:
            schedule_in_repo = pd.read_parquet("https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-data/blob/main/nba/nba_games_in_data_repo.parquet",
                                               engine = "auto", columns = None)
            schedule_in_repo["game_id"] = schedule_in_repo["game_id"].astype(int)
            done_already = schedule_in_repo["game_id"]
            schedule = schedule[~schedule["game_id"].isin(done_already)]
        schedule = schedule[schedule["season"] >= 2002]

        logger.info(f"Scraping NBA PBP for {year}...")
        games = schedule[(schedule["season"] == year)].reset_index()["game_id"].tolist()

        if len(games) == 0:
            logger.info(f"{len(games)} Games to be scraped, skipping")
            continue

        logger.info(f"Number of Games: {len(games)}")
        bad_schedule_keys = pd.DataFrame()

        t0 = time.time()
        download_game_pbps(games, process, path_to_raw, path_to_final)
        t1 = time.time()
        logger.info(f"{(t1-t0)/60} minutes to download {len(games)} game play-by-plays.")

        logger.info(f"Finished NBA PBP for {year}...")

        schedule = add_game_to_schedule(schedule)


    gc.collect()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", "-s", type = int, required = True, help = "Start year of NBA Schedule period (YYYY), eg. 2023 for 2022-23 season")
    parser.add_argument("--end_year", "-e", type = int, help = "End year of NBA Schedule period (YYYY), eg. 2023 for 2022-23 season")
    parser.add_argument("--rescrape", "-r", type = bool, default = True, help = "Rescrape all games in the schedule period")
    parser.add_argument("--process", "-p", type = bool, default = True, help = "Run processing pipeline for games in the schedule period")
    args = parser.parse_args()

    main()