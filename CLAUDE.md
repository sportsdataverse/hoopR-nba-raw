# CLAUDE.md — hoopR-nba-raw Development Guide

## Repo Overview

`hoopR-nba-raw` is the Python-side scraper that pulls ESPN NBA schedules
and per-game JSON, persists them to disk under `nba/schedules/` and
`nba/json/final/{game_id}.json`, and commits the results back to this
repo. Every push to `main` fires a `repository_dispatch` that wakes up
the downstream R parser in `hoopR-nba-data`. This repo is the
authoritative cache of raw ESPN NBA payloads — the parsing layer never
re-hits ESPN, it reads from here.

## Pipeline Position

```
ESPN APIs --[python scrape]--> hoopR-nba-raw [HERE]
                                    | push trigger
                                    v
                               hoopR-nba-data --[release upload]--> sportsdataverse-data
                                                                         | piggyback
                                                                         v
                                                                    hoopR R package
```

The push trigger is `.github/workflows/hoopR_nba_data_trigger.yaml`,
which fires `repository_dispatch` event-type `daily_nba_data` against
`sportsdataverse/hoopR-nba-data`.

This is the **ESPN NBA** raw cache. Do not confuse with:

- `hoopR-mbb-raw` — ESPN men's college basketball, same shape
- `hoopR-nba-stats-raw` — NBA Stats API cache, different upstream
- `hoopR-nba-data` — the R-side parser that consumes this repo

## Build & Development Commands

The repo is driven by `scripts/daily_nba_scraper.sh`, which loops the
season range and runs **eight** scrapers per season (schedules, json,
standings, game_rosters, draft, player_stats, team_stats, team_rosters),
then commits + pushes. All seasons are integer years.

```sh
# Full daily flow for one or more seasons (the entry point CI uses)
bash scripts/daily_nba_scraper.sh -s 2025 -e 2025 -r false

# Or call any scraper directly when iterating
python3 python/scrape_nba_schedules.py    -s 2025 -e 2025 -r false
python3 python/scrape_nba_json.py         -s 2025 -e 2025 -r false
python3 python/scrape_nba_standings.py    -s 2025 -e 2025 -r false
python3 python/scrape_nba_game_rosters.py -s 2025 -e 2025 -r false
python3 python/scrape_nba_draft.py        -s 2025 -e 2025 -r false
python3 python/scrape_nba_player_stats.py -s 2025 -e 2025 -r false
python3 python/scrape_nba_team_stats.py   -s 2025 -e 2025 -r false
python3 python/scrape_nba_team_rosters.py -s 2025 -e 2025 -r false

# Helpers (not part of the daily flow)
python3 python/process_nba_schedules.py
python3 python/add_game_links_to_schedule.py
python3 python/nba_pbp_creation.py
```

`-r true` forces re-scrape of games already on disk; `-r false` skips
existing files. **The `-r` flag defaults to `TRUE`** when unset
(`RESCRAPE=${RESCRAPE:-TRUE}`), so CI always passes `-r false` explicitly.
Output paths the scrapers write under:

- `nba/schedules/{rds,parquet}/nba_schedule_{year}.{ext}`
- `nba/nba_schedule_master.parquet` — concatenated master schedule
- `nba/json/final/{game_id}.json` — final clean payload, consumed by `hoopR-nba-data`
- `nba/json/raw/{game_id}.json`   — raw ESPN response (kept for forensics)
- `nba/errors/`                   — failed-game records
- `nba/{standings,game_rosters,draft,player_season_stats,team_stats,team_rosters}/` — per-dataset payloads
- `logs/hoopR_nba_raw_logfile_{year}.log` — per-season run log, committed separately

The scrapers default to `season_type in (2, 3, 5)` for regular season,
postseason, and play-in. Pre-2002 seasons are clamped to 2002 in
`scrape_nba_schedules.py`.

## Project Structure

```
python/
  scrape_nba_schedules.py     # ESPN schedule scrape -> nba/schedules/
  scrape_nba_json.py          # Per-game JSON scrape -> nba/json/final/{game_id}.json
  scrape_nba_standings.py     # -> nba/standings/
  scrape_nba_game_rosters.py  # -> nba/game_rosters/
  scrape_nba_draft.py         # -> nba/draft/
  scrape_nba_player_stats.py  # -> nba/player_season_stats/
  scrape_nba_team_stats.py    # -> nba/team_stats/
  scrape_nba_team_rosters.py  # -> nba/team_rosters/
  process_nba_schedules.py    # Schedule post-processing (helper, not in daily flow)
  add_game_links_to_schedule.py
  nba_pbp_creation.py         # PBP compile prototype (not in daily flow)
scripts/
  daily_nba_scraper.sh        # CI entry point — per-season loop over 8 scrapers
nba/                          # Committed scraped output (consumed downstream)
  schedules/{rds,parquet}/
  json/{raw,final}/
  errors/
  standings/  game_rosters/  draft/  player_season_stats/  team_stats/  team_rosters/
.github/workflows/
  hoopR_nba_data_trigger.yaml # Fires repository_dispatch (event-type daily_nba_data) on push
requirements.txt              # Python deps, pinned via sportsdataverse-py
```

## Daily Workflow

The current CI driver is `scripts/daily_nba_scraper.sh`, invoked by the
sportsdataverse umbrella scheduler. It runs the eight scrapers per season,
commits any new files under `nba/`, and pushes (a second commit pushes the
per-season log under `logs/`). That push fires
`.github/workflows/hoopR_nba_data_trigger.yaml`, which dispatches
`daily_nba_data` against `sportsdataverse/hoopR-nba-data`.

- **Commit message format is load-bearing**: the scraper commits as
  `"NBA Raw Updated (Start: <year> End: <year>)"`. Downstream tooling
  parses years from the head commit message — do not change the
  pattern without coordinating across `hoopR-nba-data`.
- **One push per scrape**: the daily script batches the schedule and
  JSON output into one push per season iteration so the downstream
  trigger fires once per season.
- **Force-pushes do not fire the trigger** (`push` event only). Push
  normally; never force-push to `main`.

The Python scrapers depend on `sportsdataverse-py` (declared in
`requirements.txt`); they call `sdv.nba.espn_nba_pbp(game_id, raw=True)`,
`sdv.nba.espn_nba_calendar()`, and `sdv.nba.espn_nba_schedule()`. Bug
fixes to ESPN parsing belong in `sportsdataverse-py` NBA modules — not
here.

## Cross-Repo References

- Shared conventions and broader context: <https://github.com/sportsdataverse/hoopR/blob/main/CLAUDE.md>
- Python scraper internals (the SDK this repo calls): <https://github.com/sportsdataverse/sportsdataverse-py/blob/main/CLAUDE.md>
- Downstream parser: <https://github.com/sportsdataverse/hoopR-nba-data>
- Sister repo (same shape, different league): <https://github.com/sportsdataverse/hoopR-mbb-raw>

## Project-Specific Gotchas

- `python/scrape_nba_json.py` writes JSON under `nba/json/final/{game_id}.json`. Downstream `hoopR-nba-data` reads from `https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-raw/main/nba/...`, so the file paths and commit-to-`main` are load-bearing.
- The per-push `hoopR_nba_data_trigger.yaml` workflow only fires on `push` and `workflow_dispatch`. Force-pushes can land changes without firing downstream jobs — push normally.
- Large additions of `nba/json/final/*.json` files inflate the repo. Don't reorganize the `nba/` tree without coordinating the change in `hoopR-nba-data`'s creation scripts (`R/espn_nba_0[1-3]_*.R`).
- ESPN JSON schema drift is handled in `sportsdataverse-py` (the call boundary). If a scraper starts dropping fields, fix the SDK first; this repo should stay thin.
- The shell driver uses `git pull` between steps and silently swallows output. If the scraper appears stuck, check `hoopR_nba_raw_logfile.txt` and `daily_nba.out` at the repo root — those carry the actual scraper output.
- Pre-2002 seasons are clamped to 2002 in `scrape_nba_schedules.py`; passing `-s 1999` will not back-fill earlier years.

## Commit Convention

The CI driver commits as `"NBA Raw Updated (Start: <year> End: <year>)"`
— **do not change this format**; downstream tooling parses the years
from the head commit message.

For human-authored commits (code changes, not daily scrape output), use
[Conventional Commits](https://www.conventionalcommits.org/):

```
feat(scrape): add play-in season_type=5 to scrape_nba_schedules.py
fix(scrape): handle 503s in scrape_nba_json without aborting the season loop
chore(deps): bump sportsdataverse-py pin in requirements.txt
ci: align push trigger with new workflow secret name
```

Prefer scoped subjects (`feat(scrape): ...`, `ci(trigger): ...`). Use
`type!:` or a `BREAKING CHANGE:` footer for breaking changes. Split
unrelated work into separate commits for reviewability.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
