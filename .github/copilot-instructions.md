# hoopR-nba-raw Copilot Instructions

## Project Context

This repo is the Python ESPN-scrape stage for the NBA. It writes
per-game JSON under `nba/json/final/{game_id}.json` and commits results
to `main`. Every push wakes the downstream R parser in `hoopR-nba-data`
via `repository_dispatch` (event-type `daily_nba_data`, defined in
`.github/workflows/hoopR_nba_data_trigger.yaml`).

Pipeline: `ESPN -> hoopR-nba-raw [HERE] -> hoopR-nba-data -> sportsdataverse-data -> hoopR`.

Do not confuse with `hoopR-nba-stats-raw` (NBA Stats API cache) or
`hoopR-mbb-raw` (ESPN men's college basketball, same shape).

## Repository Workflow

- Branch from `main`; `main` is the default and release branch.
- The CI entry point is `scripts/daily_nba_scraper.sh -s <START> -e <END> -r <true|false>`.
- Scrapers shell out to `sportsdataverse-py`. Fix ESPN parser bugs upstream there, not here.
- Don't reorganize the `nba/` output tree without aligning `hoopR-nba-data/R/espn_nba_0[1-3]_*.R`.
- The daily commit message `"NBA Raw Updated (Start: YYYY End: YYYY)"` is parsed downstream — do not change the format.

## Build & Development Commands

```sh
bash scripts/daily_nba_scraper.sh -s 2025 -e 2025 -r false
python3 python/espn_nba_01_schedules_scrape.py    -s 2025 -e 2025 -r false
python3 python/espn_nba_02_pbp_scrape.py          -s 2025 -e 2025 -r false
```

`-r true` forces re-scrape; `-r false` skips files already on disk. Outputs:

- `nba/schedules/{rds,parquet}/nba_schedule_{year}.{ext}`
- `nba/nba_schedule_master.parquet` (concatenated master)
- `nba/json/final/{game_id}.json` (consumed downstream)
- `nba/json/raw/{game_id}.json`, `nba/errors/` (forensics)

## Code Style

- Follow the parent SDK's Python conventions: snake_case, 4-space indent.
- Prefer `pathlib.Path`, `concurrent.futures.ThreadPoolExecutor` for parallelism, `tqdm` for progress.
- Don't add bespoke ESPN parsing here — call into `sportsdataverse.nba.*` and persist its output.
- Keep `requirements.txt` minimal and pin via `sportsdataverse[all]>=...`.
- Log to `hoopR_nba_raw_logfile.txt` via the module-level `logger`; the shell driver redirects scraper stdout to `daily_nba.out`.

## Cross-Repo References

- Shared conventions: <https://github.com/sportsdataverse/hoopR/blob/main/CLAUDE.md>
- SDK internals: <https://github.com/sportsdataverse/sportsdataverse-py/blob/main/CLAUDE.md>
- Downstream parser: <https://github.com/sportsdataverse/hoopR-nba-data>

## Conventional Commits

For human-authored commits, use: `type(scope): description`. Common
types: `feat`, `fix`, `chore`, `ci`, `docs`, `refactor`. Use `type!:`
or a `BREAKING CHANGE:` footer for breaking changes.

The CI-driven daily scrape commit format is fixed as
`"NBA Raw Updated (Start: <year> End: <year>)"` — do not change it.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
