#!/bin/bash
# Scrape raw ESPN NBA game JSON, schedules and per-dataset payloads
# Usage: bash scripts/daily_nba_scraper.sh -s 2025 -e 2025

while getopts s:e:r: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
        r) RESCRAPE=${OPTARG};;
    esac
done

RESCRAPE=${RESCRAPE:-TRUE}
echo "Rescrape set to: $RESCRAPE"
mkdir -p logs
for i in $(seq "${START_YEAR}" "${END_YEAR}")
do
    LOGFILE="logs/hoopR_nba_raw_logfile_${i}.log"
    TMPLOG=$(mktemp "/tmp/hoopR_nba_raw_logfile_${i}.XXXXXX.log")
    echo "=== Processing season $i ==="
    # Tee inside the block writes to /tmp (untracked) so the `git pull` calls
    # don't trip over their own log output being written to a tracked file.
    {
        git pull >> /dev/null
        git config --local user.email "action@github.com"
        git config --local user.name "Github Action"
        python3 python/scrape_nba_schedules.py    -s $i -e $i -r $RESCRAPE
        python3 python/scrape_nba_json.py         -s $i -e $i -r $RESCRAPE
        python3 python/scrape_nba_standings.py    -s $i -e $i -r $RESCRAPE
        python3 python/scrape_nba_game_rosters.py -s $i -e $i -r $RESCRAPE
        python3 python/scrape_nba_draft.py        -s $i -e $i -r $RESCRAPE
        python3 python/scrape_nba_player_stats.py -s $i -e $i -r $RESCRAPE
        python3 python/scrape_nba_team_stats.py   -s $i -e $i -r $RESCRAPE
        python3 python/scrape_nba_team_rosters.py -s $i -e $i -r $RESCRAPE
        git pull >> /dev/null
        git add nba/* >> /dev/null
        git add nba/nba_schedule_master.* >> /dev/null
        git pull >> /dev/null
        git add . >> /dev/null
        git commit -m "NBA Raw Updated (Start: $i End: $i)" || echo "No changes to commit"
        git pull >> /dev/null
        git push >> /dev/null
    } 2>&1 | tee "$TMPLOG"

    # Block is finished and pushed; tee has closed $TMPLOG. Now copy the log
    # into its tracked location and commit/push it on its own.
    cp "$TMPLOG" "$LOGFILE"
    git pull --rebase >> /dev/null || true
    git add "$LOGFILE"
    git commit -m "NBA Raw log update (Start: $i End: $i)" >> /dev/null || echo "No log changes to commit"
    git push >> /dev/null
    rm -f "$TMPLOG"
done
