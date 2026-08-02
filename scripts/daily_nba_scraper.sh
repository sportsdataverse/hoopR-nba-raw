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

# Scraper failures used to be swallowed: each scraper ran bare, so a crash left
# the loop running, the partial day got committed, and the job still exited 0.
# scrape_nba_player_stats.py sat dead for two sportsdataverse-py release cycles
# that way -- aborting at import on a removed symbol, every day, silently green.
#
# run_scraper keeps that resilience (one dead scraper must not stop the others,
# and whatever DID scrape should still be committed) but records the failure so
# the run goes RED at the end and someone actually looks.
#
# NOTE: the scrapers run inside `{ ... } | tee`, and a pipe is a SUBSHELL -- a
# counter variable incremented in there is discarded when it exits. That's why
# failures go to a FILE. Do not "simplify" this to a FAILED=$((FAILED+1)) var.
FAILLOG=$(mktemp "/tmp/hoopR_nba_raw_failures.XXXXXX")
trap 'rm -f "$FAILLOG"' EXIT

run_scraper() {
    local label="$1"; shift
    "$@"
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        echo "!!! SCRAPER FAILED (rc=$rc): $label"
        echo "$label rc=$rc" >> "$FAILLOG"
    fi
    return 0
}
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
        run_scraper schedules    python3 python/scrape_nba_schedules.py    -s $i -e $i -r $RESCRAPE
        run_scraper json         python3 python/scrape_nba_json.py         -s $i -e $i -r $RESCRAPE
        run_scraper standings    python3 python/scrape_nba_standings.py    -s $i -e $i -r $RESCRAPE
        run_scraper game_rosters python3 python/scrape_nba_game_rosters.py -s $i -e $i -r $RESCRAPE
        run_scraper draft        python3 python/scrape_nba_draft.py        -s $i -e $i -r $RESCRAPE
        run_scraper player_stats python3 python/scrape_nba_player_stats.py -s $i -e $i -r $RESCRAPE
        run_scraper team_stats   python3 python/scrape_nba_team_stats.py   -s $i -e $i -r $RESCRAPE
        run_scraper team_rosters python3 python/scrape_nba_team_rosters.py -s $i -e $i -r $RESCRAPE
        run_scraper player_core  python3 python/scrape_nba_player_core.py  -s $i -e $i -r $RESCRAPE
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

# Everything that could scrape has scraped, and every partial result is
# committed and pushed -- only now do we decide the exit code. A dead scraper
# must turn the run RED; a green run over silently-missing data is worse than
# an obvious failure.
if [ -s "$FAILLOG" ]; then
    echo ""
    echo "=================================================="
    echo "SCRAPER FAILURES (data for these is NOT up to date)"
    cat "$FAILLOG"
    echo "=================================================="
    exit 1
fi
echo "All scrapers OK."
