#!/bin/bash
while getopts s:e:r: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
        r) RESCRAPE=${OPTARG};;
    esac
done
for i in $(seq ${START_YEAR} ${END_YEAR})
do
    echo "$i"
    git config --local user.email "action@github.com"
    git config --local user.name "Github Action"
    git pull
    python3 python/scrape_nba_schedules.py -s $i -e $i
    python3 python/scrape_nba_json.py -s $i -e $i
    git pull
    git add .
    git pull  >> /dev/null
    git add nba/* >> /dev/null
    git pull  >> /dev/null
    git commit -m "NBA Raw Updated (Start: $i End: $i)" || echo "No changes to commit"
    git pull  >> /dev/null
    git push --quiet
done