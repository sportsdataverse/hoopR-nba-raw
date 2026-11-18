"""clean_schedule on the committed schedules: the 1998-99 lockout games sit in one season, once."""

from pathlib import Path

import pandas as pd
import pytest
from espn_nba_01_schedules_scrape import clean_schedule

PQ = Path(__file__).resolve().parents[1] / "nba" / "schedules" / "parquet"
SEASONS = sorted(int(p.stem.rsplit("_", 1)[1]) for p in PQ.glob("nba_schedule_*.parquet"))


def _read(season):
    return pd.read_parquet(PQ / f"nba_schedule_{season}.parquet")


def _games(df):
    day = pd.to_datetime(df["date"], utc=True).dt.tz_convert("America/New_York").dt.date
    return set(zip(day, df["home_id"], df["away_id"]))


@pytest.mark.parametrize("season", SEASONS)
def test_clean_is_idempotent_and_season_pure(season):
    raw = _read(season)
    out = clean_schedule(raw, season)
    if season not in (1999, 2000):
        assert out.equals(raw.reset_index(drop=True)), "only the lockout pair has rows to drop"
    assert (out["season"] == season).all()
    assert out["game_id"].is_unique
    assert clean_schedule(out, season).equals(out)


def test_the_lockout_season_is_one_season_once():
    s99, s00 = clean_schedule(_read(1999), 1999), clean_schedule(_read(2000), 2000)
    assert (s99["season_type"] == 2).sum() == 725  # the 1998-99 lockout regular season
    assert (len(s99), len(s00)) == (792, 1247)
    assert not (_games(s99) & _games(s00)), "a game in both files"
    assert not (set(s99["game_id"]) & set(s00["game_id"]))
    alt = s00[s00["game_id"] >= 400_000_000]["game_id"].tolist()
    assert sorted(alt) == [
        400218915,
        400218927,
    ]  # the two Bulls games ESPN lists only under alternate ids
    assert (pd.to_datetime(s00["date"], utc=True) >= pd.Timestamp("1999-07-01", tz="UTC")).all()
