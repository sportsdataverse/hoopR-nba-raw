"""The schedule id cast: every id becomes Int64, missing stays null, a non-integer id raises."""

import math

import pandas as pd
import pyreadr
import pytest
from espn_nba_01_schedules_scrape import cast_ids_int64, write_schedule


def test_text_float_and_int_ids_become_int64_with_nulls_kept():
    df = pd.DataFrame(
        {
            "game_id": [401, 402, 403],
            "home_id": [" 12", "", None],
            "tournament_id": [3.0, math.nan, 5.0],
            "venue_id": pd.array([7, None, 9], dtype="Int64"),
            "name": ["a", "", None],
        }
    )
    name = df["name"].copy()
    out = cast_ids_int64(df)
    for col in ("game_id", "home_id", "tournament_id", "venue_id"):
        assert str(out[col].dtype) == "Int64", col
    assert out["home_id"].tolist()[0] == 12
    assert out["home_id"].isna().tolist() == [False, True, True]
    assert out["tournament_id"].isna().tolist() == [False, True, False]
    assert out["name"].equals(name)  # non-id columns untouched


@pytest.mark.parametrize("bad", [["abc"], ["1.5"], [1.5]])
def test_non_integer_id_raises(bad):
    with pytest.raises(ValueError, match="home_id"):
        cast_ids_int64(pd.DataFrame({"home_id": bad}))


def test_rds_ids_are_r_integers_with_na(tmp_path):
    df = pd.DataFrame({"game_id": [401, 402], "home_id": ["12", ""], "attendance": [5, 6]})
    write_schedule(df, tmp_path / "s.parquet", str(tmp_path / "s.rds"))
    assert str(pd.read_parquet(tmp_path / "s.parquet")["home_id"].dtype) == "Int64"
    rds = pyreadr.read_r(str(tmp_path / "s.rds"))[None]
    assert rds["home_id"].isna().tolist() == [False, True] and rds["game_id"].tolist() == [401, 402]


def test_rds_id_too_big_for_r_integer_raises(tmp_path):
    with pytest.raises(ValueError, match="game_id"):
        write_schedule(
            pd.DataFrame({"game_id": [2**31]}), tmp_path / "s.parquet", str(tmp_path / "s.rds")
        )
