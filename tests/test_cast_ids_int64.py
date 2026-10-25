"""The schedule id cast: every id becomes Int64, missing stays null, a non-integer id raises."""

import math

import pandas as pd
import pytest
from espn_nba_01_schedules_scrape import cast_ids_int64


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
