"""The CLI contract, and the rescrape defect it exists to prevent.

`argparse(type=bool)` plus `default=True` meant every daily run re-fetched the
entire archive: bash passes the string "false", and `bool("false")` is True.
"""

from __future__ import annotations

import pytest
from sportsdataverse.scrape.espn.cli import season_args, str2bool


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("false", False),
        ("False", False),
        ("FALSE", False),
        (" false ", False),
        ("0", False),
        ("no", False),
        ("off", False),
        ("", False),
        ("true", True),
        ("True", True),
        ("1", True),
        ("yes", True),
        ("on", True),
        (True, True),
        (False, False),
    ],
)
def test_str2bool_parses_shell_strings(raw, expected):
    assert str2bool(raw) is expected


def test_unrecognised_text_does_not_trigger_a_rescrape():
    """A typo in a cron definition must not re-fetch the whole archive."""
    assert str2bool("ture") is False
    assert str2bool("maybe") is False


def test_rescrape_defaults_to_false():
    """The raw tree is the checkpoint; captured payloads are never re-fetched
    unless explicitly asked for."""
    assert season_args(["--start_year", "2026"]).rescrape is False


def test_rescrape_false_string_stays_false():
    """The exact invocation the daily workflow makes: -r "$RESCRAPE"."""
    assert season_args(["--start_year", "2026", "-r", "false"]).rescrape is False


def test_rescrape_true_string_is_honoured():
    assert season_args(["--start_year", "2026", "-r", "true"]).rescrape is True


def test_end_year_defaults_to_start_year():
    args = season_args(["--start_year", "2026"])
    assert args.end_year == 2026


def test_explicit_range_is_preserved():
    args = season_args(["-s", "2007", "-e", "2013"])
    assert (args.start_year, args.end_year) == (2007, 2013)


def test_start_year_is_required():
    with pytest.raises(SystemExit):
        season_args([])
