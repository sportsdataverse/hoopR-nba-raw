"""Cross-repo stage-numbering gate for the ESPN `-raw` scraper family.

The 2026-08-02 pipeline audit fixed a convention that nothing enforced: stage
NUMBER semantics are shared across every ESPN `-raw` repo (hoopR-mbb-raw,
hoopR-nba-raw, wehoop-wbb-raw, wehoop-wnba-raw) -- 01 schedules, 02 pbp,
03 standings, 04 game_rosters, 05 draft, 06 player_stats, 07 team_stats,
08 team_rosters, 09 player_core, 99 the schedule-master creation script.
10+ is league-specific extras (draft combines, officials, ...), never governed
here. A league that has no ESPN page for a stage (college has no draft) simply
never builds that number -- a hole, and holes are NEVER compacted, because
cross-repo number semantics beat dense numbering.

Why it exists: nothing stopped a future edit from quietly building
`team_stats` under 08 in one repo while every sibling uses 07 for it, or from
reusing 07 for a different dataset. Either mistake is invisible reading one
repo in isolation -- it only shows up when you line the family up side by
side, which is exactly what this gate does inside a single repo's test run by
carrying the shared table as data.

No per-repo data is needed: holes require no justification (the whole point
of the convention), and the two accepted name spellings for 06/07 already
cover every repo in the family. That makes this file byte-identical across
all four repos -- diff it against a sibling copy as the drift check.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# NN -> acceptable base names (the `espn_<lg>_NN_<name>_scrape.py` /
# `_creation.py` stem, minus the league and the scrape/creation suffix).
# Shared, ecosystem-wide, and intentionally NOT keyed by league.
CANONICAL: dict[str, frozenset[str]] = {
    "01": frozenset({"schedules"}),
    "02": frozenset({"pbp"}),
    "03": frozenset({"standings"}),
    "04": frozenset({"game_rosters"}),
    "05": frozenset({"draft"}),
    "06": frozenset({"player_stats", "player_season_stats"}),
    "07": frozenset({"team_stats", "team_season_stats"}),
    "08": frozenset({"team_rosters"}),
    "09": frozenset({"player_core"}),
    "99": frozenset({"schedule_master"}),
}

# Inverted once: a canonical name -> the one NN it is allowed to live under,
# regardless of which NN a stray file was actually numbered.
ALIAS_TO_NUM: dict[str, str] = {name: num for num, names in CANONICAL.items() for name in names}

_STAGE_RE = re.compile(r"^espn_(?P<lg>[a-z]+)_(?P<num>\d{2})_(?P<name>.+)_(?:scrape|creation)$")


def _stages() -> dict[str, tuple[str, Path]]:
    """NN -> (base_name, path) for every numbered stage script in python/."""
    found: dict[str, tuple[str, Path]] = {}
    dupes = []
    for path in sorted((REPO / "python").glob("espn_*.py")):
        m = _STAGE_RE.match(path.stem)
        if not m:
            continue
        num = m.group("num")
        if num in found:
            dupes.append(num)
        found[num] = (m.group("name"), path)
    assert not dupes, f"stage number(s) reused for different scripts: {sorted(set(dupes))}"
    return found


def _league() -> str:
    """League slug, derived from the numbered stage filenames themselves."""
    leagues = {
        m.group("lg")
        for path in (REPO / "python").glob("espn_*.py")
        if (m := _STAGE_RE.match(path.stem))
    }
    assert len(leagues) == 1, f"expected exactly one league prefix under python/, found {leagues}"
    return leagues.pop()


def test_layout_is_discoverable():
    """The engine self-configures; if discovery is wrong every result below is
    a vacuous pass rather than a real check."""
    assert _league()
    assert _stages(), "no numbered espn_<lg>_NN_<name>_scrape|creation.py files found"


def test_stage_numbers_are_unique():
    # Independent of the assert inside _stages() -- a refactor there shouldn't
    # be able to silently drop this guarantee.
    nums = [
        m.group("num")
        for p in (REPO / "python").glob("espn_*.py")
        if (m := _STAGE_RE.match(p.stem))
    ]
    dupes = sorted({n for n in nums if nums.count(n) > 1})
    assert not dupes, f"stage numbers reused: {dupes} -- numbers are dataset identities"


def test_stage_numbers_match_ecosystem_meaning():
    """A canonical dataset must sit at its shared NN, and a canonical NN must
    hold its shared dataset. A hole (the NN or the dataset is simply absent
    here) is always fine -- nothing requires every canonical stage to exist.
    """
    checked = 0
    wrong = []
    for num, (name, path) in _stages().items():
        if name in ALIAS_TO_NUM:
            checked += 1
            expected = ALIAS_TO_NUM[name]
            if expected != num:
                wrong.append(
                    f"{path.name}: builds {name!r}, which is stage {expected} elsewhere in the family"
                )
        elif num in CANONICAL:
            checked += 1
            if name not in CANONICAL[num]:
                wrong.append(
                    f"{path.name}: stage {num} means {sorted(CANONICAL[num])} elsewhere, this builds {name!r}"
                )
    assert checked >= 5, (
        f"only matched {checked} canonical stage(s) -- the naming pattern likely "
        "drifted and this gate is vacuously passing"
    )
    assert not wrong, (
        "stage numbering disagrees with the shared ecosystem convention:\n"
        + "\n".join(f"  {w}" for w in wrong)
    )
