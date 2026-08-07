"""Every numbered scraper must import and answer --help.

Ported from the wehoop-wbb-raw template suite, where this guard caught two
real defects: a script importing a module that existed nowhere, and a
pyproject rewrite that silently dropped runtime deps. Both are import-time
failures — exactly what a daily cron discovers at 5am and a test discovers
in one second.
"""

from __future__ import annotations

import ast
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

PY_DIR = Path(__file__).resolve().parents[1] / "python"
SCRIPTS = sorted(PY_DIR.glob("*.py"))

# The ecosystem-wide ESPN-raw stage canon (same number = same dataset in every
# -raw repo: nba/mbb/wnba/wbb): 01 schedules, 02 pbp, 03 standings,
# 04 game_rosters, 05 draft, 06 player_stats, 07 team_stats, 08 team_rosters,
# 09 player_core, 10+ league extras, 99 master. This repo carries 01-09 with no
# holes; `espn_nba_00_all_scrape.py` / `_99_schedule_master_creation.py` do not
# exist here yet — add them to this list when they land.
EXPECTED = [
    "espn_nba_01_schedules_scrape.py",
    "espn_nba_02_pbp_scrape.py",
    "espn_nba_03_standings_scrape.py",
    "espn_nba_04_game_rosters_scrape.py",
    "espn_nba_05_draft_scrape.py",
    "espn_nba_06_player_stats_scrape.py",
    "espn_nba_07_team_stats_scrape.py",
    "espn_nba_08_team_rosters_scrape.py",
    "espn_nba_09_player_core_scrape.py",
]


def test_the_numbered_scripts_are_exactly_what_we_expect():
    """Numbers are run order. A gap or a stray file means the pipeline and the
    directory listing have diverged."""
    assert [p.name for p in SCRIPTS] == EXPECTED


@pytest.mark.parametrize("path", SCRIPTS, ids=lambda p: p.name)
def test_script_imports(path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[path.stem] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(path.stem, None)


@pytest.mark.parametrize("path", SCRIPTS, ids=lambda p: p.name)
def test_script_help_exits_zero(path):
    proc = subprocess.run(
        [sys.executable, str(path), "--help"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
    assert "--start_year" in proc.stdout


@pytest.mark.parametrize("path", SCRIPTS, ids=lambda p: p.name)
def test_no_script_uses_type_bool(path):
    """``argparse(type=bool)`` is the defect that made every daily run
    re-download the whole archive: bash passes "false" and bool("false") is
    True. Nothing may reintroduce it.

    Checked against the AST, not the text -- a comment explaining the
    antipattern is fine, an ``add_argument(type=bool)`` call is not.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    offenders = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for kw in node.keywords
        if kw.arg == "type" and isinstance(kw.value, ast.Name) and kw.value.id == "bool"
    ]
    assert offenders == [], (
        f"{path.name} passes type=bool at line(s) {offenders}; use sportsdataverse.scrape.espn.cli.str2bool"
    )
