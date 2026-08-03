#!/usr/bin/env bash
# Shared interpreter resolution. Sourced (not executed) by the scrape drivers.
#
# Sets SDV_PY to a python that carries sportsdataverse. Mirrors
# hoopR-nba-stats-raw/scripts/_venv.sh so every SDV scrape entry point resolves
# its interpreter one way.
#
# Resolution order:
#   1. $SDV_VENV_PYTHON            -- explicit override, always wins
#   2. this repo's .venv           -- the normal case
#   3. one-time `uv sync` bootstrap, then .venv again
#   4. ambient python3             -- last resort, loudly warned
#
# (3) is deliberately NOT the banned "uv run inside a scrape". The ban exists
# because `uv run` re-syncs the venv to the lockfile ON EVERY INVOCATION, which
# can swap sportsdataverse under a running multi-hour sweep. This runs once,
# before any scraping, and only when the venv is missing -- so a fresh host
# becomes self-sufficient instead of needing a manual step.
#
# (4) exists because a host without uv would otherwise be unrunnable. It is
# safe here ONLY because every driver runs an import preflight immediately
# after sourcing this file: a stale or wrong ambient env fails loudly there,
# which is the failure mode that cost wehoop-wnba-raw three weeks in 2026-07.
# Do not use this resolver without that preflight.

_sdv_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

_sdv_resolve() {
  if [ -n "${SDV_VENV_PYTHON:-}" ]; then
    SDV_PY="$SDV_VENV_PYTHON"
  elif [ -x "$_sdv_repo/.venv/Scripts/python.exe" ]; then
    SDV_PY="$_sdv_repo/.venv/Scripts/python.exe"      # Windows
  elif [ -x "$_sdv_repo/.venv/bin/python" ]; then
    SDV_PY="$_sdv_repo/.venv/bin/python"              # POSIX
  else
    SDV_PY=""
  fi
}

_sdv_resolve

if [ -z "$SDV_PY" ] && command -v uv >/dev/null 2>&1; then
  echo "No project venv found; bootstrapping once with 'uv sync' (pre-scrape)." >&2
  ( cd "$_sdv_repo" && uv sync --quiet ) || echo "WARN: uv sync failed" >&2
  _sdv_resolve
fi

if [ -z "$SDV_PY" ]; then
  # python3 on POSIX, python on Windows (Git Bash has no python3 shim).
  for _cand in python3 python; do
    if command -v "$_cand" >/dev/null 2>&1; then
      SDV_PY="$(command -v "$_cand")"
      break
    fi
  done
fi

if [ -n "$SDV_PY" ] && [ -z "${SDV_VENV_PYTHON:-}" ] && [ ! -d "$_sdv_repo/.venv" ]; then
  echo "WARN: no project venv and no uv; falling back to ambient $SDV_PY." >&2
  echo "      The import preflight below is what makes this safe -- if that" >&2
  echo "      fails, install uv and re-run:" >&2
  echo "        curl -LsSf https://astral.sh/uv/install.sh | sh && uv sync" >&2
fi

if [ -z "$SDV_PY" ] || [ ! -x "$SDV_PY" ]; then
  echo "FATAL: no usable python found." >&2
  echo "       Tried \$SDV_VENV_PYTHON, $_sdv_repo/.venv, 'uv sync', python3." >&2
  echo "       Fix: install uv and run 'uv sync' in $_sdv_repo," >&2
  echo "            or set SDV_VENV_PYTHON to an interpreter carrying" >&2
  echo "            sportsdataverse." >&2
  exit 2
fi

export SDV_PY
