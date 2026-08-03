#!/usr/bin/env bash
# Shared interpreter resolution. Sourced (not executed) by the scrape drivers.
#
# Sets SDV_PY to the python that carries sportsdataverse. Mirrors
# hoopR-nba-stats-raw/scripts/_venv.sh so all SDV scrape entry points resolve
# their interpreter one way.
#
# Resolution order: $SDV_VENV_PYTHON -> this repo's .venv (Windows, then POSIX).
#
# Deliberately NOT `uv run`: that resyncs the venv to the lockfile mid-sweep,
# which can swap sportsdataverse under a running multi-hour scrape. It also
# makes uv a RUNTIME dependency of every scrape, which is exactly what left the
# droplet unable to run these drivers. Build the venv with uv ahead of time
# (`uv sync`); the scrape itself only needs the interpreter.

_sdv_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -n "${SDV_VENV_PYTHON:-}" ]; then
  SDV_PY="$SDV_VENV_PYTHON"
elif [ -x "$_sdv_repo/.venv/Scripts/python.exe" ]; then
  SDV_PY="$_sdv_repo/.venv/Scripts/python.exe"      # Windows
else
  SDV_PY="$_sdv_repo/.venv/bin/python"              # POSIX
fi

if [ ! -x "$SDV_PY" ]; then
  echo "FATAL: venv python not found at $SDV_PY" >&2
  echo "       run 'uv sync' in $_sdv_repo, or set SDV_VENV_PYTHON to an" >&2
  echo "       interpreter that already carries sportsdataverse." >&2
  exit 2
fi

export SDV_PY
