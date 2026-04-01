#!/bin/bash
set -e

case "$1" in
  auth)
    # Interactive OAuth authentication - run once to get Tesla token
    # Usage: docker compose run --rm -it tesla-solar auth
    exec python /app/tesla_solar_download.py --email "${TESLA_EMAIL:?TESLA_EMAIL must be set}"
    ;;
  run)
    # One-off manual run - passes remaining args to run_daily.py
    # Usage: docker compose run --rm tesla-solar run --publish-only
    shift
    exec python /app/run_daily.py "$@"
    ;;
  schedule|"")
    # Default: run on a daily schedule (see SCHEDULE_HOUR/SCHEDULE_MINUTE env vars)
    exec python /app/scheduler.py
    ;;
  *)
    exec "$@"
    ;;
esac
