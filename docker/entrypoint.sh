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
    # Default: run daily scheduler + live poller (15-min API polling)
    # The scheduler handles daily bulk CSV downloads.
    # The live poller handles near-real-time MQTT + InfluxDB publishing.
    echo "Starting daily scheduler in background..."
    python /app/scheduler.py &
    SCHEDULER_PID=$!

    echo "Starting live poller (interval=${POLL_INTERVAL_SECONDS:-900}s)..."
    python /app/live_poller.py &
    POLLER_PID=$!

    # Wait for either to exit; if one dies, kill the other
    trap "kill $SCHEDULER_PID $POLLER_PID 2>/dev/null; exit" SIGTERM SIGINT
    wait -n $SCHEDULER_PID $POLLER_PID
    EXIT_CODE=$?
    echo "Process exited with code $EXIT_CODE, shutting down..."
    kill $SCHEDULER_PID $POLLER_PID 2>/dev/null
    wait
    exit $EXIT_CODE
    ;;
  poller)
    # Run only the live poller (no daily scheduler)
    exec python /app/live_poller.py
    ;;
  *)
    exec "$@"
    ;;
esac
