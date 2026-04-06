#!/usr/bin/env python3
"""
Daily scheduler for Tesla Solar Download.

Runs run_daily.py on a configurable schedule (default: 23:30 daily).

Environment variables:
  SCHEDULE_HOUR   - Hour to run (0-23, default: 23)
  SCHEDULE_MINUTE - Minute to run (0-59, default: 30)
  RUN_ARGS        - Extra args to pass to run_daily.py (default: --influxdb-all-history)
"""
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

SCHEDULE_HOUR = int(os.environ.get('SCHEDULE_HOUR', '23'))
SCHEDULE_MINUTE = int(os.environ.get('SCHEDULE_MINUTE', '30'))
RUN_ARGS = os.environ.get('RUN_ARGS', '--influxdb-all-history').split()


def run_command():
    """Run the daily Tesla Solar download and publish."""
    cmd = ['python', '/app/run_daily.py'] + RUN_ARGS
    logger.info('Running: %s', ' '.join(cmd))
    result = subprocess.run(cmd)
    if result.returncode == 0:
        logger.info('Daily run completed successfully')
    else:
        logger.error('Daily run failed with exit code %d', result.returncode)


def seconds_until_next_run():
    """Calculate seconds until the next scheduled run."""
    now = datetime.now()
    next_run = now.replace(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(days=1)
    return (next_run - now).total_seconds()


def main():
    logger.info(
        'Tesla Solar scheduler started. Running daily at %02d:%02d.',
        SCHEDULE_HOUR, SCHEDULE_MINUTE,
    )

    while True:
        wait = seconds_until_next_run()
        next_run = datetime.now() + timedelta(seconds=wait)
        logger.info(
            'Next run at %s (in %.1f hours)',
            next_run.strftime('%Y-%m-%d %H:%M:%S'),
            wait / 3600,
        )
        time.sleep(wait)
        run_command()


if __name__ == '__main__':
    main()
