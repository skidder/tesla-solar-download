#!/usr/bin/env python3
"""
One-time backfill of `tesla_solar_energy` measurement in InfluxDB.

Background
----------
Earlier versions of this project wrote `tesla_solar_energy` points from
`live_poller.py` every ~15 minutes with `_time = now()`. The Tesla
CALENDAR_HISTORY_DATA endpoint resets to 0 at local midnight, so the
resulting series contained ~96 in-progress points per day plus a long
stretch of 0s after midnight PT. Daily aggregations (`aggregateWindow`
with `fn: last`) were therefore unreliable.

The new ingest path (in `influxdb_publisher.py`) writes one point per
local calendar day, anchored at local midnight, tagged `day=YYYY-MM-DD`.
This script:

  1. Deletes the entire historical `tesla_solar_energy` measurement from
     the bucket (or a bounded date range, with --start / --end).
  2. Re-ingests from the existing per-month CSV files under
     `<DATA_DIR>/<site_id>/energy/*.csv` using the corrected publisher.

It is idempotent: re-running cleanly replaces the same per-day points.

Usage
-----
    # Dry run — show what would happen without touching InfluxDB
    DATA_DIR=/data python3 scripts/backfill_energy.py --dry-run

    # Full backfill
    DATA_DIR=/data python3 scripts/backfill_energy.py

    # Bounded re-ingest (re-pull yesterday + today after fixing a bad day)
    DATA_DIR=/data python3 scripts/backfill_energy.py \
        --start 2026-04-27 --end 2026-04-28

Environment variables (read via `config.Config`):
    INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET, DATA_DIR
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# Project root on path so we can import config and the publisher.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import Config  # noqa: E402
from influxdb_publisher import InfluxDBPublisher, get_all_csv_data, get_all_csv_files  # noqa: E402

LOCAL_TZ = ZoneInfo("America/Los_Angeles")
ENERGY_MEASUREMENT = "tesla_solar_energy"


def _data_dir() -> Path:
    """Match the convention used elsewhere in the project."""
    return Path(os.environ.get("DATA_DIR", "download"))


def _parse_day(s: str) -> datetime:
    """Parse YYYY-MM-DD as local midnight (Pacific)."""
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=LOCAL_TZ)


def discover_site_ids(data_dir: Path) -> list[str]:
    """Find site_id subdirectories under DATA_DIR."""
    if not data_dir.exists():
        return []
    return sorted(
        p.name
        for p in data_dir.iterdir()
        if p.is_dir() and (p / "energy").is_dir()
    )


def delete_range(
    publisher: InfluxDBPublisher,
    start: datetime,
    stop: datetime,
    site_id: str | None = None,
    dry_run: bool = False,
) -> bool:
    """
    Delete `tesla_solar_energy` points in the given UTC range from the bucket.

    Uses the InfluxDB v2 delete API. start/stop must be aware datetimes;
    they are converted to UTC RFC3339 before the call.
    """
    start_utc = start.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
    stop_utc = stop.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
    predicate = f'_measurement="{ENERGY_MEASUREMENT}"'
    if site_id:
        predicate += f' AND site_id="{site_id}"'

    if dry_run:
        logging.info(
            "[dry-run] DELETE bucket=%s start=%s stop=%s predicate=%s",
            publisher.config.INFLUXDB_BUCKET,
            start_utc,
            stop_utc,
            predicate,
        )
        return True

    try:
        from influxdb_client.client.delete_api import DeleteApi

        delete_api: DeleteApi = publisher.client.delete_api()
        delete_api.delete(
            start=start_utc,
            stop=stop_utc,
            predicate=predicate,
            bucket=publisher.config.INFLUXDB_BUCKET,
            org=publisher.config.INFLUXDB_ORG,
        )
        logging.info(
            "Deleted %s in [%s, %s) site=%s",
            ENERGY_MEASUREMENT,
            start_utc,
            stop_utc,
            site_id or "*",
        )
        return True
    except Exception as exc:
        logging.error("Delete failed: %s", exc)
        return False


def filter_records(records: list[dict], start: datetime | None, end: datetime | None) -> list[dict]:
    """Keep only records whose local-day falls within [start, end]."""
    if start is None and end is None:
        return records

    kept: list[dict] = []
    for r in records:
        ts = r.get("timestamp", "")
        if not ts:
            continue
        try:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=LOCAL_TZ)
        except ValueError:
            continue
        day = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        if start is not None and day < start:
            continue
        if end is not None and day > end:
            continue
        kept.append(r)
    return kept


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", help="Show planned actions without writing")
    p.add_argument("--start", type=_parse_day, default=None, help="Inclusive start day (YYYY-MM-DD, local time)")
    p.add_argument("--end", type=_parse_day, default=None, help="Inclusive end day (YYYY-MM-DD, local time)")
    p.add_argument("--site-id", default=None, help="Restrict to a single site_id (default: all)")
    p.add_argument("--skip-delete", action="store_true", help="Re-ingest only, do not delete first")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    Config.validate()

    data_dir = _data_dir()
    if not data_dir.exists():
        logging.error("DATA_DIR does not exist: %s", data_dir)
        return 2

    site_ids = [args.site_id] if args.site_id else discover_site_ids(data_dir)
    if not site_ids:
        logging.error("No site_id directories found under %s", data_dir)
        return 2
    logging.info("Sites: %s", site_ids)

    publisher = InfluxDBPublisher()
    if not publisher.connect():
        logging.error("Failed to connect to InfluxDB")
        return 3

    try:
        # Step 1: delete the existing range (or all-time if unbounded).
        if not args.skip_delete:
            # Influx delete API requires a bounded range. Use 1970..2100
            # for "all time" if no start/end given.
            del_start = args.start or datetime(1970, 1, 1, tzinfo=LOCAL_TZ)
            # End is inclusive in CLI; delete API stop is exclusive. Add 1 day.
            del_stop = (args.end or datetime(2100, 1, 1, tzinfo=LOCAL_TZ)) + timedelta(days=1)
            for sid in site_ids:
                if not delete_range(publisher, del_start, del_stop, sid, args.dry_run):
                    return 4

        # Step 2: re-ingest from CSVs.
        total_written = 0
        for sid in site_ids:
            energy_dir = data_dir / sid / "energy"
            csv_files = get_all_csv_files(energy_dir)
            if not csv_files:
                logging.warning("No energy CSVs for site %s", sid)
                continue

            for csv_file in sorted(csv_files):
                rows = get_all_csv_data(csv_file)
                rows = filter_records(rows, args.start, args.end)
                if not rows:
                    logging.debug("No matching rows in %s", csv_file.name)
                    continue

                if args.dry_run:
                    days = sorted({r.get("timestamp", "")[:10] for r in rows if r.get("timestamp")})
                    logging.info(
                        "[dry-run] would write %d row(s) from %s covering days: %s%s",
                        len(rows),
                        csv_file.name,
                        ", ".join(days[:5]),
                        " ..." if len(days) > 5 else "",
                    )
                    continue

                written = publisher.write_energy_batch(sid, rows)
                total_written += written
                logging.info("site=%s file=%s rows=%d written=%d", sid, csv_file.name, len(rows), written)

        if args.dry_run:
            logging.info("[dry-run] complete — no writes performed")
        else:
            logging.info("Backfill complete: %d points written", total_written)
        return 0
    finally:
        try:
            publisher.client.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
