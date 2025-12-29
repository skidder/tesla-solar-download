"""
InfluxDB Publisher for Tesla Solar Data.

Publishes solar data to InfluxDB for historical storage and visualization.
InfluxDB properly handles timestamps, making it ideal for backfilling historical data.

Home Assistant can then read from InfluxDB via:
- The InfluxDB integration (for sensors)
- Grafana dashboards embedded in HA
"""

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from config import Config

logger = logging.getLogger(__name__)


class InfluxDBPublisher:
    """Publishes Tesla Solar data to InfluxDB."""

    def __init__(self, config: type[Config] = Config):
        self.config = config
        self.client: Optional[InfluxDBClient] = None
        self.write_api = None

    def connect(self) -> bool:
        """Connect to InfluxDB."""
        if not self.config.INFLUXDB_ENABLED:
            logger.info("InfluxDB is disabled")
            return False

        try:
            self.client = InfluxDBClient(
                url=self.config.INFLUXDB_URL,
                token=self.config.INFLUXDB_TOKEN,
                org=self.config.INFLUXDB_ORG,
            )

            # Test connection
            health = self.client.health()
            if health.status != "pass":
                logger.error(f"InfluxDB health check failed: {health.message}")
                return False

            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            logger.info(f"Connected to InfluxDB at {self.config.INFLUXDB_URL}")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")
            return False

    def disconnect(self):
        """Disconnect from InfluxDB."""
        if self.client:
            self.client.close()
            self.client = None
            self.write_api = None

    def write_power_point(self, site_id: str, timestamp: str, data: dict):
        """Write a power data point to InfluxDB."""
        if not self.write_api:
            return

        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

            point = (
                Point("tesla_solar_power")
                .tag("site_id", site_id)
                .time(dt, WritePrecision.S)
            )

            for field in ["solar_power", "battery_power", "grid_power", "load_power"]:
                if field in data:
                    try:
                        point = point.field(field, float(data[field]))
                    except (ValueError, TypeError):
                        pass

            self.write_api.write(
                bucket=self.config.INFLUXDB_BUCKET,
                org=self.config.INFLUXDB_ORG,
                record=point,
            )

        except Exception as e:
            logger.error(f"Failed to write power point: {e}")

    def write_soe_point(self, site_id: str, timestamp: str, data: dict):
        """Write a battery SOE data point to InfluxDB."""
        if not self.write_api:
            return

        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

            point = (
                Point("tesla_solar_soe")
                .tag("site_id", site_id)
                .time(dt, WritePrecision.S)
            )

            if "soe" in data:
                try:
                    point = point.field("soe", float(data["soe"]))
                except (ValueError, TypeError):
                    pass

            self.write_api.write(
                bucket=self.config.INFLUXDB_BUCKET,
                org=self.config.INFLUXDB_ORG,
                record=point,
            )

        except Exception as e:
            logger.error(f"Failed to write SOE point: {e}")

    def write_energy_point(self, site_id: str, timestamp: str, data: dict):
        """Write an energy data point to InfluxDB."""
        if not self.write_api:
            return

        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

            point = (
                Point("tesla_solar_energy")
                .tag("site_id", site_id)
                .time(dt, WritePrecision.S)
            )

            energy_fields = [
                "solar_energy_exported",
                "grid_energy_imported",
                "grid_energy_exported_from_solar",
                "grid_energy_exported_from_battery",
                "battery_energy_exported",
                "battery_energy_imported_from_grid",
                "battery_energy_imported_from_solar",
                "consumer_energy_imported_from_grid",
                "consumer_energy_imported_from_solar",
                "consumer_energy_imported_from_battery",
            ]

            for field in energy_fields:
                if field in data:
                    try:
                        point = point.field(field, float(data[field]))
                    except (ValueError, TypeError):
                        pass

            self.write_api.write(
                bucket=self.config.INFLUXDB_BUCKET,
                org=self.config.INFLUXDB_ORG,
                record=point,
            )

        except Exception as e:
            logger.error(f"Failed to write energy point: {e}")

    def write_power_batch(self, site_id: str, records: list[dict]):
        """Write a batch of power records to InfluxDB."""
        if not self.write_api:
            return

        points = []
        for record in records:
            timestamp = record.get("timestamp", "")
            if not timestamp:
                continue

            try:
                dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

                point = (
                    Point("tesla_solar_power")
                    .tag("site_id", site_id)
                    .time(dt, WritePrecision.S)
                )

                for field in ["solar_power", "battery_power", "grid_power", "load_power"]:
                    if field in record:
                        try:
                            point = point.field(field, float(record[field]))
                        except (ValueError, TypeError):
                            pass

                points.append(point)

            except Exception:
                pass

        if points:
            self.write_api.write(
                bucket=self.config.INFLUXDB_BUCKET,
                org=self.config.INFLUXDB_ORG,
                record=points,
            )

    def write_soe_batch(self, site_id: str, records: list[dict]):
        """Write a batch of SOE records to InfluxDB."""
        if not self.write_api:
            return

        points = []
        for record in records:
            timestamp = record.get("timestamp", "")
            if not timestamp:
                continue

            try:
                dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

                point = (
                    Point("tesla_solar_soe")
                    .tag("site_id", site_id)
                    .time(dt, WritePrecision.S)
                )

                if "soe" in record:
                    try:
                        point = point.field("soe", float(record["soe"]))
                    except (ValueError, TypeError):
                        pass

                points.append(point)

            except Exception:
                pass

        if points:
            self.write_api.write(
                bucket=self.config.INFLUXDB_BUCKET,
                org=self.config.INFLUXDB_ORG,
                record=points,
            )

    def write_energy_batch(self, site_id: str, records: list[dict]):
        """Write a batch of energy records to InfluxDB."""
        if not self.write_api:
            return

        energy_fields = [
            "solar_energy_exported",
            "grid_energy_imported",
            "grid_energy_exported_from_solar",
            "grid_energy_exported_from_battery",
            "battery_energy_exported",
            "battery_energy_imported_from_grid",
            "battery_energy_imported_from_solar",
            "consumer_energy_imported_from_grid",
            "consumer_energy_imported_from_solar",
            "consumer_energy_imported_from_battery",
        ]

        points = []
        for record in records:
            timestamp = record.get("timestamp", "")
            if not timestamp:
                continue

            try:
                dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

                point = (
                    Point("tesla_solar_energy")
                    .tag("site_id", site_id)
                    .time(dt, WritePrecision.S)
                )

                for field in energy_fields:
                    if field in record:
                        try:
                            point = point.field(field, float(record[field]))
                        except (ValueError, TypeError):
                            pass

                points.append(point)

            except Exception:
                pass

        if points:
            self.write_api.write(
                bucket=self.config.INFLUXDB_BUCKET,
                org=self.config.INFLUXDB_ORG,
                record=points,
            )


def get_all_csv_data(csv_path: Path) -> list[dict]:
    """Read all rows from a CSV file."""
    if not csv_path.exists():
        return []

    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception as e:
        logger.error(f"Error reading {csv_path}: {e}")

    return []


def get_all_csv_files(directory: Path) -> list[Path]:
    """Get all CSV files in a directory, sorted by name (oldest first)."""
    if not directory.exists():
        return []
    return sorted([f for f in directory.glob("*.csv") if ".partial" not in f.name])


def find_site_ids(data_dir: Path) -> list[str]:
    """Find all site IDs from the download directory structure."""
    site_ids = []
    if data_dir.exists():
        for item in data_dir.iterdir():
            if item.is_dir() and item.name.isdigit():
                site_ids.append(item.name)
    return site_ids


def publish_to_influxdb(
    publisher: InfluxDBPublisher,
    data_dir: Path,
    batch_size: int = 1000,
):
    """
    Publish all historical data to InfluxDB.

    Args:
        publisher: The InfluxDB publisher instance
        data_dir: Path to the data directory
        batch_size: Number of records per batch write
    """
    site_ids = find_site_ids(data_dir)

    if not site_ids:
        logger.warning(f"No site data found in {data_dir}")
        return

    for site_id in site_ids:
        logger.info(f"Publishing data for site ***{site_id[-4:]} to InfluxDB")

        # Publish power data
        power_dir = data_dir / site_id / "power"
        power_files = get_all_csv_files(power_dir)
        if power_files:
            total_records = sum(len(get_all_csv_data(f)) for f in power_files)
            logger.info(f"  Writing {total_records} power records from {len(power_files)} files...")

            written = 0
            batch = []
            for csv_file in power_files:
                records = get_all_csv_data(csv_file)
                for record in records:
                    batch.append(record)
                    if len(batch) >= batch_size:
                        publisher.write_power_batch(site_id, batch)
                        written += len(batch)
                        batch = []
                        if written % 5000 == 0:
                            logger.info(f"    Progress: {written}/{total_records} power records")

            if batch:
                publisher.write_power_batch(site_id, batch)
                written += len(batch)

            logger.info(f"  Wrote {written} power records")

        # Publish SOE data
        soe_dir = data_dir / site_id / "soe"
        soe_files = get_all_csv_files(soe_dir)
        if soe_files:
            total_records = sum(len(get_all_csv_data(f)) for f in soe_files)
            logger.info(f"  Writing {total_records} SOE records from {len(soe_files)} files...")

            written = 0
            batch = []
            for csv_file in soe_files:
                records = get_all_csv_data(csv_file)
                for record in records:
                    batch.append(record)
                    if len(batch) >= batch_size:
                        publisher.write_soe_batch(site_id, batch)
                        written += len(batch)
                        batch = []

            if batch:
                publisher.write_soe_batch(site_id, batch)
                written += len(batch)

            logger.info(f"  Wrote {written} SOE records")

        # Publish energy data
        energy_dir = data_dir / site_id / "energy"
        energy_files = get_all_csv_files(energy_dir)
        if energy_files:
            total_records = sum(len(get_all_csv_data(f)) for f in energy_files)
            logger.info(f"  Writing {total_records} energy records from {len(energy_files)} files...")

            written = 0
            batch = []
            for csv_file in energy_files:
                records = get_all_csv_data(csv_file)
                for record in records:
                    batch.append(record)
                    if len(batch) >= batch_size:
                        publisher.write_energy_batch(site_id, batch)
                        written += len(batch)
                        batch = []

            if batch:
                publisher.write_energy_batch(site_id, batch)
                written += len(batch)

            logger.info(f"  Wrote {written} energy records")


def publish_daily_to_influxdb(publisher: InfluxDBPublisher, data_dir: Path):
    """
    Publish today's data to InfluxDB.
    
    This writes ALL records from the most recent CSV files (today's data),
    not just the last data point. InfluxDB handles duplicates gracefully
    (same timestamp = overwrite), so this is safe to run multiple times.
    """
    site_ids = find_site_ids(data_dir)

    if not site_ids:
        logger.warning(f"No site data found in {data_dir}")
        return

    for site_id in site_ids:
        logger.info(f"Publishing daily data for site ***{site_id[-4:]} to InfluxDB")

        # Get the most recent power file (today's data) and write ALL records
        power_dir = data_dir / site_id / "power"
        power_files = sorted(power_dir.glob("*.csv"), reverse=True) if power_dir.exists() else []
        if power_files:
            # Write all records from the most recent file (today)
            records = get_all_csv_data(power_files[0])
            if records:
                publisher.write_power_batch(site_id, records)
                logger.info(f"  Wrote {len(records)} power records from {power_files[0].name}")

        # Get the most recent SOE file and write ALL records
        soe_dir = data_dir / site_id / "soe"
        soe_files = sorted(soe_dir.glob("*.csv"), reverse=True) if soe_dir.exists() else []
        if soe_files:
            records = get_all_csv_data(soe_files[0])
            if records:
                publisher.write_soe_batch(site_id, records)
                logger.info(f"  Wrote {len(records)} SOE records from {soe_files[0].name}")

        # Get the most recent energy file and write ALL records
        energy_dir = data_dir / site_id / "energy"
        energy_files = sorted(energy_dir.glob("*.csv"), reverse=True) if energy_dir.exists() else []
        if energy_files:
            records = get_all_csv_data(energy_files[0])
            if records:
                publisher.write_energy_batch(site_id, records)
                logger.info(f"  Wrote {len(records)} energy records from {energy_files[0].name}")


def main():
    """Main entry point for standalone InfluxDB publishing."""
    import argparse

    parser = argparse.ArgumentParser(description="Publish Tesla Solar data to InfluxDB")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--all-history",
        action="store_true",
        help="Publish all historical data (default: latest only)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Records per batch write (default: 1000)",
    )
    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.debug else getattr(logging, Config.LOG_LEVEL)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Check if InfluxDB is configured
    if not Config.INFLUXDB_ENABLED:
        logger.error("InfluxDB is not enabled. Set INFLUXDB_ENABLED=true in your .env")
        return 1

    # Connect and publish
    publisher = InfluxDBPublisher()
    if not publisher.connect():
        logger.error("Failed to connect to InfluxDB")
        return 1

    try:
        data_dir = Path(Config.DATA_DIR)

        if args.all_history:
            logger.info("Publishing ALL historical data to InfluxDB...")
            publish_to_influxdb(publisher, data_dir, batch_size=args.batch_size)
        else:
            logger.info("Publishing today's data to InfluxDB...")
            publish_daily_to_influxdb(publisher, data_dir)

        logger.info("Successfully published data to InfluxDB")
    finally:
        publisher.disconnect()

    return 0


if __name__ == "__main__":
    exit(main())





