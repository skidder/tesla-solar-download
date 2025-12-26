"""
MQTT Publisher for Tesla Solar Data.

Publishes solar data to an MQTT broker for Home Assistant integration.
Supports Home Assistant MQTT Discovery for automatic sensor configuration.
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import paho.mqtt.client as mqtt

from config import Config

logger = logging.getLogger(__name__)


# Sensor definitions for Home Assistant MQTT Discovery
POWER_SENSORS = {
    "solar_power": {
        "name": "Solar Power",
        "unit": "W",
        "device_class": "power",
        "state_class": "measurement",
        "icon": "mdi:solar-power",
    },
    "battery_power": {
        "name": "Battery Power",
        "unit": "W",
        "device_class": "power",
        "state_class": "measurement",
        "icon": "mdi:battery",
    },
    "grid_power": {
        "name": "Grid Power",
        "unit": "W",
        "device_class": "power",
        "state_class": "measurement",
        "icon": "mdi:transmission-tower",
    },
    "load_power": {
        "name": "Home Load",
        "unit": "W",
        "device_class": "power",
        "state_class": "measurement",
        "icon": "mdi:home-lightning-bolt",
    },
}

SOE_SENSORS = {
    "soe": {
        "name": "Battery State of Charge",
        "unit": "%",
        "device_class": "battery",
        "state_class": "measurement",
        "icon": "mdi:battery",
    },
}

ENERGY_SENSORS = {
    "solar_energy_exported": {
        "name": "Solar Energy Today",
        "unit": "Wh",
        "device_class": "energy",
        "state_class": "total_increasing",
        "icon": "mdi:solar-power",
    },
    "grid_energy_imported": {
        "name": "Grid Energy Imported Today",
        "unit": "Wh",
        "device_class": "energy",
        "state_class": "total_increasing",
        "icon": "mdi:transmission-tower-import",
    },
    "grid_energy_exported_from_solar": {
        "name": "Solar Energy Exported to Grid",
        "unit": "Wh",
        "device_class": "energy",
        "state_class": "total_increasing",
        "icon": "mdi:transmission-tower-export",
    },
    "battery_energy_exported": {
        "name": "Battery Energy Discharged Today",
        "unit": "Wh",
        "device_class": "energy",
        "state_class": "total_increasing",
        "icon": "mdi:battery-minus",
    },
    "battery_energy_imported_from_solar": {
        "name": "Battery Energy Charged Today",
        "unit": "Wh",
        "device_class": "energy",
        "state_class": "total_increasing",
        "icon": "mdi:battery-plus",
    },
    "consumer_energy_imported_from_solar": {
        "name": "Home Solar Consumption Today",
        "unit": "Wh",
        "device_class": "energy",
        "state_class": "total_increasing",
        "icon": "mdi:home-lightning-bolt",
    },
}


class MQTTPublisher:
    """Publishes Tesla Solar data to MQTT broker."""

    def __init__(self, config: type[Config] = Config):
        self.config = config
        self.client: Optional[mqtt.Client] = None
        self._connected = False

    def connect(self) -> bool:
        """Connect to the MQTT broker."""
        if not self.config.MQTT_ENABLED:
            logger.info("MQTT is disabled")
            return False

        try:
            self.client = mqtt.Client(
                client_id=self.config.MQTT_CLIENT_ID,
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            )

            if self.config.MQTT_USERNAME:
                self.client.username_pw_set(
                    self.config.MQTT_USERNAME,
                    self.config.MQTT_PASSWORD,
                )

            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect

            logger.info(f"Connecting to MQTT broker at {self.config.MQTT_HOST}:{self.config.MQTT_PORT}")
            self.client.connect(self.config.MQTT_HOST, self.config.MQTT_PORT, keepalive=60)
            self.client.loop_start()

            # Wait briefly for connection
            import time
            timeout = 10
            while not self._connected and timeout > 0:
                time.sleep(0.5)
                timeout -= 0.5

            if not self._connected:
                logger.error("Failed to connect to MQTT broker within timeout")
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            return False

    def disconnect(self):
        """Disconnect from the MQTT broker."""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            self._connected = False

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        """Callback when connected to MQTT broker."""
        if reason_code == 0:
            logger.info("Connected to MQTT broker")
            self._connected = True
        else:
            logger.error(f"Failed to connect to MQTT broker: {reason_code}")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        """Callback when disconnected from MQTT broker."""
        logger.info(f"Disconnected from MQTT broker: {reason_code}")
        self._connected = False

    def _publish(self, topic: str, payload: Any, retain: bool = True):
        """Publish a message to MQTT."""
        if not self.client or not self._connected:
            logger.warning("Not connected to MQTT broker")
            return False

        if isinstance(payload, dict):
            payload = json.dumps(payload)

        result = self.client.publish(topic, payload, retain=retain)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.error(f"Failed to publish to {topic}: {result.rc}")
            return False

        logger.debug(f"Published to {topic}: {payload}")
        return True

    def publish_ha_discovery(self, site_id: str):
        """Publish Home Assistant MQTT Discovery configuration."""
        if not self.config.HA_DISCOVERY_ENABLED:
            logger.info("Home Assistant discovery is disabled")
            return

        device_info = {
            "identifiers": [f"tesla_solar_{site_id}"],
            "name": f"Tesla Solar {site_id[-4:]}",
            "manufacturer": "Tesla",
            "model": "Solar/Powerwall",
        }

        all_sensors = {**POWER_SENSORS, **SOE_SENSORS, **ENERGY_SENSORS}

        for sensor_id, sensor_config in all_sensors.items():
            discovery_topic = (
                f"{self.config.MQTT_DISCOVERY_PREFIX}/sensor/"
                f"tesla_solar_{site_id}/{sensor_id}/config"
            )

            state_topic = f"{self.config.MQTT_TOPIC_PREFIX}/{site_id}/{sensor_id}"

            discovery_payload = {
                "name": sensor_config["name"],
                "unique_id": f"tesla_solar_{site_id}_{sensor_id}",
                "state_topic": state_topic,
                "unit_of_measurement": sensor_config["unit"],
                "device_class": sensor_config["device_class"],
                "state_class": sensor_config["state_class"],
                "icon": sensor_config["icon"],
                "device": device_info,
            }

            self._publish(discovery_topic, discovery_payload, retain=True)

        # Also publish availability topic
        availability_topic = f"{self.config.MQTT_TOPIC_PREFIX}/{site_id}/availability"
        self._publish(availability_topic, "online", retain=True)

        logger.info(f"Published Home Assistant discovery config for site {site_id[-4:]}")

    def publish_power_data(self, site_id: str, data: dict):
        """Publish power data (solar, battery, grid, load)."""
        for sensor_id in POWER_SENSORS:
            if sensor_id in data:
                topic = f"{self.config.MQTT_TOPIC_PREFIX}/{site_id}/{sensor_id}"
                value = round(float(data[sensor_id]), 2)
                self._publish(topic, str(value), retain=True)

        # Also publish timestamp
        if "timestamp" in data:
            topic = f"{self.config.MQTT_TOPIC_PREFIX}/{site_id}/power_timestamp"
            self._publish(topic, data["timestamp"], retain=True)

    def publish_soe_data(self, site_id: str, data: dict):
        """Publish battery state of charge data."""
        if "soe" in data:
            topic = f"{self.config.MQTT_TOPIC_PREFIX}/{site_id}/soe"
            value = round(float(data["soe"]), 1)
            self._publish(topic, str(value), retain=True)

        if "timestamp" in data:
            topic = f"{self.config.MQTT_TOPIC_PREFIX}/{site_id}/soe_timestamp"
            self._publish(topic, data["timestamp"], retain=True)

    def publish_energy_data(self, site_id: str, data: dict):
        """Publish energy data (daily totals)."""
        for sensor_id in ENERGY_SENSORS:
            if sensor_id in data:
                topic = f"{self.config.MQTT_TOPIC_PREFIX}/{site_id}/{sensor_id}"
                value = round(float(data[sensor_id]), 2)
                self._publish(topic, str(value), retain=True)

        if "timestamp" in data:
            topic = f"{self.config.MQTT_TOPIC_PREFIX}/{site_id}/energy_timestamp"
            self._publish(topic, data["timestamp"], retain=True)


def get_latest_csv_data(csv_path: Path) -> Optional[dict]:
    """Read the latest row from a CSV file."""
    if not csv_path.exists():
        return None

    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            if rows:
                return rows[-1]  # Return the last row
    except Exception as e:
        logger.error(f"Error reading {csv_path}: {e}")

    return None


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
    # Sort by filename to get chronological order, exclude partial files for history
    return sorted([f for f in directory.glob("*.csv") if ".partial" not in f.name])


def find_site_ids(data_dir: Path) -> list[str]:
    """Find all site IDs from the download directory structure."""
    site_ids = []
    if data_dir.exists():
        for item in data_dir.iterdir():
            if item.is_dir() and item.name.isdigit():
                site_ids.append(item.name)
    return site_ids


def get_latest_power_file(data_dir: Path, site_id: str) -> Optional[Path]:
    """Get the most recent power CSV file for a site."""
    power_dir = data_dir / site_id / "power"
    if not power_dir.exists():
        return None

    # Get all CSV files (including partial)
    csv_files = sorted(power_dir.glob("*.csv"), reverse=True)
    if csv_files:
        return csv_files[0]
    return None


def get_latest_soe_file(data_dir: Path, site_id: str) -> Optional[Path]:
    """Get the most recent SOE CSV file for a site."""
    soe_dir = data_dir / site_id / "soe"
    if not soe_dir.exists():
        return None

    csv_files = sorted(soe_dir.glob("*.csv"), reverse=True)
    if csv_files:
        return csv_files[0]
    return None


def get_latest_energy_file(data_dir: Path, site_id: str) -> Optional[Path]:
    """Get the most recent energy CSV file for a site."""
    energy_dir = data_dir / site_id / "energy"
    if not energy_dir.exists():
        return None

    csv_files = sorted(energy_dir.glob("*.csv"), reverse=True)
    if csv_files:
        return csv_files[0]
    return None


def publish_all_data(publisher: MQTTPublisher, data_dir: Path):
    """Publish latest data for all sites to MQTT."""
    site_ids = find_site_ids(data_dir)

    if not site_ids:
        logger.warning(f"No site data found in {data_dir}")
        return

    for site_id in site_ids:
        logger.info(f"Publishing data for site ***{site_id[-4:]}")

        # Publish HA discovery config
        publisher.publish_ha_discovery(site_id)

        # Publish power data
        power_file = get_latest_power_file(data_dir, site_id)
        if power_file:
            power_data = get_latest_csv_data(power_file)
            if power_data:
                publisher.publish_power_data(site_id, power_data)
                logger.info(f"  Published power data from {power_file.name}")

        # Publish SOE data
        soe_file = get_latest_soe_file(data_dir, site_id)
        if soe_file:
            soe_data = get_latest_csv_data(soe_file)
            if soe_data:
                publisher.publish_soe_data(site_id, soe_data)
                logger.info(f"  Published SOE data from {soe_file.name}")

        # Publish energy data (today's totals from the latest day in the current month)
        energy_file = get_latest_energy_file(data_dir, site_id)
        if energy_file:
            energy_data = get_latest_csv_data(energy_file)
            if energy_data:
                publisher.publish_energy_data(site_id, energy_data)
                logger.info(f"  Published energy data from {energy_file.name}")


def publish_historical_data(
    publisher: MQTTPublisher,
    data_dir: Path,
    batch_size: int = 100,
    delay_between_batches: float = 0.1,
):
    """
    Publish all historical data for all sites to MQTT.

    This publishes each data point with its timestamp embedded in a JSON payload.
    Data is published to history-specific topics that Home Assistant can subscribe to
    for importing into long-term statistics.

    Args:
        publisher: The MQTT publisher instance
        data_dir: Path to the data directory
        batch_size: Number of records to publish before a small delay
        delay_between_batches: Seconds to wait between batches (to avoid overwhelming broker)
    """
    import time

    site_ids = find_site_ids(data_dir)

    if not site_ids:
        logger.warning(f"No site data found in {data_dir}")
        return

    for site_id in site_ids:
        logger.info(f"Publishing historical data for site ***{site_id[-4:]}")

        # Publish HA discovery config first
        publisher.publish_ha_discovery(site_id)

        # Publish historical power data
        power_dir = data_dir / site_id / "power"
        power_files = get_all_csv_files(power_dir)
        if power_files:
            total_records = 0
            for csv_file in power_files:
                records = get_all_csv_data(csv_file)
                total_records += len(records)

            logger.info(f"  Publishing {total_records} power records from {len(power_files)} files...")
            published = 0
            for csv_file in power_files:
                records = get_all_csv_data(csv_file)
                for i, record in enumerate(records):
                    _publish_historical_power_record(publisher, site_id, record)
                    published += 1

                    if published % batch_size == 0:
                        time.sleep(delay_between_batches)
                        if published % 1000 == 0:
                            logger.info(f"    Progress: {published}/{total_records} power records")

            logger.info(f"  Published {published} power records")

        # Publish historical SOE data
        soe_dir = data_dir / site_id / "soe"
        soe_files = get_all_csv_files(soe_dir)
        if soe_files:
            total_records = 0
            for csv_file in soe_files:
                records = get_all_csv_data(csv_file)
                total_records += len(records)

            logger.info(f"  Publishing {total_records} SOE records from {len(soe_files)} files...")
            published = 0
            for csv_file in soe_files:
                records = get_all_csv_data(csv_file)
                for record in records:
                    _publish_historical_soe_record(publisher, site_id, record)
                    published += 1

                    if published % batch_size == 0:
                        time.sleep(delay_between_batches)
                        if published % 1000 == 0:
                            logger.info(f"    Progress: {published}/{total_records} SOE records")

            logger.info(f"  Published {published} SOE records")

        # Publish historical energy data
        energy_dir = data_dir / site_id / "energy"
        energy_files = get_all_csv_files(energy_dir)
        if energy_files:
            total_records = 0
            for csv_file in energy_files:
                records = get_all_csv_data(csv_file)
                total_records += len(records)

            logger.info(f"  Publishing {total_records} energy records from {len(energy_files)} files...")
            published = 0
            for csv_file in energy_files:
                records = get_all_csv_data(csv_file)
                for record in records:
                    _publish_historical_energy_record(publisher, site_id, record)
                    published += 1

                    if published % batch_size == 0:
                        time.sleep(delay_between_batches)

            logger.info(f"  Published {published} energy records")

        # Publish the latest values to the main topics (for current state)
        logger.info("  Publishing latest values to main topics...")
        power_file = get_latest_power_file(data_dir, site_id)
        if power_file:
            power_data = get_latest_csv_data(power_file)
            if power_data:
                publisher.publish_power_data(site_id, power_data)

        soe_file = get_latest_soe_file(data_dir, site_id)
        if soe_file:
            soe_data = get_latest_csv_data(soe_file)
            if soe_data:
                publisher.publish_soe_data(site_id, soe_data)

        energy_file = get_latest_energy_file(data_dir, site_id)
        if energy_file:
            energy_data = get_latest_csv_data(energy_file)
            if energy_data:
                publisher.publish_energy_data(site_id, energy_data)


def _publish_historical_power_record(publisher: MQTTPublisher, site_id: str, record: dict):
    """Publish a single historical power record as JSON with timestamp."""
    timestamp = record.get("timestamp", "")
    topic = f"{publisher.config.MQTT_TOPIC_PREFIX}/{site_id}/history/power"

    payload = {"timestamp": timestamp}
    for sensor_id in POWER_SENSORS:
        if sensor_id in record:
            try:
                payload[sensor_id] = round(float(record[sensor_id]), 2)
            except (ValueError, TypeError):
                pass

    publisher._publish(topic, payload, retain=False)


def _publish_historical_soe_record(publisher: MQTTPublisher, site_id: str, record: dict):
    """Publish a single historical SOE record as JSON with timestamp."""
    timestamp = record.get("timestamp", "")
    topic = f"{publisher.config.MQTT_TOPIC_PREFIX}/{site_id}/history/soe"

    payload = {"timestamp": timestamp}
    if "soe" in record:
        try:
            payload["soe"] = round(float(record["soe"]), 1)
        except (ValueError, TypeError):
            pass

    publisher._publish(topic, payload, retain=False)


def _publish_historical_energy_record(publisher: MQTTPublisher, site_id: str, record: dict):
    """Publish a single historical energy record as JSON with timestamp."""
    timestamp = record.get("timestamp", "")
    topic = f"{publisher.config.MQTT_TOPIC_PREFIX}/{site_id}/history/energy"

    payload = {"timestamp": timestamp}
    for sensor_id in ENERGY_SENSORS:
        if sensor_id in record:
            try:
                payload[sensor_id] = round(float(record[sensor_id]), 2)
            except (ValueError, TypeError):
                pass

    publisher._publish(topic, payload, retain=False)


def main():
    """Main entry point for standalone MQTT publishing."""
    import argparse

    parser = argparse.ArgumentParser(description="Publish Tesla Solar data to MQTT")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--all-history",
        action="store_true",
        help="Publish all historical data (useful for initial setup/backfill)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of records per batch when publishing history (default: 100)",
    )
    parser.add_argument(
        "--batch-delay",
        type=float,
        default=0.1,
        help="Delay in seconds between batches (default: 0.1)",
    )
    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.debug else getattr(logging, Config.LOG_LEVEL)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Validate config
    errors = Config.validate()
    if errors:
        for error in errors:
            logger.error(f"Configuration error: {error}")
        return 1

    # Connect and publish
    publisher = MQTTPublisher()
    if not publisher.connect():
        logger.error("Failed to connect to MQTT broker")
        return 1

    try:
        data_dir = Path(Config.DATA_DIR)

        if args.all_history:
            logger.info("Publishing ALL historical data to MQTT...")
            publish_historical_data(
                publisher,
                data_dir,
                batch_size=args.batch_size,
                delay_between_batches=args.batch_delay,
            )
            logger.info("Successfully published all historical data to MQTT")
        else:
            publish_all_data(publisher, data_dir)
            logger.info("Successfully published latest data to MQTT")
    finally:
        publisher.disconnect()

    return 0


if __name__ == "__main__":
    exit(main())

