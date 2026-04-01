#!/usr/bin/env python3
"""
Daily Tesla Solar Data Download and Publish.

This script orchestrates:
1. Downloading the latest Tesla Solar data
2. Publishing the data to MQTT for Home Assistant (real-time sensors)
3. Publishing the data to InfluxDB (historical storage)

Designed to run unattended via systemd timer or cron.
"""

import argparse
import logging
import sys
import traceback
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config import Config

# Setup logging
def setup_logging(debug: bool = False):
    """Configure logging for the application."""
    log_level = logging.DEBUG if debug else getattr(logging, Config.LOG_LEVEL, logging.INFO)
    
    handlers = [logging.StreamHandler()]
    
    if Config.LOG_FILE:
        log_path = Path(Config.LOG_FILE)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(Config.LOG_FILE))
    
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )
    
    return logging.getLogger(__name__)


def download_tesla_data(email: str, debug: bool = False) -> bool:
    """
    Download Tesla Solar data using the existing tesla_solar_download module.
    
    Returns True if successful, False otherwise.
    """
    import teslapy
    from dateutil.parser import parse
    import pytz
    from datetime import timedelta
    import time
    import os
    
    # Import the download functions from the original module
    from tesla_solar_download import (
        _download_energy_data,
        _download_power_data,
        _delete_partial_energy_files,
        _delete_partial_power_files,
        _delete_partial_soe_files,
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        cache_file = os.environ.get("TESLA_CACHE_FILE", "cache.json")
        tesla = teslapy.Tesla(email, cache_file=cache_file, retry=2, timeout=10)
        
        if not tesla.authorized:
            logger.error(
                "Tesla API not authorized. Please run tesla_solar_download.py "
                "manually first to complete the OAuth flow."
            )
            return False
        
        for product in tesla.api('PRODUCT_LIST')['response']:
            resource_type = product.get('resource_type')
            if resource_type in ('battery', 'solar'):
                site_id = product['energy_site_id']
                obfuscated_site_id = f'***{str(site_id)[-4:]}'
                
                logger.info(f'Downloading energy data for {resource_type} site {obfuscated_site_id}')
                try:
                    _delete_partial_energy_files(site_id)
                    _download_energy_data(tesla, site_id, debug=debug)
                except Exception as e:
                    logger.error(f"Error downloading energy data: {e}")
                    if debug:
                        traceback.print_exc()
                
                logger.info(f'Downloading power data for {resource_type} site {obfuscated_site_id}')
                try:
                    _delete_partial_power_files(site_id)
                    _delete_partial_soe_files(site_id)
                    _download_power_data(tesla, site_id, debug=debug)
                except Exception as e:
                    logger.error(f"Error downloading power data: {e}")
                    if debug:
                        traceback.print_exc()
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to download Tesla data: {e}")
        if debug:
            traceback.print_exc()
        return False


def publish_to_mqtt(
    data_dir: Path,
    all_history: bool = False,
    batch_size: int = 100,
    batch_delay: float = 0.1,
) -> bool:
    """
    Publish data to MQTT.
    
    Args:
        data_dir: Path to the data directory
        all_history: If True, publish all historical data; otherwise just latest
        batch_size: Number of records per batch when publishing history
        batch_delay: Delay in seconds between batches
    
    Returns True if successful, False otherwise.
    """
    if not Config.MQTT_ENABLED:
        logging.getLogger(__name__).info("MQTT publishing is disabled")
        return True
    
    from mqtt_publisher import MQTTPublisher, publish_all_data, publish_historical_data
    
    logger = logging.getLogger(__name__)
    
    publisher = MQTTPublisher()
    if not publisher.connect():
        logger.error("Failed to connect to MQTT broker")
        return False
    
    try:
        if all_history:
            logger.info("Publishing ALL historical data to MQTT (this may take a while)...")
            publish_historical_data(
                publisher,
                data_dir,
                batch_size=batch_size,
                delay_between_batches=batch_delay,
            )
            logger.info("Successfully published all historical data to MQTT")
        else:
            publish_all_data(publisher, data_dir)
            logger.info("Successfully published latest data to MQTT")
        return True
    except Exception as e:
        logger.error(f"Failed to publish to MQTT: {e}")
        return False
    finally:
        publisher.disconnect()


def publish_to_influxdb(
    data_dir: Path,
    all_history: bool = False,
    batch_size: int = 1000,
) -> bool:
    """
    Publish data to InfluxDB.
    
    Args:
        data_dir: Path to the data directory
        all_history: If True, publish all historical data; otherwise just latest
        batch_size: Number of records per batch when publishing history
    
    Returns True if successful, False otherwise.
    """
    if not Config.INFLUXDB_ENABLED:
        logging.getLogger(__name__).info("InfluxDB publishing is disabled")
        return True
    
    from influxdb_publisher import (
        InfluxDBPublisher,
        publish_to_influxdb as influx_publish_all,
        publish_daily_to_influxdb,
    )
    
    logger = logging.getLogger(__name__)
    
    publisher = InfluxDBPublisher()
    if not publisher.connect():
        logger.error("Failed to connect to InfluxDB")
        return False
    
    try:
        if all_history:
            logger.info("Publishing ALL historical data to InfluxDB...")
            influx_publish_all(publisher, data_dir, batch_size=batch_size)
            logger.info("Successfully published all historical data to InfluxDB")
        else:
            publish_daily_to_influxdb(publisher, data_dir)
            logger.info("Successfully published today's data to InfluxDB")
        return True
    except Exception as e:
        logger.error(f"Failed to publish to InfluxDB: {e}")
        traceback.print_exc()
        return False
    finally:
        publisher.disconnect()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download Tesla Solar data and publish to MQTT/InfluxDB"
    )
    parser.add_argument(
        "--email",
        type=str,
        default=Config.TESLA_EMAIL,
        help="Tesla account email (or set TESLA_EMAIL env var)",
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Only download data, don't publish",
    )
    parser.add_argument(
        "--publish-only",
        action="store_true",
        help="Only publish existing data, don't download",
    )
    parser.add_argument(
        "--all-history",
        action="store_true",
        help="Publish all historical data (useful for initial setup/backfill)",
    )
    parser.add_argument(
        "--influxdb-only",
        action="store_true",
        help="Only publish to InfluxDB (skip MQTT)",
    )
    parser.add_argument(
        "--mqtt-only",
        action="store_true",
        help="Only publish to MQTT (skip InfluxDB)",
    )
    parser.add_argument(
        "--influxdb-all-history",
        action="store_true",
        help="Publish all history to InfluxDB (while still publishing latest to MQTT)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Records per batch when publishing history (default: 1000)",
    )
    parser.add_argument(
        "--batch-delay",
        type=float,
        default=0.1,
        help="Delay in seconds between MQTT batches (default: 0.1)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=Config.DEBUG,
        help="Enable debug mode",
    )
    args = parser.parse_args()
    
    logger = setup_logging(args.debug)
    
    logger.info(f"Tesla Solar Daily Run started at {datetime.now().isoformat()}")
    
    # Validate configuration
    email = args.email
    if not email and not args.publish_only:
        logger.error("Tesla email is required. Set TESLA_EMAIL env var or use --email")
        return 1
    
    data_dir = Path(Config.DATA_DIR)
    success = True
    
    # Download data
    if not args.publish_only:
        logger.info("Starting Tesla data download...")
        if not download_tesla_data(email, debug=args.debug):
            logger.error("Data download failed")
            success = False
        else:
            logger.info("Data download completed")
    
    # Publish to MQTT
    if not args.download_only and not args.influxdb_only:
        if Config.MQTT_ENABLED:
            if args.all_history:
                logger.info("Publishing ALL historical data to MQTT...")
            else:
                logger.info("Publishing latest data to MQTT...")
            
            if not publish_to_mqtt(
                data_dir,
                all_history=args.all_history,
                batch_size=args.batch_size,
                batch_delay=args.batch_delay,
            ):
                logger.error("MQTT publish failed")
                success = False
            else:
                logger.info("MQTT publish completed")
    
    # Publish to InfluxDB
    if not args.download_only and not args.mqtt_only:
        if Config.INFLUXDB_ENABLED:
            influxdb_all_history = args.all_history or args.influxdb_all_history
            if influxdb_all_history:
                logger.info("Publishing ALL historical data to InfluxDB...")
            else:
                logger.info("Publishing latest data to InfluxDB...")

            if not publish_to_influxdb(
                data_dir,
                all_history=influxdb_all_history,
                batch_size=args.batch_size,
            ):
                logger.error("InfluxDB publish failed")
                success = False
            else:
                logger.info("InfluxDB publish completed")
    
    if success:
        logger.info("Daily run completed successfully")
        return 0
    else:
        logger.error("Daily run completed with errors")
        return 1


if __name__ == "__main__":
    sys.exit(main())

