"""
Configuration management for Tesla Solar Download.

Configuration can be provided via:
1. Environment variables (recommended for automation)
2. A .env file in the project root
3. config.yaml file in the project root

Environment variables take precedence over config files.
"""

import os
from pathlib import Path
from typing import Optional

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Try to load config.yaml if pyyaml is available
_yaml_config = {}
try:
    import yaml
    config_path = Path(__file__).parent / "config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            _yaml_config = yaml.safe_load(f) or {}
except ImportError:
    pass


def _get_config(key: str, default: Optional[str] = None) -> Optional[str]:
    """Get config value from environment or yaml config."""
    # Environment variables take precedence
    env_value = os.environ.get(key)
    if env_value is not None:
        return env_value
    # Fall back to yaml config (convert key from UPPER_SNAKE to lower_snake)
    yaml_key = key.lower()
    return _yaml_config.get(yaml_key, default)


class Config:
    """Configuration settings for Tesla Solar Download."""
    
    # Tesla account settings
    TESLA_EMAIL: str = _get_config("TESLA_EMAIL", "")
    
    # MQTT settings for Home Assistant integration
    MQTT_ENABLED: bool = _get_config("MQTT_ENABLED", "true").lower() == "true"
    MQTT_HOST: str = _get_config("MQTT_HOST", "localhost")
    MQTT_PORT: int = int(_get_config("MQTT_PORT", "1883"))
    MQTT_USERNAME: Optional[str] = _get_config("MQTT_USERNAME")
    MQTT_PASSWORD: Optional[str] = _get_config("MQTT_PASSWORD")
    MQTT_TOPIC_PREFIX: str = _get_config("MQTT_TOPIC_PREFIX", "tesla_solar")
    MQTT_DISCOVERY_PREFIX: str = _get_config("MQTT_DISCOVERY_PREFIX", "homeassistant")
    MQTT_CLIENT_ID: str = _get_config("MQTT_CLIENT_ID", "tesla_solar_download")
    
    # Home Assistant MQTT Discovery (auto-configure sensors)
    HA_DISCOVERY_ENABLED: bool = _get_config("HA_DISCOVERY_ENABLED", "true").lower() == "true"
    
    # InfluxDB settings (for historical data storage)
    INFLUXDB_ENABLED: bool = _get_config("INFLUXDB_ENABLED", "false").lower() == "true"
    INFLUXDB_URL: str = _get_config("INFLUXDB_URL", "http://localhost:8086")
    INFLUXDB_TOKEN: str = _get_config("INFLUXDB_TOKEN", "")
    INFLUXDB_ORG: str = _get_config("INFLUXDB_ORG", "home")
    INFLUXDB_BUCKET: str = _get_config("INFLUXDB_BUCKET", "tesla_solar")
    
    # Data directory
    DATA_DIR: str = _get_config("DATA_DIR", str(Path(__file__).parent / "download"))
    
    # Logging
    LOG_LEVEL: str = _get_config("LOG_LEVEL", "INFO")
    LOG_FILE: Optional[str] = _get_config("LOG_FILE")
    
    # Debug mode
    DEBUG: bool = _get_config("DEBUG", "false").lower() == "true"
    
    @classmethod
    def validate(cls) -> list[str]:
        """Validate configuration and return list of errors."""
        errors = []
        if not cls.TESLA_EMAIL:
            errors.append("TESLA_EMAIL is required")
        if cls.MQTT_ENABLED:
            if not cls.MQTT_HOST:
                errors.append("MQTT_HOST is required when MQTT is enabled")
        return errors
    
    @classmethod
    def to_dict(cls) -> dict:
        """Return configuration as a dictionary (excludes sensitive values)."""
        return {
            "tesla_email": cls.TESLA_EMAIL[:3] + "***" if cls.TESLA_EMAIL else None,
            "mqtt_enabled": cls.MQTT_ENABLED,
            "mqtt_host": cls.MQTT_HOST,
            "mqtt_port": cls.MQTT_PORT,
            "mqtt_topic_prefix": cls.MQTT_TOPIC_PREFIX,
            "ha_discovery_enabled": cls.HA_DISCOVERY_ENABLED,
            "influxdb_enabled": cls.INFLUXDB_ENABLED,
            "influxdb_url": cls.INFLUXDB_URL,
            "influxdb_org": cls.INFLUXDB_ORG,
            "influxdb_bucket": cls.INFLUXDB_BUCKET,
            "data_dir": cls.DATA_DIR,
            "log_level": cls.LOG_LEVEL,
            "debug": cls.DEBUG,
        }


# Example config.yaml template
CONFIG_TEMPLATE = """# Tesla Solar Download Configuration
# Copy this to config.yaml and fill in your values

# Tesla account email (required)
tesla_email: "your_tesla_email@example.com"

# MQTT Settings for Home Assistant
mqtt_enabled: true
mqtt_host: "localhost"  # or your HA IP address
mqtt_port: 1883
mqtt_username: ""  # leave empty if not using authentication
mqtt_password: ""
mqtt_topic_prefix: "tesla_solar"

# Home Assistant MQTT Discovery
# When enabled, sensors will auto-appear in Home Assistant
ha_discovery_enabled: true

# Logging
log_level: "INFO"  # DEBUG, INFO, WARNING, ERROR
log_file: ""  # leave empty for stdout only

# Debug mode
debug: false
"""


if __name__ == "__main__":
    # Print current configuration
    print("Current Configuration:")
    for key, value in Config.to_dict().items():
        print(f"  {key}: {value}")
    
    errors = Config.validate()
    if errors:
        print("\nConfiguration Errors:")
        for error in errors:
            print(f"  - {error}")

