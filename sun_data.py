#!/usr/bin/env python3
"""
Sun data calculation for solar optimization.

Provides sunrise/sunset/altitude/azimuth data using astral library.
Used by live_poller to enrich solar data with sun position.

Environment variables:
  LATITUDE  - Site latitude (default: 38.05 for Benicia, CA)
  LONGITUDE - Site longitude (default: -122.16 for Benicia, CA)
  TIMEZONE  - Timezone name (default: America/Los_Angeles)
"""

import logging
import os
from datetime import datetime, timezone as tz

logger = logging.getLogger(__name__)

try:
    from astral import LocationInfo
    from astral.sun import sun, elevation, azimuth
    HAS_ASTRAL = True
except ImportError:
    HAS_ASTRAL = False
    logger.warning("astral library not available; sun data will be empty")


class SunData:
    """Calculate and manage sun position data."""

    def __init__(self):
        """Initialize with location from environment or defaults."""
        if not HAS_ASTRAL:
            logger.warning("astral library not installed, sun data disabled")
            self.location = None
            return

        # Benicia, CA defaults (866 Oxford Way)
        self.latitude = float(os.environ.get('LATITUDE', '38.05'))
        self.longitude = float(os.environ.get('LONGITUDE', '-122.16'))
        self.timezone_name = os.environ.get('TIMEZONE', 'America/Los_Angeles')

        self.location = LocationInfo(
            name='Home',
            region='CA',
            timezone=self.timezone_name,
            latitude=self.latitude,
            longitude=self.longitude,
        )

    def get_sun_status(self):
        """
        Calculate current sun position and timing info.

        Returns:
            dict with sunrise, sunset, altitude, azimuth, is_daytime, etc.
            or empty dict if astral is not available.
        """
        if not self.location:
            return {}

        try:
            now = datetime.now(tz.utc)

            # Get sun times for today (use local date)
            import pytz
            local_tz = pytz.timezone(self.timezone_name)
            local_now = now.astimezone(local_tz)

            s = sun(self.location.observer, date=local_now.date(), tzinfo=local_tz)
            sunrise = s['sunrise']
            sunset = s['sunset']

            # Get current sun position
            alt = elevation(self.location.observer, now)
            az = azimuth(self.location.observer, now)

            is_daytime = sunrise <= local_now <= sunset
            time_to_sunset = max(0, (sunset - local_now).total_seconds() / 3600) if is_daytime else 0
            time_to_sunrise = max(0, (sunrise - local_now).total_seconds() / 3600) if not is_daytime else 0

            return {
                'timestamp': local_now.isoformat(),
                'sunrise': sunrise.strftime('%H:%M'),
                'sunset': sunset.strftime('%H:%M'),
                'altitude': round(alt, 2),  # degrees above horizon
                'azimuth': round(az, 2),  # degrees from north
                'is_daytime': is_daytime,
                'time_to_sunset_hours': round(time_to_sunset, 2),
                'time_to_sunrise_hours': round(time_to_sunrise, 2),
            }

        except Exception as e:
            logger.error(f'Failed to calculate sun data: {e}')
            import traceback
            traceback.print_exc()
            return {}

    def get_production_factor(self):
        """
        Get a solar production factor (0-1) based on sun altitude.

        Simple model: 0 if below horizon, scales with sin(altitude).
        Peaks at 1.0 when sun is at max altitude for this latitude (~75° summer solstice).
        """
        data = self.get_sun_status()
        if not data or not data.get('is_daytime'):
            return 0.0

        alt = data.get('altitude', 0)
        if alt <= 0:
            return 0.0

        import math
        # Use sin(altitude) as production factor — better model than linear
        # Normalize so that 75° (approximate max for 38°N) = 1.0
        factor = math.sin(math.radians(alt)) / math.sin(math.radians(75))
        return min(1.0, max(0.0, factor))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    sd = SunData()
    data = sd.get_sun_status()
    print("Sun Status:")
    for key, val in data.items():
        print(f"  {key}: {val}")
    print(f"\nProduction Factor: {sd.get_production_factor():.2%}")
