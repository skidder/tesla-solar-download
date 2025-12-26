# Tesla Solar/Powerwall Data Downloader

## Introduction

This script will download your entire history of Tesla Solar power and energy data:
solar/battery/grid power data in 5 minute intervals, battery state of charge in 15 minute intervals,
and daily totals for solar/home/battery/grid energy.

The script is using the [unofficial Tesla API](https://tesla-api.timdorr.com/)
and [TeslaPy](https://github.com/tdorssers/TeslaPy) library.  Data is stored in CSV files: one file per
day for power, and one file per month for energy.  You can run the script repeatedly and it will only
download new data.

Note: if you're not comfortable running Python code and want better data exports from your Tesla solar/battery system,
consider the [Netzero app](https://www.netzero.energy).

## Installation

1. If needed, install Python 3 and git.
2. Clone the repo:
    ```bash
    git clone https://github.com/netzero-labs/tesla-solar-download.git
    cd tesla-solar-download
    ```

2. Install the package dependencies:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip3 install --upgrade pip
    pip3 install -r requirements.txt
    ```

## Usage
```bash
source venv/bin/activate
python3 ./tesla_solar_download.py --email my_tesla_email@gmail.com
```

Follow the instructions to log in to Tesla with your web browser (this is needed to generate an API
token, the credentials are only sent to Tesla).  Once you've logged in, the token will be stored
locally so you can rerun the script if needed.

Data will start downloading to the `download` directory.  Starting with today and going back in time
all the way to the Tesla system's installation date.

Power data downloads take ~1.5 seconds per day (~10 minutes per year of data).  This is mostly due
to delays used to slow down the rate of API requests.  You may interrupt and restart the process
-- any CSV files that already exist will be skipped during the next run.

Energy downloads are faster (less than 30s per year).


## Data

Power data is formatted as follows:
`download/<site_id>/power/2022-07-19.csv`
```CSV
timestamp,solar_power,battery_power,grid_power,load_power
[...]
2023-07-19 10:40:00,7506.428571428572,-7401.224489795918,612.5714285714286,717.775510204082
2023-07-19 10:45:00,7576.836734693878,-3342.0408163265306,-3555.030612244898,679.7653061224487
2023-07-19 10:50:00,7616.666666666667,-3466.6666666666665,-3544.4,605.5999999999999
[...]
```

- One CSV file per day.
- Every file starts at midnight and ends at 11.55pm, in 5 minute increments.
- All power values are in Watts. Note: to get Watt-hour energy values for the 5-minute interval, divide the value by 12. You can then add up all the values and divide by 1000 for the daily kWh total.
- load_power is simply a sum of solar+battery+grid+generator power and is what is shown as "house" load in the Tesla app.  (Note: this value is not included in API responses since it can be easily derived.)

Energy data:
`download/<site_id>/energy/2022-07.csv`
```CSV
timestamp,solar_energy_exported,grid_energy_imported,grid_energy_exported_from_solar,grid_energy_exported_from_battery,battery_energy_exported,battery_energy_imported_from_grid,battery_energy_imported_from_solar,consumer_energy_imported_from_grid,consumer_energy_imported_from_solar,consumer_energy_imported_from_battery
2023-07-01 01:00:00,66700,6493.5,43456,0,16760,249.5,15640.5,6244,7603.5,16760
2023-07-02 01:00:00,66780,6353,40874,0,14060,260,18510,6093,7396,14060
2023-07-03 01:00:00,67380,6282,45964.5,0,10030,230,15580,6052,5835.5,10030
[...]
```

Powerwall state of charge data:
`download/<site_id>/soe/2022-07-19.csv`
```CSV
timestamp,soe
2024-07-19 00:00:00,44
2024-07-19 00:15:00,43
2024-07-19 00:30:00,43
[...]
```

## Automated Daily Runs

The project supports automated daily data downloads with optional Home Assistant integration via MQTT.

### Configuration

1. Copy the example environment file and edit it:
    ```bash
    cp env.example .env
    nano .env
    ```

2. Configure your settings:
    ```bash
    # Required
    TESLA_EMAIL=your_tesla_email@example.com
    
    # MQTT (for Home Assistant)
    MQTT_ENABLED=true
    MQTT_HOST=your-home-assistant-ip
    MQTT_PORT=1883
    MQTT_USERNAME=mqtt_user
    MQTT_PASSWORD=mqtt_password
    ```

3. Install additional dependencies:
    ```bash
    pip3 install -r requirements.txt
    ```

### Running the Daily Script

```bash
# Download data and publish to MQTT
python3 run_daily.py

# Download only (no MQTT)
python3 run_daily.py --download-only

# Publish existing data to MQTT only
python3 run_daily.py --publish-only

# Publish ALL historical data (useful for initial setup/backfill)
python3 run_daily.py --publish-only --all-history

# Adjust batch settings for history publish (reduce broker load)
python3 run_daily.py --publish-only --all-history --batch-size 50 --batch-delay 0.2

# Debug mode
python3 run_daily.py --debug
```

### Setting Up Automatic Daily Runs (systemd)

1. Run the installer:
    ```bash
    cd systemd
    ./install.sh
    ```

2. Check the timer status:
    ```bash
    systemctl --user status tesla-solar.timer
    ```

3. View logs:
    ```bash
    journalctl --user -u tesla-solar.service -f
    ```

The service runs daily at 11:30 PM by default (configurable in the timer file).

## Home Assistant Integration

The project publishes data to MQTT with Home Assistant auto-discovery support. Sensors will automatically appear in Home Assistant after the first data publish.

### Prerequisites

1. **MQTT Broker**: Install the Mosquitto broker add-on in Home Assistant
2. **MQTT Integration**: Enable MQTT in Home Assistant Settings → Devices & Services

### Sensors

The following sensors are automatically created:

**Power (5-minute intervals):**
- Solar Power (W)
- Battery Power (W)
- Grid Power (W)
- Home Load (W)

**Battery:**
- Battery State of Charge (%)

**Energy (daily totals):**
- Solar Energy Today (Wh)
- Grid Energy Imported (Wh)
- Grid Energy Exported (Wh)
- Battery Energy Charged/Discharged (Wh)

### Energy Dashboard

Add the sensors to your Energy Dashboard:
1. Settings → Dashboards → Energy
2. Add "Tesla Solar Energy Today" under Solar Panels
3. Add grid import/export sensors

See `homeassistant/README.md` for detailed configuration and dashboard examples.

## InfluxDB Integration (Historical Data)

**MQTT only stores the latest value** - it doesn't preserve historical timestamps. For proper historical data import with correct timestamps, use InfluxDB.

### Setting Up InfluxDB

1. **Install InfluxDB** (via Home Assistant add-on or standalone):
   ```bash
   # Docker example
   docker run -d -p 8086:8086 \
     -v influxdb-data:/var/lib/influxdb2 \
     influxdb:2.7
   ```

2. **Create a bucket and token** in the InfluxDB UI (http://localhost:8086)

3. **Configure your .env**:
   ```bash
   INFLUXDB_ENABLED=true
   INFLUXDB_URL=http://localhost:8086
   INFLUXDB_TOKEN=your-token-here
   INFLUXDB_ORG=home
   INFLUXDB_BUCKET=tesla_solar
   ```

### Importing Historical Data

```bash
# Import all historical data to InfluxDB (with proper timestamps!)
python run_daily.py --publish-only --all-history --influxdb-only

# Or use the standalone script
python influxdb_publisher.py --all-history
```

### Viewing in Home Assistant

1. Install the **InfluxDB integration** in Home Assistant
2. Or use **Grafana** with the InfluxDB datasource for advanced dashboards
3. Grafana can be embedded in Home Assistant via iframe

### Data Schema in InfluxDB

```
Measurements:
- tesla_solar_power  (solar_power, battery_power, grid_power, load_power)
- tesla_solar_soe    (soe)
- tesla_solar_energy (solar_energy_exported, grid_energy_imported, etc.)

Tags:
- site_id
```

Example Flux query:
```flux
from(bucket: "tesla_solar")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "tesla_solar_power")
  |> filter(fn: (r) => r._field == "solar_power")
```
