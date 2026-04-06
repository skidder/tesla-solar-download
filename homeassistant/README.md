# Home Assistant Integration

This directory contains example configurations for integrating Tesla Solar data with Home Assistant.

## Prerequisites

1. **MQTT Broker**: You need an MQTT broker running. Home Assistant has a built-in MQTT broker add-on:
   - Go to Settings → Add-ons → Add-on Store
   - Search for "Mosquitto broker" and install it
   - Start the broker and note the credentials

2. **MQTT Integration**: Enable MQTT in Home Assistant:
   - Go to Settings → Devices & Services → Add Integration
   - Search for "MQTT" and configure it to connect to your broker

## Automatic Discovery (Recommended)

When `HA_DISCOVERY_ENABLED=true` (the default), sensors will **automatically appear** in Home Assistant after the first data publish. No manual configuration needed!

The sensors will appear under a device called "Tesla Solar XXXX" (where XXXX is the last 4 digits of your site ID).

## Manual Configuration (Optional)

If you prefer manual configuration or want to customize the sensors, add the following to your `configuration.yaml`:

```yaml
mqtt:
  sensor:
    # Power sensors (5-minute interval data)
    - name: "Tesla Solar Power"
      state_topic: "tesla_solar/{site_id}/solar_power"
      unit_of_measurement: "W"
      device_class: power
      state_class: measurement
      icon: mdi:solar-power

    - name: "Tesla Battery Power"
      state_topic: "tesla_solar/{site_id}/battery_power"
      unit_of_measurement: "W"
      device_class: power
      state_class: measurement
      icon: mdi:battery

    - name: "Tesla Grid Power"
      state_topic: "tesla_solar/{site_id}/grid_power"
      unit_of_measurement: "W"
      device_class: power
      state_class: measurement
      icon: mdi:transmission-tower

    - name: "Tesla Home Load"
      state_topic: "tesla_solar/{site_id}/load_power"
      unit_of_measurement: "W"
      device_class: power
      state_class: measurement
      icon: mdi:home-lightning-bolt

    # Battery state of charge
    - name: "Tesla Battery SOC"
      state_topic: "tesla_solar/{site_id}/soe"
      unit_of_measurement: "%"
      device_class: battery
      state_class: measurement

    # Energy sensors (daily totals)
    - name: "Tesla Solar Energy Today"
      state_topic: "tesla_solar/{site_id}/solar_energy_exported"
      unit_of_measurement: "Wh"
      device_class: energy
      state_class: total_increasing
      icon: mdi:solar-power

    - name: "Tesla Grid Import Today"
      state_topic: "tesla_solar/{site_id}/grid_energy_imported"
      unit_of_measurement: "Wh"
      device_class: energy
      state_class: total_increasing
      icon: mdi:transmission-tower-import

    - name: "Tesla Grid Export Today"
      state_topic: "tesla_solar/{site_id}/grid_energy_exported_from_solar"
      unit_of_measurement: "Wh"
      device_class: energy
      state_class: total_increasing
      icon: mdi:transmission-tower-export
```

Replace `{site_id}` with your actual Tesla site ID (found in the download directory name).

## Energy Dashboard

To add Tesla Solar to the Energy Dashboard:

1. Go to Settings → Dashboards → Energy
2. Under "Solar Panels", click "Add Solar Production"
3. Select "Tesla Solar Energy Today" sensor
4. Under "Grid Consumption", add "Tesla Grid Import Today"
5. Under "Return to Grid", add "Tesla Grid Export Today"
6. If you have a Powerwall, add battery sensors under "Battery Systems"

## Example Lovelace Cards

### Power Flow Card

```yaml
type: custom:power-flow-card
entities:
  grid: sensor.tesla_grid_power
  solar: sensor.tesla_solar_power
  battery: sensor.tesla_battery_power
  home: sensor.tesla_home_load
```

### Simple Gauge Card

```yaml
type: gauge
entity: sensor.tesla_solar_power
name: Solar Production
min: 0
max: 10000
severity:
  green: 5000
  yellow: 2000
  red: 0
```

### Battery State Card

```yaml
type: gauge
entity: sensor.tesla_battery_soc
name: Powerwall
min: 0
max: 100
severity:
  green: 50
  yellow: 20
  red: 0
```

## Publishing Historical Data

For initial setup or backfilling, you can publish all historical data:

```bash
# Publish all historical data
python run_daily.py --publish-only --all-history

# With custom batch settings (slower, gentler on broker)
python run_daily.py --publish-only --all-history --batch-size 50 --batch-delay 0.2
```

Historical data is published to separate topics with timestamps embedded in JSON payloads:
- `tesla_solar/{site_id}/history/power` - Power readings with timestamp
- `tesla_solar/{site_id}/history/soe` - Battery SOC with timestamp  
- `tesla_solar/{site_id}/history/energy` - Daily energy totals with timestamp

### Processing Historical Data in Home Assistant

To capture historical data, create an automation that listens for these topics:

```yaml
automation:
  - alias: "Import Tesla Solar History"
    trigger:
      - platform: mqtt
        topic: "tesla_solar/+/history/power"
    action:
      - service: logbook.log
        data:
          name: "Tesla Solar"
          message: "{{ trigger.payload }}"
```

For more advanced import (like into InfluxDB), you can subscribe to the history topics
and process the JSON payloads which include the original timestamp.

## Troubleshooting

### Sensors not appearing
1. Check MQTT broker is running: `mosquitto_pub -t test -m "hello"`
2. Verify the data is being published: `mosquitto_sub -t "tesla_solar/#" -v`
3. Check Home Assistant logs for MQTT errors

### Data not updating
1. Verify the systemd timer is running: `systemctl --user status tesla-solar.timer`
2. Check the service logs: `journalctl --user -u tesla-solar.service -f`
3. Run manually to test: `python run_daily.py --debug`

### Finding your Site ID
The site ID is the numeric directory name in the `download/` folder:
```
download/
└── 1234567890/    ← This is your site_id
    ├── energy/
    ├── power/
    └── soe/
```

