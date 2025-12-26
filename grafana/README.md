# Grafana Dashboard for Tesla Solar

This directory contains a pre-built Grafana dashboard for visualizing Tesla Solar data from InfluxDB.

## Dashboard Features

![Dashboard Preview](https://via.placeholder.com/800x400?text=Tesla+Solar+Dashboard)

### Current Status (Top Row)
- ☀️ **Solar Power** - Current solar production (gauge)
- 🔋 **Battery Power** - Current charge/discharge rate (gauge)
- ⚡ **Grid Power** - Current grid import/export (gauge)
- 🔋 **Battery SOC** - Current battery state of charge (gauge)

### Power Over Time
- **Power Flow** - Line chart showing solar, battery, grid, and home load over time
- **Battery State of Charge** - Battery percentage over time with threshold coloring

### Energy Statistics
- **Today's Stats** - Solar generated, grid import/export, home solar use
- **Daily Energy Bar Charts** - Solar/grid/export per day
- **Daily Battery Activity** - Charge/discharge per day

### Summary Statistics (Bottom)
- Total solar generated (kWh)
- Total grid import/export (kWh)
- Total battery charged/discharged (kWh)
- Self-powered percentage calculation

## Prerequisites

1. **InfluxDB 2.x** with Tesla Solar data imported
2. **Grafana** (standalone or as Home Assistant add-on)
3. **InfluxDB data source** configured in Grafana

## Installation

### Step 1: Configure InfluxDB Data Source in Grafana

1. Go to **Configuration → Data Sources → Add data source**
2. Select **InfluxDB**
3. Configure:
   - **Query Language**: Flux
   - **URL**: `http://localhost:8086` (or your InfluxDB URL)
   - **Organization**: Your InfluxDB org (e.g., `home`)
   - **Token**: Your InfluxDB API token
   - **Default Bucket**: `tesla_solar`
4. Click **Save & Test**

### Step 2: Import the Dashboard

1. Go to **Dashboards → Import**
2. Click **Upload JSON file**
3. Select `tesla-solar-dashboard.json` from this directory
4. Select your InfluxDB data source
5. Click **Import**

### Step 3: Configure Variables (if needed)

The dashboard uses two variables:
- **DS_INFLUXDB** - Your InfluxDB data source (auto-detected)
- **bucket** - The InfluxDB bucket name (default: `tesla_solar`)

If your bucket name is different, click the gear icon and update the `bucket` variable.

## Customization

### Adjusting Thresholds

The gauges have preset thresholds. To customize:
1. Click on a panel
2. Select **Edit**
3. Go to **Field** tab → **Thresholds**
4. Adjust values to match your system capacity

### Panel Size

- Resize panels by dragging the bottom-right corner
- Rearrange by dragging the title bar

### Time Range

- Default: Last 7 days
- Use the time picker in the top-right to change
- For historical analysis, try "Last 30 days" or "This year"

## Embedding in Home Assistant

### Option 1: Webpage Card (iframe)

Add to your Lovelace dashboard:

```yaml
type: iframe
url: http://your-grafana-ip:3000/d/tesla-solar/tesla-solar-dashboard?orgId=1&kiosk
aspect_ratio: 16:9
```

### Option 2: Custom Panel Card

Install the [Grafana Dashboard Card](https://github.com/custom-cards/canvas-gauge-card) HACS integration.

### Option 3: Screenshot Automation

Use Grafana's rendering API to take periodic screenshots and display as images in HA.

## Troubleshooting

### "No Data" on Panels

1. Verify InfluxDB data source is working (Test button)
2. Check the bucket name matches your configuration
3. Ensure data has been imported: run `python run_daily.py --publish-only --all-history --influxdb-only`

### Missing Metrics

Some metrics may not be available if you don't have a Powerwall:
- Battery power/SOE panels will show no data
- This is expected - solar-only systems won't have battery metrics

### Flux Query Errors

If you see query errors:
1. Check your InfluxDB version (requires 2.x with Flux)
2. Verify the data source is configured for Flux (not InfluxQL)

## Data Schema Reference

The dashboard expects data in this format:

```
Measurements:
├── tesla_solar_power
│   ├── solar_power (W)
│   ├── battery_power (W)
│   ├── grid_power (W)
│   └── load_power (W)
├── tesla_solar_soe
│   └── soe (%)
└── tesla_solar_energy
    ├── solar_energy_exported (Wh)
    ├── grid_energy_imported (Wh)
    ├── grid_energy_exported_from_solar (Wh)
    ├── battery_energy_exported (Wh)
    ├── battery_energy_imported_from_solar (Wh)
    └── consumer_energy_imported_from_solar (Wh)

Tags: site_id
```

