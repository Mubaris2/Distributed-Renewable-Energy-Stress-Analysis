"""
Pull historical weather (temperature, humidity, wind speed) from Open-Meteo's
free archive API and join it onto the already-fetched ENTSO-E energy CSV.

No API key needed for Open-Meteo.

Run locally:
    python fetch_weather.py --country NL --energy-csv ../data/processed/entsoe_NL_2026-07-06_2026-10-04.csv

Output: combined CSV in data/processed/ with energy + weather columns.
"""

import argparse
from pathlib import Path

import requests
import pandas as pd

# National reference coordinates (single representative point per country,
# matches the single-point weather-proxy approach used in several of the
# cited papers; document this simplification in the report).
COUNTRY_COORDS = {
    "NL": (52.11, 5.18),   # De Bilt, NL's standard national weather reference station
    "BE": (50.85, 4.35),   # Brussels
    "FR": (48.85, 2.35),   # Paris
    "DE-LU": (50.11, 8.68),  # Frankfurt, central DE reference
}

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = ROOT / "data" / "processed"


def fetch_weather(lat, lon, start_date, end_date):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "UTC",
    }
    resp = requests.get(ARCHIVE_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    hourly = data.get("hourly", {})
    if not hourly or "time" not in hourly:
        raise RuntimeError(f"Unexpected response from Open-Meteo: {data}")
    df = pd.DataFrame({
        "timestamp": pd.to_datetime(hourly["time"]),
        "temperature_c": hourly.get("temperature_2m"),
        "humidity_pct": hourly.get("relative_humidity_2m"),
        "wind_speed_ms": hourly.get("wind_speed_10m"),
    })
    return df


def interpolate_to_energy_grid(weather_df, energy_timestamps):
    """Open-Meteo is hourly, energy data is 15-min. Reindex weather onto the
    energy timestamps and linearly interpolate rather than downsampling the
    energy series. Document this as a modeled limitation, not hidden."""
    weather_df = weather_df.set_index("timestamp").sort_index()
    full_index = pd.DatetimeIndex(sorted(set(weather_df.index) | set(energy_timestamps)))
    weather_reindexed = weather_df.reindex(full_index).interpolate(method="time")
    return weather_reindexed.loc[energy_timestamps].reset_index().rename(columns={"index": "timestamp"})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", default="NL", choices=COUNTRY_COORDS.keys())
    parser.add_argument("--energy-csv", required=True, help="Path to the processed ENTSO-E CSV to join onto")
    args = parser.parse_args()

    energy_path = Path(args.energy_csv)
    if not energy_path.exists():
        raise FileNotFoundError(f"Energy CSV not found: {energy_path}")

    energy_df = pd.read_csv(energy_path, parse_dates=["timestamp"])
    start_date = energy_df["timestamp"].min().strftime("%Y-%m-%d")
    end_date = energy_df["timestamp"].max().strftime("%Y-%m-%d")

    lat, lon = COUNTRY_COORDS[args.country]
    print(f"Fetching weather for {args.country} ({lat}, {lon}), {start_date} to {end_date}...")
    weather_df = fetch_weather(lat, lon, start_date, end_date)
    print(f"Got {len(weather_df)} hourly weather rows.")

    print("Interpolating weather onto the 15-min energy timestamp grid...")
    weather_aligned = interpolate_to_energy_grid(weather_df, energy_df["timestamp"])

    merged = energy_df.merge(weather_aligned, on="timestamp", how="left")

    out_path = PROCESSED_DIR / f"entsoe_weather_{args.country}_{start_date}_{end_date}.csv"
    merged.to_csv(out_path, index=False)
    print(f"\nSaved {len(merged)} rows to {out_path}")

    print("\n--- Combined data quality summary ---")
    print(f"Rows: {len(merged)}")
    print("Missing values per column:")
    print(merged.isna().sum())


if __name__ == "__main__":
    main()
