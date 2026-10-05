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


def find_latest_energy_csv(country):
    """Auto-discover the most recently fetched entsoe_<COUNTRY>_*.csv for this
    country, so adding countries doesn't mean manually tracking file paths."""
    matches = sorted(PROCESSED_DIR.glob(f"entsoe_{country}_*.csv"))
    # excludes files already named entsoe_weather_... since the glob is anchored
    # to "entsoe_<country>_", not "entsoe_weather_"
    if not matches:
        return None
    return matches[-1]  # lexicographic sort on date-stamped names = most recent


def process_one_country(country, energy_path_override=None):
    energy_path = Path(energy_path_override) if energy_path_override else find_latest_energy_csv(country)
    if energy_path is None or not energy_path.exists():
        print(f"\n[{country}] No energy CSV found (looked for entsoe_{country}_*.csv in "
              f"{PROCESSED_DIR}). Run fetch_entsoe.py for this country first, skipping.")
        return None

    print(f"\n=== {country} ===")
    print(f"Using energy data: {energy_path.name}")
    energy_df = pd.read_csv(energy_path, parse_dates=["timestamp"])
    start_date = energy_df["timestamp"].min().strftime("%Y-%m-%d")
    end_date = energy_df["timestamp"].max().strftime("%Y-%m-%d")

    lat, lon = COUNTRY_COORDS[country]
    print(f"Fetching weather for {country} ({lat}, {lon}), {start_date} to {end_date}...")
    weather_df = fetch_weather(lat, lon, start_date, end_date)
    print(f"Got {len(weather_df)} hourly weather rows.")

    print("Interpolating weather onto the 15-min energy timestamp grid...")
    weather_aligned = interpolate_to_energy_grid(weather_df, energy_df["timestamp"])

    merged = energy_df.merge(weather_aligned, on="timestamp", how="left")
    if "country" not in merged.columns:
        merged.insert(0, "country", country)

    out_path = PROCESSED_DIR / f"entsoe_weather_{country}_{start_date}_{end_date}.csv"
    merged.to_csv(out_path, index=False)
    print(f"Saved {len(merged)} rows to {out_path}")

    print(f"--- {country} combined data quality summary ---")
    print(f"Rows: {len(merged)}")
    print("Missing values per column:")
    print(merged.isna().sum())
    return out_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", default="NL",
                         help="Comma-separated country codes, e.g. NL or NL,BE,FR. "
                              f"Valid codes: {', '.join(COUNTRY_COORDS.keys())}")
    parser.add_argument("--energy-csv", default=None,
                         help="Optional: explicit path to one energy CSV. Only valid "
                              "when --country names exactly one country; ignored "
                              "(with auto-discovery used instead) for multi-country runs.")
    args = parser.parse_args()

    countries = [c.strip() for c in args.country.split(",") if c.strip()]
    invalid = [c for c in countries if c not in COUNTRY_COORDS]
    if invalid:
        raise SystemExit(f"Unknown country code(s): {invalid}. Valid codes: {list(COUNTRY_COORDS.keys())}")

    if args.energy_csv and len(countries) > 1:
        print("[warning] --energy-csv is ignored for multi-country runs, auto-discovering each country's file instead.")

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    results = {}
    for country in countries:
        override = args.energy_csv if (args.energy_csv and len(countries) == 1) else None
        results[country] = process_one_country(country, override)

    print("\n=== Summary across all requested countries ===")
    for country, path in results.items():
        status = f"saved to {path}" if path else "SKIPPED, see warnings above"
        print(f"  {country}: {status}")


if __name__ == "__main__":
    main()
