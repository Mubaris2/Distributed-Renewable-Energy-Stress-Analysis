"""
Pull Actual Load, Solar generation, and Wind generation (onshore + offshore)
from the ENTSO-E Transparency Platform for one bidding zone.

Run locally (requires network access to entsoe.eu, which this sandbox does not have):
    pip install -r requirements.txt
    cp .env.example .env   # then paste your real token into .env
    python fetch_entsoe.py --country NL --months 3

Output: raw XML saved to data/raw/, parsed CSV saved to data/processed/
"""

import argparse
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
import pandas as pd
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

BASE_URL = "https://web-api.tp.entsoe.eu/api"
NS = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
NS_LOAD = {"ns": "urn:iec62325.351:tc57wg16:451-6:loaddocument:3:0"}

# EIC domain codes for the pilot bundle (extend when scaling up)
DOMAIN_CODES = {
    "NL": "10YNL----------L",
    "BE": "10YBE----------2",
    "FR": "10YFR-RTE------C",
    "DE-LU": "10Y1001A1001A82H",
}

# psrType codes for generation-per-type requests
PSR_TYPES = {
    "solar": "B16",
    "wind_onshore": "B19",
    "wind_offshore": "B18",
}

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"


def get_token():
    load_dotenv(ROOT / ".env")
    token = os.getenv("ENTSOE_API_TOKEN")
    if not token:
        raise RuntimeError("ENTSOE_API_TOKEN not found. Copy .env.example to .env and fill it in.")
    return token


def chunk_date_range(start: datetime, end: datetime, days=30):
    """ENTSO-E rejects overly long ranges for some endpoints, so pull in chunks."""
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=days), end)
        yield cur, nxt
        cur = nxt


def fetch_load(domain_code, start, end, token):
    frames = []
    for chunk_start, chunk_end in chunk_date_range(start, end):
        params = {
            "documentType": "A65",
            "processType": "A16",  # Realised
            "outBiddingZone_Domain": domain_code,
            "periodStart": chunk_start.strftime("%Y%m%d%H%M"),
            "periodEnd": chunk_end.strftime("%Y%m%d%H%M"),
            "securityToken": token,
        }
        resp = requests.get(BASE_URL, params=params, timeout=60)
        resp.raise_for_status()
        frames.append(parse_timeseries_xml(resp.text, "load_mw", NS_LOAD))
        time.sleep(1)  # be polite to the API
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_generation(domain_code, psr_type, column_name, start, end, token):
    frames = []
    for chunk_start, chunk_end in chunk_date_range(start, end):
        params = {
            "documentType": "A75",
            "processType": "A16",
            "in_Domain": domain_code,
            "psrType": psr_type,
            "periodStart": chunk_start.strftime("%Y%m%d%H%M"),
            "periodEnd": chunk_end.strftime("%Y%m%d%H%M"),
            "securityToken": token,
        }
        resp = requests.get(BASE_URL, params=params, timeout=60)
        resp.raise_for_status()
        frames.append(parse_timeseries_xml(resp.text, column_name, NS))
        time.sleep(1)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def parse_timeseries_xml(xml_text, column_name, ns):
    """Flatten ENTSO-E's nested TimeSeries/Period/Point XML into timestamp, value rows."""
    root = ET.fromstring(xml_text)
    rows = []
    for ts in root.findall(".//ns:TimeSeries", ns):
        for period in ts.findall("ns:Period", ns):
            start_str = period.find("ns:timeInterval/ns:start", ns).text
            resolution = period.find("ns:resolution", ns).text
            start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%MZ")
            step = pd.Timedelta(resolution.replace("PT", "").replace("M", "min").replace("H", "h"))
            for point in period.findall("ns:Point", ns):
                position = int(point.find("ns:position", ns).text)
                value = float(point.find("ns:quantity", ns).text)
                timestamp = start_dt + (position - 1) * step
                rows.append({"timestamp": timestamp, column_name: value})
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", default="NL", choices=DOMAIN_CODES.keys())
    parser.add_argument("--months", type=int, default=3)
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    token = get_token()
    domain_code = DOMAIN_CODES[args.country]
    end = datetime.utcnow()
    start = end - timedelta(days=30 * args.months)

    print(f"Fetching {args.country} load, {start.date()} to {end.date()}...")
    load_df = fetch_load(domain_code, start, end, token)

    print("Fetching solar generation...")
    solar_df = fetch_generation(domain_code, PSR_TYPES["solar"], "solar_mw", start, end, token)

    print("Fetching wind onshore generation...")
    wind_on_df = fetch_generation(domain_code, PSR_TYPES["wind_onshore"], "wind_onshore_mw", start, end, token)

    print("Fetching wind offshore generation...")
    wind_off_df = fetch_generation(domain_code, PSR_TYPES["wind_offshore"], "wind_offshore_mw", start, end, token)

    # Merge all series on timestamp
    merged = load_df
    for df in [solar_df, wind_on_df, wind_off_df]:
        if not df.empty:
            merged = merged.merge(df, on="timestamp", how="outer")
    merged = merged.sort_values("timestamp").reset_index(drop=True)

    out_path = PROCESSED_DIR / f"entsoe_{args.country}_{start.date()}_{end.date()}.csv"
    merged.to_csv(out_path, index=False)
    print(f"Saved {len(merged)} rows to {out_path}")

    # Quick data-quality summary, this is the check before committing to the dataset
    print("\n--- Data quality summary ---")
    print(f"Rows: {len(merged)}")
    print(f"Date range: {merged['timestamp'].min()} to {merged['timestamp'].max()}")
    print("Missing values per column:")
    print(merged.isna().sum())
    expected_rows = int((end - start).total_seconds() / 900)  # assuming 15-min resolution
    print(f"Expected rows at 15-min resolution: ~{expected_rows}, got {len(merged)}")


if __name__ == "__main__":
    main()
