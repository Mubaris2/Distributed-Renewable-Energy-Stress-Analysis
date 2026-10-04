"""
Pull Actual Load, Solar generation, and Wind generation (onshore + offshore)
from the ENTSO-E Transparency Platform for one bidding zone.

Run locally (requires network access to entsoe.eu):
    pip install -r requirements.txt
    cp .env.example .env   # then paste your real token into .env
    python fetch_entsoe.py --country NL --months 3

Output: raw XML saved to data/raw/, parsed CSV saved to data/processed/
"""

import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import pandas as pd
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

BASE_URL = "https://web-api.tp.entsoe.eu/api"

DOMAIN_CODES = {
    "NL": "10YNL----------L",
    "BE": "10YBE----------2",
    "FR": "10YFR-RTE------C",
    "DE-LU": "10Y1001A1001A82H",
}

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
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=days), end)
        yield cur, nxt
        cur = nxt


def strip_ns(tag):
    return tag.split("}")[-1] if "}" in tag else tag


def check_for_api_error(xml_text):
    """ENTSO-E returns a 200 OK with an Acknowledgement_MarketDocument when a
    request is valid but has no data or a parameter problem. Surface that reason
    instead of silently returning an empty frame."""
    root = ET.fromstring(xml_text)
    if strip_ns(root.tag) == "Acknowledgement_MarketDocument":
        reason = None
        for el in root.iter():
            if strip_ns(el.tag) == "text":
                reason = el.text
        print(f"    [API note] {reason or 'No data / acknowledgement returned, no reason text found.'}")
        return True
    return False


def parse_timeseries_xml(xml_text, column_name):
    """Namespace-agnostic flattening of TimeSeries/Period/Point into timestamp rows.
    Different ENTSO-E document types use slightly different namespace URIs, so we
    match on local tag name instead of hardcoding a namespace."""
    root = ET.fromstring(xml_text)
    rows = []
    for ts in root.iter():
        if strip_ns(ts.tag) != "TimeSeries":
            continue
        for period in ts:
            if strip_ns(period.tag) != "Period":
                continue
            start_str, resolution, points = None, None, []
            for child in period:
                tag = strip_ns(child.tag)
                if tag == "timeInterval":
                    for sub in child:
                        if strip_ns(sub.tag) == "start":
                            start_str = sub.text
                elif tag == "resolution":
                    resolution = child.text
                elif tag == "Point":
                    pos, qty = None, None
                    for sub in child:
                        stag = strip_ns(sub.tag)
                        if stag == "position":
                            pos = int(sub.text)
                        elif stag == "quantity":
                            qty = float(sub.text)
                    if pos is not None and qty is not None:
                        points.append((pos, qty))
            if not start_str or not resolution:
                continue
            start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%MZ")
            step = pd.Timedelta(resolution.replace("PT", "").replace("M", "min").replace("H", "h"))
            for pos, qty in points:
                rows.append({"timestamp": start_dt + (pos - 1) * step, column_name: qty})
    return pd.DataFrame(rows)


def fetch_series(params_base, column_name, start, end, token, label, save_raw_prefix=None):
    frames = []
    for i, (chunk_start, chunk_end) in enumerate(chunk_date_range(start, end)):
        params = dict(params_base)
        params["periodStart"] = chunk_start.strftime("%Y%m%d%H%M")
        params["periodEnd"] = chunk_end.strftime("%Y%m%d%H%M")
        params["securityToken"] = token
        resp = requests.get(BASE_URL, params=params, timeout=60)
        if resp.status_code != 200:
            print(f"    [HTTP {resp.status_code}] {label} chunk {chunk_start.date()}-{chunk_end.date()}: {resp.text[:300]}")
            continue
        if save_raw_prefix and i == 0:
            RAW_DIR.mkdir(parents=True, exist_ok=True)
            (RAW_DIR / f"{save_raw_prefix}_sample.xml").write_text(resp.text)
        if check_for_api_error(resp.text):
            continue
        frames.append(parse_timeseries_xml(resp.text, column_name))
        time.sleep(1)
    if not frames:
        return pd.DataFrame(columns=["timestamp", column_name])
    combined = pd.concat(frames, ignore_index=True)
    # ENTSO-E often returns multiple document revisions (corrections/resubmissions)
    # covering the same period, not just the latest. Keep the last value seen per
    # timestamp, which corresponds to the most recently returned revision.
    before = len(combined)
    combined = combined.drop_duplicates(subset="timestamp", keep="last").sort_values("timestamp").reset_index(drop=True)
    if before != len(combined):
        print(f"    [dedup] {label}: {before} rows -> {len(combined)} rows after dropping revision duplicates")
    return combined


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", default="NL", choices=DOMAIN_CODES.keys())
    parser.add_argument("--months", type=int, default=3)
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    token = get_token()
    domain_code = DOMAIN_CODES[args.country]
    end = datetime.now(timezone.utc).replace(tzinfo=None)
    start = end - timedelta(days=30 * args.months)

    print(f"Fetching {args.country} load, {start.date()} to {end.date()}...")
    load_df = fetch_series(
        {"documentType": "A65", "processType": "A16", "outBiddingZone_Domain": domain_code},
        "load_mw", start, end, token, "load", save_raw_prefix="load",
    )

    print("Fetching solar generation...")
    solar_df = fetch_series(
        {"documentType": "A75", "processType": "A16", "in_Domain": domain_code, "psrType": PSR_TYPES["solar"]},
        "solar_mw", start, end, token, "solar", save_raw_prefix="solar",
    )

    print("Fetching wind onshore generation...")
    wind_on_df = fetch_series(
        {"documentType": "A75", "processType": "A16", "in_Domain": domain_code, "psrType": PSR_TYPES["wind_onshore"]},
        "wind_onshore_mw", start, end, token, "wind_onshore",
    )

    print("Fetching wind offshore generation...")
    wind_off_df = fetch_series(
        {"documentType": "A75", "processType": "A16", "in_Domain": domain_code, "psrType": PSR_TYPES["wind_offshore"]},
        "wind_offshore_mw", start, end, token, "wind_offshore",
    )

    if load_df.empty:
        print("\nLoad series came back empty. Check data/raw/load_sample.xml for the API's raw response"
              " (likely an Acknowledgement_MarketDocument explaining why), then stop here before merging.")
        return

    merged = load_df
    for df, name in [(solar_df, "solar"), (wind_on_df, "wind_onshore"), (wind_off_df, "wind_offshore")]:
        if df.empty:
            print(f"    [warning] {name} series is empty, skipping from merge.")
            continue
        merged = merged.merge(df, on="timestamp", how="outer")
    merged = merged.sort_values("timestamp").reset_index(drop=True)

    out_path = PROCESSED_DIR / f"entsoe_{args.country}_{start.date()}_{end.date()}.csv"
    merged.to_csv(out_path, index=False)
    print(f"\nSaved {len(merged)} rows to {out_path}")

    print("\n--- Data quality summary ---")
    print(f"Rows: {len(merged)}")
    print(f"Date range: {merged['timestamp'].min()} to {merged['timestamp'].max()}")
    print("Missing values per column:")
    print(merged.isna().sum())
    expected_rows = int((end - start).total_seconds() / 900)  # 15-min resolution assumption
    print(f"Expected rows at 15-min resolution: ~{expected_rows}, got {len(merged)}")


if __name__ == "__main__":
    main()
