"""
scrape_worker.py

Runs the DoneDeal scraper, cleans the output into the shape the
Lovable Cloud / Supabase `listings` table expects, and upserts.

Intended to be invoked from GitHub Actions on a cron.

Env vars required:
    SUPABASE_URL          e.g. https://abc123.supabase.co
    SUPABASE_SERVICE_KEY  service_role key (NOT anon)
    SCRAPE_MAKES          optional comma-separated list of makes to limit to,
                          e.g. "BMW,Mercedes-Benz,Audi,Volkswagen,Toyota".
                          If unset, scrape everything DoneDeal returns.
"""

from __future__ import annotations

import os
import re
import math
import sys
import datetime as dt
from typing import Any

import pandas as pd
from supabase import create_client, Client

import donedeal as dd


# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
SCRAPE_MAKES = [m.strip() for m in os.environ.get("SCRAPE_MAKES", "").split(",") if m.strip()]

# Safety rails: we mark a listing inactive if it hasn't been seen within this window.
INACTIVE_AFTER_HOURS = 48

# Batch size for upsert calls (Supabase rejects very large payloads)
UPSERT_CHUNK = 500


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def _listing_id_from_url(url: str) -> str | None:
    """DoneDeal URLs look like .../cars-for-sale/<slug>/<numeric-id>"""
    if not isinstance(url, str):
        return None
    m = re.search(r"/(\d+)/?$", url)
    return m.group(1) if m else None


def _safe_int(x: Any) -> int | None:
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return int(x)
    except (ValueError, TypeError):
        return None


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        f = float(x)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None


def _parse_engine_l(s: Any) -> float | None:
    if not isinstance(s, str):
        return None
    m = re.search(r"(\d+\.\d+)\s*L", s, re.IGNORECASE)
    return float(m.group(1)) if m else None


def _parse_engine_hp(s: Any) -> float | None:
    if not isinstance(s, str):
        return None
    m = re.search(r"(\d+)\s*(hp|bhp)", s, re.IGNORECASE)
    return float(m.group(1)) if m else None


def _parse_nct(s: Any) -> str | None:
    """'Jul 2025' -> '2025-07-01' (ISO date)."""
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        d = dt.datetime.strptime(s.strip(), "%b %Y").date()
        return d.isoformat()
    except ValueError:
        return None


def _empty_to_none(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


# ------------------------------------------------------------------
# Scrape
# ------------------------------------------------------------------
def scrape_dataframe() -> pd.DataFrame:
    scraper = dd.CarScraper()

    if SCRAPE_MAKES:
        for make in SCRAPE_MAKES:
            scraper.set_make_model(make=make)
    # else: no make filter — scraper will pull what DoneDeal returns for the default URL

    scraper.scrape(batch_size=500)
    df = scraper.DataFrame

    if df is None or df.empty:
        print("[scrape] No rows returned.", flush=True)
        return pd.DataFrame()

    # Keep only the columns we care about (robust to missing)
    wanted = set(dd.constants.RECOMMENDED_COLUMNS)
    keep = [c for c in df.columns if c in wanted]
    df = df[keep].copy()

    # Built-in cleaners from the donedeal module
    if "geoPoint" in df.columns:
        df = dd.data_cleaning.assign_lat_lon(df)
    if "mileage" in df.columns:
        df = dd.data_cleaning.assign_mileage(df)
    if "price" in df.columns:
        df = dd.data_cleaning.price_to_float(df)
        df = df.loc[~df.price.apply(dd.data_cleaning.isBsPrice)]
    if "kilometers" in df.columns:
        df = df.loc[~df.kilometers.apply(dd.data_cleaning.isBsPrice)]

    return df.reset_index(drop=True)


# ------------------------------------------------------------------
# Transform to Supabase rows
# ------------------------------------------------------------------
def df_to_listing_rows(df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()

    for _, r in df.iterrows():
        url = r.get("friendlyUrl")
        lid = _listing_id_from_url(url)
        make = r.get("make")
        model = r.get("model")
        if not (lid and make and model):
            continue  # can't key or group without these

        row = {
            "id": lid,
            "url": url,
            "header": _empty_to_none(r.get("header")),
            "make": str(make),
            "model": str(model),
            "trim": _empty_to_none(r.get("trim")),
            "year": _safe_int(r.get("year")),
            "mileage_km": _safe_float(r.get("kilometers")),
            "price_eur": _safe_float(r.get("price")),
            "fuel_type": _empty_to_none(r.get("fuelType")),
            "transmission": _empty_to_none(r.get("transmission")),
            "engine_l": _parse_engine_l(r.get("engine")),
            "engine_hp": _parse_engine_hp(r.get("enginePower")),
            "body_type": _empty_to_none(r.get("bodyType")),
            "colour": _empty_to_none(r.get("colour")),
            "seats": _safe_int(r.get("seats")),
            "num_doors": _safe_int(r.get("numDoors")),
            "owners": _safe_int(r.get("owners")),
            "road_tax_eur": _safe_float(r.get("roadTax")),
            "nct_expiry": _parse_nct(r.get("NCTExpiry")),
            "seller_type": _empty_to_none(r.get("sellerType")),
            "county": _empty_to_none(r.get("county")),
            "county_town": _empty_to_none(r.get("countyTown")),
            "lat": _safe_float(r.get("lat")),
            "lon": _safe_float(r.get("lon")),
            "last_seen_at": now_iso,
            "is_active": True,
        }
        # Supabase won't overwrite first_seen_at on upsert because it's set
        # by the column default only on insert — we leave it off the payload.
        rows.append(row)

    return rows


# ------------------------------------------------------------------
# Upsert + deactivate stale
# ------------------------------------------------------------------
def upsert_chunks(supabase: Client, rows: list[dict]) -> int:
    if not rows:
        return 0
    written = 0
    for i in range(0, len(rows), UPSERT_CHUNK):
        chunk = rows[i : i + UPSERT_CHUNK]
        resp = (
            supabase.table("listings")
            .upsert(chunk, on_conflict="id")
            .execute()
        )
        written += len(chunk)
        print(f"[upsert] wrote {written}/{len(rows)}", flush=True)
    return written


def deactivate_stale(supabase: Client, hours: int = INACTIVE_AFTER_HOURS) -> int:
    cutoff = (
        dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours)
    ).isoformat()
    resp = (
        supabase.table("listings")
        .update({"is_active": False})
        .lt("last_seen_at", cutoff)
        .eq("is_active", True)
        .execute()
    )
    n = len(resp.data) if getattr(resp, "data", None) else 0
    print(f"[deactivate] marked {n} stale listings inactive (cutoff {cutoff})", flush=True)
    return n


# ------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------
def main() -> int:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    print("[scrape] fetching listings from DoneDeal...", flush=True)
    df = scrape_dataframe()
    print(f"[scrape] cleaned DataFrame has {len(df)} rows", flush=True)

    rows = df_to_listing_rows(df)
    print(f"[transform] {len(rows)} rows ready for upsert", flush=True)

    upsert_chunks(supabase, rows)
    deactivate_stale(supabase)

    print("[done]", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
