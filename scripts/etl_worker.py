"""
etl_worker.py

One-pass ETL:
  1. Scrape DoneDeal.
  2. Clean and transform into listing rows.
  3. POST listings to the Lovable Cloud `ingest-listings` edge function.
  4. Fit a per-(make, model) linear regression on the in-memory data.
  5. POST outliers + price-model coefficients to the `ingest-outliers`
     edge function. That function also triggers `fan_out_alerts()` so
     newly-flagged underpriced listings produce in-app notifications.

This replaces the earlier split scrape_worker.py + score_worker.py.
We merged them because, with Lovable Cloud's edge-function ingestion
pattern, the Python side no longer has direct DB read access — and
scoring in-memory off the fresh scrape is simpler than adding a
"read listings" edge function.

Env vars required (all set as GitHub Actions secrets):
    INGEST_LISTINGS_URL    https://<project>.supabase.co/functions/v1/ingest-listings
    INGEST_OUTLIERS_URL    https://<project>.supabase.co/functions/v1/ingest-outliers
    INGEST_API_KEY         shared secret; must match the INGEST_API_KEY
                           secret stored inside Lovable Cloud.
    SCRAPE_MAKES           optional comma-separated list of makes to limit to.
"""

from __future__ import annotations

import os
import re
import math
import sys
import json
import datetime as dt
from typing import Any

import numpy as np
import pandas as pd
import requests
from sklearn.linear_model import LinearRegression

import donedeal as dd


# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
INGEST_LISTINGS_URL = os.environ["INGEST_LISTINGS_URL"]
INGEST_OUTLIERS_URL = os.environ["INGEST_OUTLIERS_URL"]
INGEST_API_KEY = os.environ["INGEST_API_KEY"]
SCRAPE_MAKES = [m.strip() for m in os.environ.get("SCRAPE_MAKES", "").split(",") if m.strip()]

# Outlier thresholds
MIN_GROUP_SIZE = 30      # skip thin (make, model) cohorts
Z_THRESHOLD = -1.5       # negative residual = cheaper than predicted
MIN_PCT_BELOW = 0.15     # at least 15% under predicted price
MAX_PCT_BELOW = 0.60     # filter out obvious scam listings

# HTTP
POST_CHUNK = 200
REQUEST_TIMEOUT = 90


# ------------------------------------------------------------------
# HTTP helper
# ------------------------------------------------------------------
def post_json(url: str, payload: dict) -> dict:
    headers = {
        "x-api-key": INGEST_API_KEY,
        "content-type": "application/json",
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
    if not resp.ok:
        print(f"[http] {resp.status_code} {url} -> {resp.text[:500]}", flush=True)
        resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text}


def post_in_chunks(url: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    written = 0
    for i in range(0, len(rows), POST_CHUNK):
        chunk = rows[i : i + POST_CHUNK]
        post_json(url, {"rows": chunk})
        written += len(chunk)
        print(f"[post] {url.rsplit('/', 1)[-1]}: {written}/{len(rows)}", flush=True)
    return written


# ------------------------------------------------------------------
# Small value helpers
# ------------------------------------------------------------------
def _listing_id_from_url(url: str) -> str | None:
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


def _clean_number(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


# ------------------------------------------------------------------
# Scrape
# ------------------------------------------------------------------
def scrape_dataframe() -> pd.DataFrame:
    scraper = dd.CarScraper()
    if SCRAPE_MAKES:
        for make in SCRAPE_MAKES:
            scraper.set_make_model(make=make)

    scraper.scrape(batch_size=500)
    df = scraper.DataFrame
    if df is None or df.empty:
        print("[scrape] no rows returned", flush=True)
        return pd.DataFrame()

    wanted = set(dd.constants.RECOMMENDED_COLUMNS)
    keep = [c for c in df.columns if c in wanted]
    df = df[keep].copy()

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
# Shape rows for the edge functions
# ------------------------------------------------------------------
def df_to_listing_rows(df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for _, r in df.iterrows():
        url = r.get("friendlyUrl")
        lid = _listing_id_from_url(url)
        make = r.get("make")
        model = r.get("model")
        if not (lid and make and model):
            continue
        rows.append(
            {
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
            }
        )
    return rows


# ------------------------------------------------------------------
# Score in-memory
# ------------------------------------------------------------------
def score_listings(listing_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Fit per-(make, model) regressions, return outlier rows + model rows."""
    if not listing_rows:
        return [], []

    df = pd.DataFrame(listing_rows)
    # Drop rows we can't regress
    df = df.dropna(subset=["price_eur"])
    if df.empty:
        return [], []

    features = ["year", "mileage_km", "engine_hp"]
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()

    outlier_rows: list[dict] = []
    model_rows: list[dict] = []
    skipped_thin = 0

    for (make, model), grp in df.groupby(["make", "model"], dropna=True):
        if len(grp) < MIN_GROUP_SIZE:
            skipped_thin += 1
            continue
        work = grp.copy()
        for f in features:
            if f not in work.columns:
                work[f] = np.nan
            median = work[f].median()
            if pd.isna(median):
                median = 0.0
            work[f] = work[f].fillna(median)

        X = work[features].values.astype(float)
        y = work["price_eur"].values.astype(float)

        reg = LinearRegression().fit(X, y)
        pred = reg.predict(X)
        resid = y - pred
        sigma = float(np.std(resid, ddof=1)) if len(resid) > 1 else 0.0
        z = np.zeros_like(resid) if sigma <= 1e-9 else resid / sigma
        pct_below = np.where(pred > 0, (pred - y) / pred, 0.0)
        is_under = (
            (z <= Z_THRESHOLD)
            & (pct_below >= MIN_PCT_BELOW)
            & (pct_below <= MAX_PCT_BELOW)
        )

        for i, row in enumerate(work.itertuples(index=False)):
            outlier_rows.append(
                {
                    "listing_id": str(row.id),
                    "predicted_price_eur": _clean_number(pred[i]),
                    "residual_eur": _clean_number(resid[i]),
                    "z_score": _clean_number(z[i]),
                    "pct_below_market": _clean_number(pct_below[i]),
                    "is_underpriced": bool(is_under[i]),
                    "scored_at": now_iso,
                }
            )

        model_rows.append(
            {
                "make": str(make),
                "model": str(model),
                "coef_year": float(reg.coef_[0]),
                "coef_mileage": float(reg.coef_[1]),
                "coef_hp": float(reg.coef_[2]),
                "intercept": float(reg.intercept_),
                "residual_std": sigma,
                "sample_size": int(len(work)),
                "refreshed_at": now_iso,
            }
        )

    print(
        f"[score] scored {len(outlier_rows)} rows across {len(model_rows)} "
        f"(make,model) groups; skipped {skipped_thin} thin groups",
        flush=True,
    )
    return outlier_rows, model_rows


# ------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------
def main() -> int:
    print("[scrape] fetching listings from DoneDeal...", flush=True)
    df = scrape_dataframe()
    print(f"[scrape] cleaned DataFrame: {len(df)} rows", flush=True)

    listing_rows = df_to_listing_rows(df)
    print(f"[transform] {len(listing_rows)} rows ready for ingest-listings", flush=True)

    post_in_chunks(INGEST_LISTINGS_URL, listing_rows)

    outlier_rows, model_rows = score_listings(listing_rows)
    flagged = sum(1 for r in outlier_rows if r.get("is_underpriced"))

    # The ingest-outliers edge function takes a single combined payload.
    resp = post_json(
        INGEST_OUTLIERS_URL,
        {"rows": outlier_rows, "price_models": model_rows},
    )
    print(f"[post] ingest-outliers response: {json.dumps(resp)[:300]}", flush=True)

    print(
        f"[done] {len(listing_rows)} listings, {len(outlier_rows)} scored, "
        f"{flagged} flagged underpriced, {len(model_rows)} models refreshed",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
