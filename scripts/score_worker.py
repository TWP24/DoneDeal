"""
score_worker.py

1. Fetches every active listing from Supabase.
2. For each (make, model) with >= MIN_GROUP_SIZE listings, fits a simple
   linear regression: price_eur ~ year + mileage_km + engine_hp.
3. Upserts predictions, residuals, z-scores, and a boolean "is_underpriced"
   flag into the `outliers` table.
4. Upserts model coefficients into `price_models`.
5. Calls the `fan_out_alerts` Postgres RPC so newly-flagged outliers
   trigger in-app notifications for users with matching alert prefs.

Env vars required:
    SUPABASE_URL
    SUPABASE_SERVICE_KEY
"""

from __future__ import annotations

import os
import sys
import math
import datetime as dt

import numpy as np
import pandas as pd
from supabase import create_client, Client
from sklearn.linear_model import LinearRegression


# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

MIN_GROUP_SIZE = 30          # skip thin cohorts
Z_THRESHOLD = -1.5           # negative residual = cheaper than predicted
MIN_PCT_BELOW = 0.15         # at least 15% below predicted price
MAX_PCT_BELOW = 0.60         # cap the "too good to be true" scam band

FETCH_PAGE = 1000
UPSERT_CHUNK = 500


# ------------------------------------------------------------------
# Data fetch
# ------------------------------------------------------------------
def fetch_active_listings(supabase: Client) -> pd.DataFrame:
    """Page through listings table because Supabase caps rows per request."""
    rows: list[dict] = []
    start = 0
    while True:
        end = start + FETCH_PAGE - 1
        resp = (
            supabase.table("listings")
            .select(
                "id,make,model,year,mileage_km,engine_hp,price_eur,is_active"
            )
            .eq("is_active", True)
            .range(start, end)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < FETCH_PAGE:
            break
        start += FETCH_PAGE

    df = pd.DataFrame(rows)
    print(f"[fetch] {len(df)} active listings", flush=True)
    return df


# ------------------------------------------------------------------
# Fit + score
# ------------------------------------------------------------------
def score_group(grp: pd.DataFrame) -> tuple[pd.DataFrame, dict | None]:
    """
    Fit a linear regression on (year, mileage_km, engine_hp) to predict
    price_eur. Return per-row score frame + model coefficients.
    """
    features = ["year", "mileage_km", "engine_hp"]
    work = grp.copy()

    # Require price_eur. For features, impute median of the cohort so
    # thin/null features don't drop the row entirely.
    work = work.dropna(subset=["price_eur"])
    if len(work) < MIN_GROUP_SIZE:
        return pd.DataFrame(), None

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

    # Avoid divide-by-zero in the z-score and the pct calc
    if sigma <= 1e-9:
        z = np.zeros_like(resid)
    else:
        z = resid / sigma

    pct_below = np.where(pred > 0, (pred - y) / pred, 0.0)

    is_under = (
        (z <= Z_THRESHOLD)
        & (pct_below >= MIN_PCT_BELOW)
        & (pct_below <= MAX_PCT_BELOW)
    )

    score_frame = pd.DataFrame(
        {
            "listing_id": work["id"].values,
            "predicted_price_eur": pred,
            "residual_eur": resid,
            "z_score": z,
            "pct_below_market": pct_below,
            "is_underpriced": is_under,
        }
    )

    model_row = {
        "make": str(work["make"].iloc[0]),
        "model": str(work["model"].iloc[0]),
        "coef_year": float(reg.coef_[0]),
        "coef_mileage": float(reg.coef_[1]),
        "coef_hp": float(reg.coef_[2]),
        "intercept": float(reg.intercept_),
        "residual_std": sigma,
        "sample_size": int(len(work)),
    }
    return score_frame, model_row


def score_all(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    if df.empty:
        return pd.DataFrame(), []

    all_scores: list[pd.DataFrame] = []
    all_models: list[dict] = []
    skipped = 0

    for (make, model), grp in df.groupby(["make", "model"], dropna=True):
        if len(grp) < MIN_GROUP_SIZE:
            skipped += 1
            continue
        scores, model_row = score_group(grp)
        if model_row is None:
            skipped += 1
            continue
        all_scores.append(scores)
        all_models.append(model_row)

    scored = pd.concat(all_scores, ignore_index=True) if all_scores else pd.DataFrame()
    print(
        f"[score] scored {len(scored)} rows across {len(all_models)} (make,model) "
        f"groups; skipped {skipped} thin groups",
        flush=True,
    )
    return scored, all_models


# ------------------------------------------------------------------
# Upsert
# ------------------------------------------------------------------
def _clean_number(v):
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def upsert_outliers(supabase: Client, scored: pd.DataFrame) -> int:
    if scored.empty:
        return 0
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()

    rows = []
    for _, r in scored.iterrows():
        rows.append(
            {
                "listing_id": str(r["listing_id"]),
                "predicted_price_eur": _clean_number(r["predicted_price_eur"]),
                "residual_eur": _clean_number(r["residual_eur"]),
                "z_score": _clean_number(r["z_score"]),
                "pct_below_market": _clean_number(r["pct_below_market"]),
                "is_underpriced": bool(r["is_underpriced"]),
                "scored_at": now_iso,
            }
        )

    written = 0
    for i in range(0, len(rows), UPSERT_CHUNK):
        chunk = rows[i : i + UPSERT_CHUNK]
        supabase.table("outliers").upsert(chunk, on_conflict="listing_id").execute()
        written += len(chunk)
        print(f"[upsert outliers] {written}/{len(rows)}", flush=True)
    return written


def upsert_price_models(supabase: Client, models: list[dict]) -> int:
    if not models:
        return 0
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    for m in models:
        m["refreshed_at"] = now_iso
    supabase.table("price_models").upsert(
        models, on_conflict="make,model"
    ).execute()
    print(f"[upsert price_models] {len(models)} rows", flush=True)
    return len(models)


def fan_out_alerts(supabase: Client) -> None:
    try:
        supabase.rpc("fan_out_alerts").execute()
        print("[fan_out] ok", flush=True)
    except Exception as e:
        # Don't fail the whole job if fan-out isn't deployed yet
        print(f"[fan_out] skipped: {e}", flush=True)


# ------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------
def main() -> int:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    df = fetch_active_listings(supabase)
    if df.empty:
        print("[done] nothing to score", flush=True)
        return 0

    scored, models = score_all(df)
    upsert_outliers(supabase, scored)
    upsert_price_models(supabase, models)
    fan_out_alerts(supabase)

    flagged = int(scored["is_underpriced"].sum()) if not scored.empty else 0
    print(f"[done] flagged {flagged} underpriced listings this run", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
