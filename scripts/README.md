# DoneDeal → Lovable Cloud data pipeline

One Python worker feeds your Lovable Cloud database on a 2-hour cron via GitHub Actions. It POSTs to two Lovable Cloud edge functions (not directly to the DB) using a shared API key.

```
scripts/
├── etl_worker.py       # scrape + transform + POST to ingest-listings, score in-memory + POST to ingest-outliers
└── requirements.txt    # worker deps

.github/workflows/etl.yml   # runs the worker on a cron
```

---

## Architecture

Lovable Cloud does not expose raw Supabase `service_role` credentials, so the Python worker can't write to the database directly. Instead the flow is:

```
GitHub Actions (every 2h)
        │
        ▼
scripts/etl_worker.py
  1. scrape DoneDeal → pandas DataFrame
  2. clean + transform → listing rows
  3. POST  ──→  ingest-listings  (edge fn in Lovable Cloud)
                     └─→ upserts listings table
  4. score in-memory (linear regression per make+model)
  5. POST  ──→  ingest-outliers  (edge fn in Lovable Cloud)
                     ├─→ upserts outliers + price_models
                     └─→ calls fan_out_alerts() → notifications
```

Both edge functions are protected with a shared secret via `x-api-key` header.

---

## One-time setup

### 1. Confirm the schema is applied in Lovable Cloud

Open your Lovable project → **Cloud → Database → SQL editor** and run the schema block from `LOVABLE_BUILD_GUIDE.md` (§3). You also need the `fan_out_alerts()` function and a `price_models` table; both are in §5 and §3 of the build guide respectively.

### 2. Confirm the two edge functions are deployed

In Lovable → **Cloud → Edge functions**, you should see:

- `ingest-listings` — accepts `{ "rows": [...] }`, upserts into `listings`
- `ingest-outliers` — accepts `{ "rows": [...], "price_models": [...] }`, upserts into `outliers` and `price_models`, then calls `fan_out_alerts()`

Both should check the `x-api-key` header against the `INGEST_API_KEY` secret stored inside Lovable Cloud.

### 3. Add the shared API key as a Lovable Cloud secret

In Lovable → **Cloud → Secrets**, add:

| Name              | Value                              |
|-------------------|------------------------------------|
| `INGEST_API_KEY`  | a long random string (keep secret) |

The same value will be used on the GitHub side in step 5.

### 4. Grab the two edge function URLs

In Lovable → **Cloud → Edge functions → [function name]**, copy the invocation URL for each. They look like:

```
https://<project-id>.supabase.co/functions/v1/ingest-listings
https://<project-id>.supabase.co/functions/v1/ingest-outliers
```

### 5. Add GitHub secrets

Repo → **Settings → Secrets and variables → Actions → New repository secret**. Add:

| Name                    | Value                                          |
|-------------------------|------------------------------------------------|
| `INGEST_LISTINGS_URL`   | the ingest-listings edge function URL          |
| `INGEST_OUTLIERS_URL`   | the ingest-outliers edge function URL          |
| `INGEST_API_KEY`        | the same value you put in Lovable Cloud above  |

Optionally, under **Variables** (same page, Variables tab), add:

| Name            | Example                                        |
|-----------------|------------------------------------------------|
| `SCRAPE_MAKES`  | `BMW,Mercedes-Benz,Audi,Volkswagen,Toyota`     |

Leave `SCRAPE_MAKES` unset to scrape everything DoneDeal returns on its default search.

### 6. Do a manual test run

Repo → **Actions → DoneDeal ETL → Run workflow**. It takes a few minutes on the first run. Watch the logs — you should see lines like:

```
[scrape] cleaned DataFrame: 847 rows
[transform] 832 rows ready for ingest-listings
[post] ingest-listings: 200/832
[post] ingest-listings: 400/832
[post] ingest-listings: 600/832
[post] ingest-listings: 832/832
[score] scored 721 rows across 38 (make,model) groups; skipped 14 thin groups
[post] ingest-outliers response: {"ok":true,"written":721,"models":38,"alerts_fanned":23}
[done] 832 listings, 721 scored, 23 flagged underpriced, 38 models refreshed
```

### 7. Verify in Lovable

Open your Lovable app's Search page — listings should now appear. Flip to Explore — any underpriced ones from the run will be there. If Alerts is on for your user, you should have a notification in the bell drawer.

---

## Local development (optional)

You can run the worker from your machine the same way GitHub Actions does:

```bash
pip install -e .
pip install -r scripts/requirements.txt

export INGEST_LISTINGS_URL="https://<project>.supabase.co/functions/v1/ingest-listings"
export INGEST_OUTLIERS_URL="https://<project>.supabase.co/functions/v1/ingest-outliers"
export INGEST_API_KEY="…"
export SCRAPE_MAKES="BMW,Mercedes-Benz"   # optional

python scripts/etl_worker.py
```

Useful for faster iteration when debugging the transforms — each run writes to the same DB, which the Lovable preview reflects live.

---

## How the outlier logic works

For each `(make, model)` with at least 30 active listings:

1. Fit `price_eur ~ year + mileage_km + engine_hp` (linear regression).
2. Predict each listing's price.
3. `residual = actual − predicted`, `z = residual / std(residual)`, `pct_below = (predicted − actual) / predicted`.
4. Flag as underpriced when **all three** hold:
   - `z ≤ −1.5` (statistically cheap)
   - `pct_below ≥ 0.15` (at least 15% under predicted — avoids trivially-cheap-but-noisy calls)
   - `pct_below ≤ 0.60` (filters out obvious scam listings)

Tune `MIN_GROUP_SIZE`, `Z_THRESHOLD`, `MIN_PCT_BELOW`, and `MAX_PCT_BELOW` at the top of `etl_worker.py` once you see how the data looks in practice.

---

## Troubleshooting

**"401 Unauthorized" from either edge function** — the `INGEST_API_KEY` secret in GitHub Actions doesn't match the one stored in Lovable Cloud. They must be byte-for-byte identical.

**"404 Not Found" on the edge function URL** — double-check the URLs in GitHub secrets. The edge-function URL is under Cloud → Edge functions → [function] in Lovable, not the project-level URL.

**Listings appear but no outliers** — check the scorer logs. If every group is "skipped thin", you don't have enough volume yet per model. Either lower `MIN_GROUP_SIZE` (not recommended below 20) or widen your scrape.

**Scraper returns 0 rows** — DoneDeal has likely changed its HTML structure, which breaks the regex patterns in `donedeal/constants.py`. Test the scraper locally first:
```
python -c "import donedeal as dd; s = dd.CarScraper(); s.scrape(); print(len(s.DataFrame))"
```

**`fan_out_alerts` RPC not found** — the edge function will log and continue (or fail, depending on how it's written). Paste the function from `LOVABLE_BUILD_GUIDE.md` §5 into your Lovable Cloud SQL editor.

**Changing the schedule** — edit `.github/workflows/etl.yml`. Default is `0 */2 * * *` (every 2 hours on the hour). Common alternatives: `0,30 * * * *` (every 30 min), `*/15 * * * *` (every 15 min). GitHub Actions cron doesn't support non-uniform intervals like `*/90`.

---

## What lives where

- **Frontend** (Lovable project) — reads from `v_listings_scored`, writes to `alert_prefs` and `notifications`. Never writes to `listings` or `outliers`.
- **Edge functions** (Lovable Cloud) — the only things that write to `listings`, `outliers`, `price_models`, and call `fan_out_alerts()`. They're the trust boundary.
- **Worker** (this repo, via GitHub Actions) — scrapes, scores, and POSTs to the edge functions. Knows nothing about the DB schema directly.
- **Users** — only touch `alert_prefs` and `notifications` (their own rows, enforced by RLS).
