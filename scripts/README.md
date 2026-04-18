# DoneDeal → Lovable Cloud data pipeline

Two small Python workers feed your Lovable Cloud database on a 90-minute cron via GitHub Actions.

```
scripts/
├── scrape_worker.py   # scrape DoneDeal, upsert → listings
├── score_worker.py    # fit price model per (make,model), upsert → outliers, fan out alerts
└── requirements.txt   # worker-specific deps

.github/workflows/etl.yml   # runs both on a cron
```

---

## One-time setup

### 1. Confirm the schema is applied in Lovable Cloud

Open your Lovable project → **Cloud → Database → SQL editor** and run the schema block from `LOVABLE_BUILD_GUIDE.md` (§3). Lovable Cloud exposes a full Postgres SQL editor — it's the same thing underneath.

You also need the `fan_out_alerts()` function and a `price_models` table; both are in §5 and §3 of the build guide respectively. Paste and run those too.

### 2. Grab the credentials

In Lovable → **Cloud → API / Connect** (label may vary), copy:

- **Project URL** (looks like `https://xyz.supabase.co`)
- **`service_role` key** — NOT the anon key. The service role key bypasses RLS, which is what the workers need.

Keep the service role key secret. Never commit it, never put it in the frontend.

### 3. Push this repo to GitHub

If it isn't already on GitHub, create a repo and push. The workflow file at `.github/workflows/etl.yml` will light up automatically.

### 4. Add GitHub secrets

Repo → **Settings → Secrets and variables → Actions → New repository secret**. Add:

| Name                    | Value                                          |
|-------------------------|------------------------------------------------|
| `SUPABASE_URL`          | your Lovable Cloud project URL                 |
| `SUPABASE_SERVICE_KEY`  | your Lovable Cloud `service_role` key          |

Optionally, under **Variables** (same page, Variables tab), add:

| Name            | Example                                        |
|-----------------|------------------------------------------------|
| `SCRAPE_MAKES`  | `BMW,Mercedes-Benz,Audi,Volkswagen,Toyota`     |

Leave `SCRAPE_MAKES` unset to scrape everything DoneDeal returns on its default search.

### 5. Do a manual test run

Repo → **Actions → DoneDeal ETL → Run workflow**. It'll take a few minutes on the first run. Watch the logs — you want to see lines like:

```
[scrape] cleaned DataFrame has 847 rows
[transform] 832 rows ready for upsert
[upsert] wrote 500/832
[upsert] wrote 832/832
[score] scored 721 rows across 38 (make,model) groups; skipped 14 thin groups
[upsert outliers] 721/721
[done] flagged 23 underpriced listings this run
```

### 6. Verify in Lovable

Open your Lovable app's Search page — listings should now appear. Flip to Explore — any underpriced ones from the run will be there. If Alerts is on for your user, you should have a notification in the bell drawer.

---

## Local development (optional)

You can run the workers from your machine the same way GitHub Actions does:

```bash
pip install -e .
pip install -r scripts/requirements.txt

export SUPABASE_URL="https://xyz.supabase.co"
export SUPABASE_SERVICE_KEY="eyJ..."
export SCRAPE_MAKES="BMW,Mercedes-Benz"   # optional

python scripts/scrape_worker.py
python scripts/score_worker.py
```

Useful for faster iteration when debugging the transforms — each run writes to the same DB, which the Lovable preview will reflect live.

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

Tune `MIN_GROUP_SIZE`, `Z_THRESHOLD`, `MIN_PCT_BELOW`, and `MAX_PCT_BELOW` at the top of `score_worker.py` once you see how the data looks in practice.

---

## Troubleshooting

**"401 Unauthorized" on upsert** — you're using the anon key, not the service role key. The service role key is the longer one labelled `service_role` in your Lovable Cloud API panel.

**Listings appear but no outliers** — check the scorer logs. If every group is "skipped thin", you don't have enough volume yet per model. Either lower `MIN_GROUP_SIZE` (not recommended below 20) or widen your scrape.

**Scraper returns 0 rows** — DoneDeal has likely changed its HTML structure, which breaks the regex patterns in `donedeal/constants.py`. This is rare but inevitable. Test the scraper locally first (`python -c "import donedeal as dd; s = dd.CarScraper(); s.scrape(); print(len(s.DataFrame))"`).

**`fan_out_alerts` RPC not found** — the scorer will log and continue. Paste the function from `LOVABLE_BUILD_GUIDE.md` §5 into your Lovable Cloud SQL editor.

**Workflow runs too often** — the cron is `*/90 * * * *`. Note: standard cron doesn't support `*/90`. GitHub Actions interprets it as "every 90th minute of the hour" which fires at `:00` only. If you want true 90-minute spacing, change the cron to `0 */2 * * *` (every 2 hours) or `0,30 * * * *` (every 30 min) and accept a different cadence.

---

## What lives where

- **Frontend** (Lovable project) — reads from `v_listings_scored`, writes to `alert_prefs` and `notifications`. Never writes to `listings` or `outliers`.
- **Workers** (this repo, via GitHub Actions) — write to `listings`, `outliers`, `price_models`; call `fan_out_alerts()`. Never read notifications.
- **Users** — only touch `alert_prefs` and `notifications` (their own rows, enforced by RLS).
