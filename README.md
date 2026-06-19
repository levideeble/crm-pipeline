# CRM Campaign Pipeline

A PySpark batch pipeline (GCP Edition) that processes daily CRM vendor API data and produces two reports: a **Campaign Overview** report and a **Current Campaign Engagement** report.

Built as a take-home data engineering assessment, treated as a production system per the brief — including TDD, an incremental state design, and a self-verifying integration test.

## Overview

Each day, two JSON files land in a GCS bucket: a list of active campaigns, and a log of user engagement events (message delivered / opened / failed). This pipeline:

- Parses both files
- Tracks campaign metadata over time, since campaigns can have steps **added** (but not removed) while live
- Maintains cumulative per-user engagement state across daily runs
- Calculates each user's % completion against the campaign as it existed **at the time of their last activity** — not today's version of the campaign
- Outputs two CSV reports: Campaign Overview, and Campaign Engagement (ranked by average completion)

## Architecture

```
gs://<bucket>/
├── input/daily_files/        # Vendor's daily campaign + engagement JSON files
├── internal/
│   ├── campaign_snapshot/    # Daily snapshot of campaign metadata (Parquet)
│   └── user_state/           # Cumulative per-user/campaign engagement state (Parquet)
└── output/reports/
    ├── campaign_overview/
    └── current_campaign_engagement_report/
```

The job runs once per day. On each run it:
1. Reads today's campaigns file, appends a dated snapshot to `campaign_snapshot/`
2. Reads today's engagement file, deduplicates and cleans it, merges new activity into `user_state/` (open counts come from `MESSAGE_OPENED` events only; last activity date considers both delivered and opened events, so inactive users still appear in the report rather than disappearing)
3. Joins the updated user state against the campaign snapshot matching each user's **last activity date** (not today's date), so step additions don't retroactively affect users who completed a campaign before the change
4. Calculates completion % and writes both reports

## Key Design Decisions

- **Temporal snapshot matching** — the SLA states campaigns can gain steps while live. Naively joining against today's campaign data for everyone would retroactively penalise users who already completed a campaign before a step was added — e.g. a user who finished all 3 steps of a campaign would incorrectly show as only 75% complete (3/4) once a 4th step is added, even though nothing changed about their actual engagement. To avoid this, a snapshot of campaign metadata is taken daily, and each user is matched against the snapshot from their own last activity date, not today's. This is the most significant design choice in the pipeline, verified by the integration test.
- **Incremental user state** — rather than reprocessing all historical engagement events daily, a cumulative state table (`open_count`, `last_event_date` per user/campaign) is updated incrementally each run via a full outer join with null-safe merging.
- **Broadcast joins** — campaign data (capped at 256 campaigns per the SLA) is broadcast in all joins against user-level data, avoiding expensive shuffles at scale.
- **Step completion = `MESSAGE_OPENED`** — given this is a streaming company's marketing/advertising campaign, a message being *delivered* only confirms the platform served it; a message being *opened* is what actually signals the user engaged with it. A step is therefore only counted as completed on open, not merely delivery.
- **TDD throughout** — all transform logic was built test-first, with deliberately chosen edge cases (zero-activity users, out-of-order dates, tied rankings) rather than only happy-path data.

## Assumptions

- The daily **engagement** file contains only new events since the previous run, not a rolling window or full historical backfill. The assessment's example engagement API response shows events spanning multiple dates within a single response, which could be read as the API always returning the full event history. Given the brief describes a **daily** scrape producing **daily** files, this was assumed to be illustrative of the response shape rather than a literal "every file contains all history" requirement — i.e. each day's file is assumed to contain that day's new events only. This assumption directly underpins the incremental `user_state` design (see Known Limitations for the reprocessing implication if this assumption doesn't hold).
- The assessment states a filename format (`supplier_file_YYYYMMDDHHmmSS.json`) but the worked example uses a different pattern (`crm_campaign_20230101001500.json`) — the actual vendor filenames were never clearly confirmed. To stay robust to either, filenames are matched with a `*campaign*<date>*.json` / `*engagement*<date>*.json` wildcard pattern rather than a hardcoded prefix
- API responses are JSON arrays (`multiline` JSON), matching the assessment's example rather than JSON Lines format

## Known Limitations

- Rerunning the pipeline for a date that has already been processed will double-count engagement data, since `open_count` gets added again on top of the already-merged total — the design assumes each date is processed exactly once. In practice this incorrectly inflates `average_percent_completion`, which can exceed 100% (this was actually encountered and diagnosed during testing). Production-hardening would add idempotency/replay protection.
- If the vendor's **engagement** file is missing entirely for a given day, or is present but empty (zero events), the job will fail rather than gracefully proceeding — the engagement file must contain at least one event for the read/schema inference to succeed.
- `campaign_snapshot/` writes a row for **every** campaign on **every** daily run, even on days where that campaign's metadata didn't change at all. This was a deliberate trade-off: it keeps the join in `join_user_state_with_campaign_snapshot` simple (an exact match on `last_event_date == snapshot_date`), rather than needing more complex logic to find the most recent snapshot *before* a given date. The cost is storing some duplicate, unchanged rows daily — negligible at this scale (max 256 campaigns × 365 days ≈ 93K rows/year), but a production version at larger scale could instead only write a new row when a campaign actually changes, and "carry forward" the last known value for unchanged days when reading.

*(See inline code docstrings for the full set of documented decisions, edge cases, and future improvements.)*

## How to Run

**Requirements:** Java 17, Python 3.11

```bash
# Set up environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run unit tests (24 tests covering all transform logic)
python -m pytest tests/ -v

# Run the end-to-end integration test
# (simulates 2 days of pipeline runs, verifies a campaign step addition
# does not retroactively affect a user's completion %)
python integration_test/run_integration_test.py 2>/dev/null
```

To run the actual job (against a real GCS bucket):
```bash
python jobs/daily_campaign_job.py --bucket <your-bucket> --date YYYYMMDD
```

## Project Structure

```
crm_pipeline/
├── config/              # Config class: bucket paths, date, file patterns
├── transformations/     # Core PySpark transform logic (unit tested)
├── jobs/                # Orchestration: Spark session, I/O, pipeline flow
├── tests/                # pytest unit tests + fixtures
└── integration_test/     # Self-contained end-to-end test with mock data
```
