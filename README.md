# mp2ph

Stream Mixpanel raw-export JSONL files into PostHog as a historical migration. No
S3 bucket, no AWS account — just point it at your local `.jsonl` (or `.jsonl.gz`)
chunks and it converts each event to PostHog's schema and POSTs to `/batch/`
with `historical_migration: true`.

The transform mirrors PostHog's open-source S3 batch-import worker
([`rust/batch-import-worker/src/parse/content/mixpanel.rs`](https://github.com/PostHog/posthog/blob/master/rust/batch-import-worker/src/parse/content/mixpanel.rs))
line-for-line, so the result is byte-equivalent to what a managed S3 import
would produce.

## Why

If PostHog's managed Mixpanel connector fails for your project, the S3 import
path is the documented fallback — but it requires an S3 bucket, IAM setup, and
moving gigabytes of data around. If you already have your Mixpanel raw export
on disk, this skips both the connector and S3 entirely.

## Features

- **Idempotent re-runs.** Event UUIDs are derived deterministically (UUIDv5)
  from each event's `$insert_id` using PostHog's exact namespace — repeated
  imports of the same source data dedupe automatically because PostHog
  deduplicates on event UUID.
- **Resumable.** Per-file progress is checkpointed to `.mp2ph-state.json` after
  each file's batches succeed; interrupting and re-running picks up where it
  left off.
- **Backoff built in.** 429/5xx responses trigger exponential backoff with
  jitter; 401/403 fail fast.
- **Streams large files.** Reads gzipped JSONL line-by-line, batches up to
  ~5,000 events / ~18 MB per request (PostHog's body cap is 20 MB).
- **Dry-run.** Inspect the first transformed event before sending anything.
- **Stdlib + `requests`.** No other deps.

## Install

```bash
pip install -r requirements.txt
# or, as an installed CLI:
pip install -e .
```

Requires Python 3.9+.

## Quick start

```bash
export POSTHOG_PROJECT_TOKEN=phc_xxx

# Dry-run a single file: prints the first transformed event, no network calls
python -m mp2ph.cli --dry-run path/to/events-2024-05.jsonl.gz

# Import a single file
python -m mp2ph.cli path/to/events-2024-05.jsonl.gz

# Import a globbed set, resumable
python -m mp2ph.cli 'mixpanel-export/events-*.jsonl.gz'

# EU cloud
python -m mp2ph.cli --host eu 'mixpanel-export/events-*.jsonl.gz'

# Custom self-hosted host
python -m mp2ph.cli --host https://posthog.mycompany.io 'events-*.jsonl.gz'
```

## CLI flags

| flag | default | purpose |
| --- | --- | --- |
| `--token` | `$POSTHOG_PROJECT_TOKEN` | PostHog project token |
| `--host` | `us` | `us`, `eu`, or full URL |
| `--state-file` | `.mp2ph-state.json` | per-file resume manifest |
| `--skip-no-distinct-id` | off | drop events with no resolvable `distinct_id` instead of generating a UUID |
| `--timestamp-offset-seconds` | `0` | shift all timestamps (Mixpanel's known offset bug) |
| `--batch-events` | `5000` | max events per `/batch/` request |
| `--batch-bytes` | `18 MB` | max payload bytes per request |
| `--timeout` | `60` | HTTP timeout (seconds) |
| `--dry-run` | off | transform but don't POST; prints first event |

## What the transform does

For every Mixpanel event line, mp2ph:

1. Maps `$mp_web_page_view` → `$pageview`. All other event names are preserved.
2. Picks `distinct_id`:
   - Use `properties.distinct_id` unless it looks anonymous (starts with
     `$device:` or is only uppercase / digits / dashes), in which case fall back
     to `$distinct_id_before_identity`.
   - If neither resolves, generate a UUID (or skip if `--skip-no-distinct-id`).
3. Normalizes `properties.time`: > `10_000_000_000` is treated as milliseconds,
   else seconds. Emits ISO 8601.
4. Computes `event.uuid = uuidv5(NS, $insert_id)` using namespace
   `b"posthog_mixpanel"` (16 bytes), exactly matching PostHog's importer.
5. Renames geo properties:
   - `$city` → `$geoip_city_name`
   - `$region` → `$geoip_subdivision_1_name`
   - `mp_country_code` → `$geoip_country_code`
6. Strips Mixpanel-internal properties: `$mp_api_endpoint`,
   `mp_processing_time_ms`, `$insert_id`, `$geo_source`, `$mp_api_timestamp_ms`.
7. Adds source markers: `historical_migration: true`,
   `analytics_source: "mixpanel"`, `$import_job_id: <uuid>`, plus
   `$geoip_disable: true` (so PostHog doesn't overwrite the migrated geo
   properties using the import server's IP).

The country-name derivation that PostHog's Rust importer does (alpha-2 → long
name) is intentionally omitted; PostHog will populate `$geoip_country_name`
itself from the alpha-2 code on ingest.

## Programmatic use

```python
from mp2ph.transform import transform_event

posthog_event = transform_event(
    {"event": "X", "properties": {"time": 1700000000, "distinct_id": "u", "$insert_id": "evt-1"}},
    job_id="my-job-id",
)
```

## Tests

```bash
python -m pytest tests/ -q
```

## License

MIT
