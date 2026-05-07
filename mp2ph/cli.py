"""mp2ph CLI: stream Mixpanel JSONL exports into PostHog."""

from __future__ import annotations

import argparse
import os
import sys
from glob import glob
from pathlib import Path

from .importer import (
    DEFAULT_BATCH_BYTES,
    DEFAULT_BATCH_EVENTS,
    DEFAULT_TIMEOUT_S,
    import_files,
)

HOST_ALIASES = {
    "us": "https://us.i.posthog.com",
    "eu": "https://eu.i.posthog.com",
}


def _resolve_host(host: str) -> str:
    return HOST_ALIASES.get(host.lower(), host)


def _expand_inputs(patterns: list[str]) -> list[Path]:
    files: list[Path] = []
    for pat in patterns:
        if any(ch in pat for ch in "*?["):
            matches = sorted(glob(pat))
            if not matches:
                print(f"warning: no files matched {pat!r}", file=sys.stderr)
            files.extend(Path(m) for m in matches)
        else:
            files.append(Path(pat))
    return files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="mp2ph",
        description="Stream Mixpanel raw-export JSONL files into PostHog as a historical migration.",
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help="JSONL or JSONL.gz files (globs supported, e.g. 'events-*.jsonl.gz')",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("POSTHOG_PROJECT_TOKEN"),
        help="PostHog project token (or POSTHOG_PROJECT_TOKEN env var).",
    )
    parser.add_argument(
        "--host",
        default="us",
        help="PostHog host: 'us', 'eu', or a full URL (default: us).",
    )
    parser.add_argument(
        "--state-file",
        default=".mp2ph-state.json",
        help="Path to JSON manifest for resume tracking (default: .mp2ph-state.json).",
    )
    parser.add_argument(
        "--skip-no-distinct-id",
        action="store_true",
        help="Drop events with no resolvable distinct_id instead of generating a UUID.",
    )
    parser.add_argument(
        "--timestamp-offset-seconds",
        type=int,
        default=0,
        help="Add this many seconds to every event timestamp (workaround for known Mixpanel offset bugs).",
    )
    parser.add_argument(
        "--batch-events",
        type=int,
        default=DEFAULT_BATCH_EVENTS,
        help=f"Max events per /batch/ request (default: {DEFAULT_BATCH_EVENTS}).",
    )
    parser.add_argument(
        "--batch-bytes",
        type=int,
        default=DEFAULT_BATCH_BYTES,
        help=f"Max payload bytes per /batch/ request (default: {DEFAULT_BATCH_BYTES}; PostHog cap is 20MB).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_S,
        help=f"HTTP timeout in seconds (default: {DEFAULT_TIMEOUT_S}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Transform and batch but don't POST to PostHog. Prints the first transformed event for inspection.",
    )

    args = parser.parse_args(argv)

    if not args.dry_run and not args.token:
        parser.error("--token is required (or set POSTHOG_PROJECT_TOKEN), unless --dry-run is set.")

    files = _expand_inputs(args.inputs)
    if not files:
        parser.error("no input files matched.")

    state_path = Path(args.state_file)
    host = _resolve_host(args.host)

    stats = import_files(
        files,
        api_key=args.token or "DRY_RUN",
        host=host,
        state_path=state_path,
        skip_no_distinct_id=args.skip_no_distinct_id,
        timestamp_offset_seconds=args.timestamp_offset_seconds,
        max_events_per_batch=args.batch_events,
        max_bytes_per_batch=args.batch_bytes,
        timeout_s=args.timeout,
        dry_run=args.dry_run,
    )

    print(
        f"\ndone: {stats.files_done}/{stats.files_total} files, "
        f"{stats.events_sent} events sent ({stats.events_skipped} skipped) "
        f"in {stats.batches_sent} batches over {stats.elapsed():.1f}s"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
