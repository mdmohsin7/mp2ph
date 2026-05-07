"""Stream Mixpanel JSONL files into PostHog's /batch/ endpoint."""

from __future__ import annotations

import gzip
import io
import json
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

import requests

from .transform import transform_event

DEFAULT_BATCH_EVENTS = 5000
DEFAULT_BATCH_BYTES = 18 * 1024 * 1024  # PostHog cap is 20MB; leave headroom
DEFAULT_TIMEOUT_S = 60
MAX_RETRIES = 8


@dataclass
class ImportStats:
    files_done: int = 0
    files_total: int = 0
    events_sent: int = 0
    events_skipped: int = 0
    batches_sent: int = 0
    bytes_sent: int = 0
    started_at: float = field(default_factory=time.monotonic)

    def elapsed(self) -> float:
        return time.monotonic() - self.started_at


def open_jsonl(path: Path) -> io.TextIOBase:
    if str(path).endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8")  # type: ignore[arg-type]
    return open(path, "r", encoding="utf-8")


def iter_mixpanel_events(path: Path) -> Iterator[dict[str, Any]]:
    with open_jsonl(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def iter_batches(
    events: Iterable[dict[str, Any]],
    *,
    max_events: int,
    max_bytes: int,
) -> Iterator[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    batch_size = 0
    for ev in events:
        encoded_size = len(json.dumps(ev, separators=(",", ":")).encode("utf-8")) + 1
        if batch and (len(batch) >= max_events or batch_size + encoded_size > max_bytes):
            yield batch
            batch = []
            batch_size = 0
        batch.append(ev)
        batch_size += encoded_size
    if batch:
        yield batch


def _post_with_retry(url: str, payload: dict[str, Any], *, timeout_s: int, log) -> None:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    backoff = 1.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                url,
                data=body,
                headers={"Content-Type": "application/json"},
                timeout=timeout_s,
            )
        except requests.RequestException as e:
            log(f"  network error (attempt {attempt}): {e}; sleeping {backoff:.1f}s")
            time.sleep(backoff + random.uniform(0, 0.5))
            backoff = min(backoff * 2, 60)
            continue

        if 200 <= resp.status_code < 300:
            return

        if resp.status_code in (401, 403):
            raise RuntimeError(f"PostHog rejected auth ({resp.status_code}); check the project token.")

        if resp.status_code == 429 or resp.status_code >= 500:
            sleep_for = backoff + random.uniform(0, 0.5)
            log(f"  HTTP {resp.status_code} (attempt {attempt}); sleeping {sleep_for:.1f}s")
            time.sleep(sleep_for)
            backoff = min(backoff * 2, 60)
            continue

        snippet = resp.text[:300] if resp.text else ""
        raise RuntimeError(f"PostHog returned {resp.status_code}: {snippet}")

    raise RuntimeError(f"giving up after {MAX_RETRIES} retries posting to {url}")


def load_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {"completed_files": [], "job_id": str(uuid.uuid4())}
    with state_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def save_state(state_path: Path, state: dict[str, Any]) -> None:
    tmp = state_path.with_suffix(state_path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)
    os.replace(tmp, state_path)


def import_files(
    files: list[Path],
    *,
    api_key: str,
    host: str,
    state_path: Path,
    skip_no_distinct_id: bool = False,
    timestamp_offset_seconds: int = 0,
    max_events_per_batch: int = DEFAULT_BATCH_EVENTS,
    max_bytes_per_batch: int = DEFAULT_BATCH_BYTES,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    dry_run: bool = False,
    log=print,
) -> ImportStats:
    """Stream a list of JSONL/JSONL.gz files into PostHog. Resumable per-file."""
    stats = ImportStats(files_total=len(files))
    state = load_state(state_path)
    completed = set(state.get("completed_files", []))
    job_id = state.get("job_id") or str(uuid.uuid4())
    state["job_id"] = job_id
    save_state(state_path, state)

    url = f"{host.rstrip('/')}/batch/"

    for file_path in files:
        key = str(file_path.resolve())
        if key in completed:
            log(f"[skip] {file_path} (already completed)")
            stats.files_done += 1
            continue

        log(f"[file] {file_path}")
        events_for_file = 0
        skipped_for_file = 0

        def transformed() -> Iterator[dict[str, Any]]:
            nonlocal events_for_file, skipped_for_file
            for raw in iter_mixpanel_events(file_path):
                try:
                    ev = transform_event(
                        raw,
                        job_id=job_id,
                        skip_no_distinct_id=skip_no_distinct_id,
                        timestamp_offset_seconds=timestamp_offset_seconds,
                    )
                except ValueError:
                    skipped_for_file += 1
                    continue
                if ev is None:
                    skipped_for_file += 1
                    continue
                events_for_file += 1
                yield ev

        for batch in iter_batches(
            transformed(),
            max_events=max_events_per_batch,
            max_bytes=max_bytes_per_batch,
        ):
            payload = {
                "api_key": api_key,
                "historical_migration": True,
                "batch": batch,
            }
            if dry_run:
                stats.batches_sent += 1
                stats.events_sent += len(batch)
                if stats.batches_sent == 1:
                    log("  [dry-run] first transformed event:")
                    log("  " + json.dumps(batch[0], indent=2, sort_keys=True).replace("\n", "\n  "))
            else:
                _post_with_retry(url, payload, timeout_s=timeout_s, log=log)
                stats.batches_sent += 1
                stats.events_sent += len(batch)
                stats.bytes_sent += len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))

        stats.events_skipped += skipped_for_file
        completed.add(key)
        state["completed_files"] = sorted(completed)
        save_state(state_path, state)
        stats.files_done += 1
        log(
            f"[done] {file_path.name}: {events_for_file} events, "
            f"{skipped_for_file} skipped, total {stats.events_sent} sent in {stats.elapsed():.1f}s"
        )

    return stats
