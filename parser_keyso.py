#!/usr/bin/env python3
"""
Keys.so batch domain parser — true pipeline mode.

Architecture:
  Every BATCH_INTERVAL seconds: POST the next batch → rid goes into in_flight queue.
  Simultaneously: collect any in_flight rid whose age >= INITIAL_WAIT.
  No per-wave waiting — all batches overlap on the server.

  Expected throughput: ~30 min for 35k domains (vs ~77 min in wave mode).

  Completion check → find domains not in output at all; retry via pipeline
  Error retry     → strip error rows from output; re-process those domains
  Final check     → one more completion check after error retry
"""

import argparse
import csv
import logging
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# ← TUNE these constants to match Keys.so server behaviour
# ──────────────────────────────────────────────────────────────────────────────
API_BASE = "https://api.keys.so"

INITIAL_WAIT = 90       # ← TUNE: seconds from POST until report is ready.
                        # From testing: ~60-90s. Raise if you see many 202 retries.

BATCH_INTERVAL = 5      # seconds between successive POST requests.
                        # Keys.so limit: 10 req/10s. At 5s: 2/10s — safe margin ×5.
                        # In steady state: 1 POST + 1 GET per 5s = 4/10s total.

POST_TIMEOUT = 90       # seconds — POST /report/group can be slow
GET_TIMEOUT  = 30       # seconds — GET /report/group/domains is usually fast
POST_RETRY_WAIT = 20    # seconds before retrying a failed POST

NOT_READY_RETRY_WAIT    = 60    # seconds between GET retries when server returns 202
NOT_READY_TOTAL_TIMEOUT = 60    # ← TUNE: max total seconds to wait per 202 batch
                                #          re-uses same rid — no re-POST needed

COMPLETION_ROUNDS = 3   # max completion-check rounds

# Batches smaller than this use the simple endpoint (one GET per domain) instead of
# POST /report/group, which requires 2+ domains and has a long server-side wait.
# Also: batch_size=200 returns HTTP 410 — keep batch_size <= 100.
MIN_BATCH_FOR_GROUP = 10

# ──────────────────────────────────────────────────────────────────────────────
# Output columns
# ──────────────────────────────────────────────────────────────────────────────
KEYSO_METRICS = ["it1", "it3", "it5", "it10", "it50", "vis"]
SERVICE_COLS  = ["http_status", "error"]
SUFFIX = " - Keyso"

KEYSO_COL_LABELS: dict[str, str] = {
    "it1":         "Запросов в Топ-1",
    "it3":         "Запросов в Топ-3",
    "it5":         "Запросов в Топ-5",
    "it10":        "Запросов в Топ-10",
    "it50":        "Запросов в Топ-50",
    "vis":         "Трафик",
    "http_status": "HTTP статус",
    "error":       "Ошибка",
}


def _col(name: str) -> str:
    """Map API field name to localized CSV column name (with suffix)."""
    return f"{KEYSO_COL_LABELS.get(name, name)}{SUFFIX}"


# Sentinel: server says "not ready yet" (code=202 in JSON body)
_NOT_READY = object()
# Sentinel: POST returned 400 "more than one domain required" — route batch to simple fallback
_USE_FALLBACK = object()


def _fmt_elapsed(sec: float) -> str:
    """Format seconds into 'N мин SS сек'."""
    m = int(sec) // 60
    s = int(sec) % 60
    return f"{m} мин {s:02d} сек"


# ══════════════════════════════════════════════════════════════════════════════
# HTTP session
# ══════════════════════════════════════════════════════════════════════════════
def make_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"X-Keyso-TOKEN": token, "Accept": "application/json"})
    return s


# ══════════════════════════════════════════════════════════════════════════════
# API calls
# ══════════════════════════════════════════════════════════════════════════════
def post_batch(
    session: requests.Session,
    domains: list[str],
    base: str,
) -> str | None:
    """
    POST /report/group → rid.
    One automatic retry after POST_RETRY_WAIT on failure.
    Returns rid string or None (batch will be caught by completion check).
    """
    url = f"{API_BASE}/report/group"
    for attempt in range(1, 3):   # 2 attempts total
        try:
            r = session.post(
                url,
                json={"base": base, "top": 50, "domains": domains},
                timeout=POST_TIMEOUT,
            )
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", POST_RETRY_WAIT))
                log.warning("POST 429 → sleeping %ds (attempt %d)", wait, attempt)
                time.sleep(wait)
                continue
            if not r.ok:
                log.error("POST HTTP %d (attempt %d): %s",
                          r.status_code, attempt, r.text[:400])
                # 400 "более одного домена" — API rejects batch (domains not in index).
                # No point retrying POST; fall back to simple endpoint immediately.
                if r.status_code == 400 and "одного домена" in r.text:
                    return _USE_FALLBACK
                if attempt == 1:
                    time.sleep(POST_RETRY_WAIT)
                continue
            rid = r.json().get("rid")
            if rid:
                return rid
            log.error("POST: no rid in response: %s", r.text[:300])
        except Exception as exc:
            log.error("POST failed (attempt %d): %s", attempt, exc)
            if attempt == 1:
                time.sleep(POST_RETRY_WAIT)
    return None


def get_single_domain_metrics(
    session: requests.Session,
    base: str,
    domain: str,
) -> dict | None:
    """
    GET /report/simple/domain_dashboard — single domain fallback.
    Returns dict with it1..vis at root level, or None on error / not found.
    Used for batches smaller than MIN_BATCH_FOR_GROUP.
    """
    url = f"{API_BASE}/report/simple/domain_dashboard"
    try:
        r = session.get(
            url, params={"base": base, "domain": domain}, timeout=GET_TIMEOUT
        )
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, dict):
            return None
        # Server may return {"code": 202/4xx, "message": "..."} as HTTP 200
        code = data.get("code")
        if code:
            if str(code) == "202":
                log.warning("simple domain=%s: 202 not ready yet — %s",
                            domain, data.get("message", "")[:80])
                return _NOT_READY
            if str(code) != "200":
                log.warning("simple domain=%s: code=%s msg=%s",
                            domain, code, data.get("message", "")[:80])
                return None
        if any(k in data for k in KEYSO_METRICS):
            return data
        return None
    except Exception as exc:
        log.error("simple domain=%s failed: %s", domain, exc)
        return None


def get_batch_domains(session: requests.Session, rid: str):
    """
    GET /report/group/domains/{rid} with pagination.

    Returns:
      list[dict]  — domain metrics (may be empty list if none found)
      _NOT_READY  — server returned code=202 "not ready yet" in JSON body
    """
    url = f"{API_BASE}/report/group/domains/{rid}"
    try:
        r = session.get(url, params={"per_page": 500, "page": 1}, timeout=GET_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        # Keys.so returns HTTP 200 but with {"code": 202, "message": "..."} when not ready
        if isinstance(data, dict) and data.get("code") == 202:
            log.warning("rid=%s not ready: %s", rid, data.get("message", "")[:120])
            return _NOT_READY

        if isinstance(data, list):
            return data

        items = data.get("data")
        if not isinstance(items, list):
            log.warning("Unexpected domains response rid=%s: %s", rid, str(data)[:200])
            return []

        all_items: list[dict] = list(items)
        total    = data.get("total", len(all_items))
        per_page = data.get("per_page") or 500
        pages    = max(1, (total + per_page - 1) // per_page)

        for page in range(2, pages + 1):
            rp = session.get(
                url, params={"per_page": per_page, "page": page}, timeout=GET_TIMEOUT
            )
            rp.raise_for_status()
            dp = rp.json()
            extra = dp.get("data") if isinstance(dp, dict) else dp
            if isinstance(extra, list):
                all_items.extend(extra)

        return all_items

    except Exception as exc:
        log.error("GET domains rid=%s failed: %s", rid, exc)
        return []


# ══════════════════════════════════════════════════════════════════════════════
# CSV helpers
# ══════════════════════════════════════════════════════════════════════════════
def find_col(fieldnames: list[str], name: str) -> str | None:
    target = name.strip().lower()
    return next((f for f in fieldnames if f.strip().lower() == target), None)


def read_done_set(output_path: Path, domain_col: str) -> set[str]:
    """Lowercase set of all domain names already written to output CSV."""
    if not output_path.exists():
        return set()
    done: set[str] = set()
    try:
        with open(output_path, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                d = (row.get(domain_col) or "").strip().lower()
                if d:
                    done.add(d)
    except Exception as exc:
        log.warning("Could not read output for dedup: %s", exc)
    return done


def ensure_header(output_path: Path, headers: list[str]) -> None:
    if output_path.exists() and output_path.stat().st_size > 0:
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        csv.DictWriter(f, fieldnames=headers, extrasaction="ignore").writeheader()


def flush_rows(output_path: Path, rows: list[dict], headers: list[str]) -> None:
    with open(output_path, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        for row in rows:
            w.writerow(row)
        f.flush()


def _make_error_row(orig: dict, error_msg: str, http_status: str = "") -> dict:
    row = dict(orig)
    for col in KEYSO_METRICS:
        row[_col(col)] = ""
    row[_col("http_status")] = http_status
    row[_col("error")]       = error_msg
    return row


def _make_result_row(orig: dict, item: dict | None, domain: str) -> dict:
    row = dict(orig)
    if item:
        for col in KEYSO_METRICS:
            row[_col(col)] = item.get(col, "")
        row[_col("http_status")] = 200
        row[_col("error")]       = ""
    else:
        for col in KEYSO_METRICS:
            row[_col(col)] = ""
        row[_col("http_status")] = ""
        row[_col("error")]       = "not found in report"
    return row


# ══════════════════════════════════════════════════════════════════════════════
# Collect helpers (used by pipeline)
# ══════════════════════════════════════════════════════════════════════════════
Batch = tuple[list[str], list[dict]]   # (lowercase_domains, original_csv_rows)


def _collect_one(
    session: requests.Session,
    rid: str,
    batch_domains: list[str],
    batch_rows: list[dict],
    output_path: Path,
    all_headers: list[str],
) -> bool:
    """
    GET one rid and write results to output.
    Returns True on success, False if server returned _NOT_READY.
    If GET fails for other reasons, writes "not found in report" rows (treated as done).
    """
    api_items = get_batch_domains(session, rid)

    if api_items is _NOT_READY:
        return False   # caller must retry

    lookup: dict[str, dict] = {}
    if isinstance(api_items, list):
        for item in api_items:
            name = (item.get("name") or item.get("domain") or "").strip().lower()
            if name:
                lookup[name] = item

    result_rows = [
        _make_result_row(orig, lookup.get(domain), domain)
        for orig, domain in zip(batch_rows, batch_domains)
    ]
    flush_rows(output_path, result_rows, all_headers)

    found = sum(1 for r in result_rows if not r.get(_col("error")))
    log.info("  rid=%s → %d found / %d not found", rid, found, len(result_rows) - found)
    return True


def _process_fallback_batch(
    session: requests.Session,
    batch_domains: list[str],
    batch_rows: list[dict],
    base: str,
    output_path: Path,
    all_headers: list[str],
) -> None:
    """
    Small-batch fallback: one GET /report/simple/domain_dashboard per domain.
    Single pass — no inline 202 retry (each request hangs 10-40s on a busy server,
    retrying N domains sequentially would block for many minutes).
    Domains returning 202 are written as "not found in report";
    run_error_retry treats these as valid final results and will not re-process them.
    """
    log.info("  Fallback (simple endpoint): %d domains", len(batch_domains))
    result_rows = []
    for domain, orig_row in zip(batch_domains, batch_rows):
        data = get_single_domain_metrics(session, base, domain)
        if data is _NOT_READY:
            data = None   # 202: server computing — treat as not found, fast exit
        result_rows.append(_make_result_row(orig_row, data, domain))
    flush_rows(output_path, result_rows, all_headers)
    found = sum(1 for r in result_rows if not r.get(_col("error")))
    log.info("  Fallback done: %d found / %d not found",
             found, len(result_rows) - found)


# ══════════════════════════════════════════════════════════════════════════════
# True pipeline: fire continuously, collect as results become ready
# ══════════════════════════════════════════════════════════════════════════════
def run_pipeline(
    session: requests.Session,
    pairs: list[tuple[str, dict]],
    batch_size: int,
    base: str,
    output_path: Path,
    input_fieldnames: list[str],
    all_headers: list[str],
    label: str = "",
) -> None:
    """
    Continuously fire POSTs every BATCH_INTERVAL seconds.
    Collect each rid once INITIAL_WAIT seconds have passed since its POST.
    Fire and collect happen in the same loop — no per-wave idle wait.

    in_flight  — list of [fire_monotonic, rid, domains, rows]
    pending_retry — dict rid → [first_not_ready_t, last_try_t, domains, rows]
                    re-used when server returns {"code": 202}
    """
    all_batches: list[tuple[list[str], list[dict]]] = [
        (
            [d for d, _ in pairs[i : i + batch_size]],
            [r for _, r in pairs[i : i + batch_size]],
        )
        for i in range(0, len(pairs), batch_size)
    ]
    total  = len(all_batches)
    pfx    = f"[{label}] " if label else ""
    log.info("%sPipeline start: %d domains → %d batches (batch_size=%d, interval=%ds)",
             pfx, len(pairs), total, batch_size, BATCH_INTERVAL)

    in_flight:     list[list]       = []   # [fire_t, rid, domains, rows]
    pending_retry: dict[str, list]  = {}   # rid → [first_t, last_try_t, domains, rows]

    batch_idx      = 0
    last_fire_t    = time.monotonic() - BATCH_INTERVAL  # fire immediately on first iteration
    fired_n        = 0
    collected_n    = 0
    done_n         = 0   # collected_n + fallback batches (for % calculation)
    pipeline_start = time.monotonic()
    last_progress_t = pipeline_start  # for periodic idle progress

    def _log_progress(extra: str = "") -> None:
        """Log current pipeline progress: %, ETA, in_flight count."""
        pct     = done_n / total * 100 if total else 0
        elapsed = time.monotonic() - pipeline_start
        if done_n > 0 and elapsed > 1:
            rate        = done_n / elapsed          # batches per second
            remaining_s = (total - done_n) / rate
            eta         = f"ETA ~{_fmt_elapsed(remaining_s)}"
        else:
            eta = "ETA calculating…"
        log.info("%s%d/%d батчей (%.1f%%) | in_flight=%d | %s%s",
                 pfx, done_n, total, pct, len(in_flight), eta,
                 f" | {extra}" if extra else "")

    while batch_idx < total or in_flight or pending_retry:
        now      = time.monotonic()
        did_work = False

        # ── 1. Collect all in-flight batches whose INITIAL_WAIT has elapsed ───
        still: list[list] = []
        for entry in in_flight:
            fire_t, rid, domains, rows = entry
            if now - fire_t >= INITIAL_WAIT:
                ok = _collect_one(session, rid, domains, rows, output_path, all_headers)
                did_work = True
                if ok:
                    collected_n += 1
                    done_n      += 1
                    _log_progress()
                else:
                    pending_retry[rid] = [now, now, domains, rows]
            else:
                still.append(entry)
        in_flight = still

        # ── 2. Retry 202 not-ready batches ────────────────────────────────────
        now = time.monotonic()
        for rid in list(pending_retry):
            first_t, last_t, domains, rows = pending_retry[rid]
            if now - first_t >= NOT_READY_TOTAL_TIMEOUT:
                log.warning(
                    "%s202 timeout rid=%s (%d domains) — dropping; "
                    "completion check will retry.",
                    pfx, rid, len(domains),
                )
                del pending_retry[rid]
                did_work = True
                continue
            if now - last_t >= NOT_READY_RETRY_WAIT:
                log.info("%sRetrying 202 rid=%s (%.0fs since first attempt)",
                         pfx, rid, now - first_t)
                ok = _collect_one(session, rid, domains, rows, output_path, all_headers)
                if ok:
                    collected_n += 1
                    done_n      += 1
                    del pending_retry[rid]
                    _log_progress("202 retry ok")
                else:
                    pending_retry[rid][1] = time.monotonic()  # update last_try_t
                did_work = True

        # ── 3. Fire next batch if BATCH_INTERVAL has elapsed ──────────────────
        now = time.monotonic()
        if batch_idx < total and (now - last_fire_t) >= BATCH_INTERVAL:
            domains, rows = all_batches[batch_idx]
            batch_num     = batch_idx + 1
            batch_idx    += 1

            if len(domains) < MIN_BATCH_FOR_GROUP:
                log.info("%sBatch %d/%d (%d domains) → simple endpoint fallback",
                         pfx, batch_num, total, len(domains))
                _process_fallback_batch(
                    session, domains, rows, base, output_path, all_headers
                )
                last_fire_t  = time.monotonic()
                done_n      += 1
                did_work     = True
                _log_progress("fallback")
            else:
                log.info("%sBatch %d/%d (%d domains) → POST…",
                         pfx, batch_num, total, len(domains))
                rid = post_batch(session, domains, base)
                last_fire_t = time.monotonic()
                if rid is _USE_FALLBACK:
                    # POST 400 "более одного домена" — API rejects batch (domains not in index).
                    # Route immediately to simple endpoint instead of waiting for completion check.
                    log.info("%sBatch %d/%d POST rejected → simple endpoint fallback",
                             pfx, batch_num, total)
                    _process_fallback_batch(
                        session, domains, rows, base, output_path, all_headers
                    )
                    done_n += 1
                    _log_progress("POST→fallback")
                elif rid:
                    in_flight.append([last_fire_t, rid, domains, rows])
                    fired_n += 1
                    log.info("%sBatch %d/%d → rid=%s  (in_flight=%d, pending_retry=%d)",
                             pfx, batch_num, total, rid, len(in_flight), len(pending_retry))
                else:
                    log.warning("%sBatch %d/%d POST failed → completion check will retry",
                                pfx, batch_num, total)
                did_work = True

        # ── 4. Nothing ready right now — brief idle sleep ─────────────────────
        if not did_work:
            now = time.monotonic()
            if now - last_progress_t >= 30:
                extra = ""
                if in_flight:
                    oldest_age = now - in_flight[0][0]
                    wait_left  = max(0.0, INITIAL_WAIT - oldest_age)
                    extra = f"ждём ответа сервера ещё ~{wait_left:.0f}с"
                _log_progress(extra)
                last_progress_t = now
            time.sleep(0.5)

    log.info("%sPipeline done — %d fired, %d collected, %d pending_retry dropped",
             pfx, fired_n, collected_n, len(pending_retry))


# ══════════════════════════════════════════════════════════════════════════════
# Completion check — retry domains not present in output at all
# ══════════════════════════════════════════════════════════════════════════════
def run_completion_check(
    session: requests.Session,
    scope_rows: list[dict],
    actual_col: str,
    base: str,
    batch_size: int,
    output_path: Path,
    input_fieldnames: list[str],
    all_headers: list[str],
    label: str = "Completion check",
) -> None:
    for rnd in range(1, COMPLETION_ROUNDS + 1):
        done = read_done_set(output_path, actual_col)
        missing = [
            ((r.get(actual_col) or "").strip().lower(), r)
            for r in scope_rows
            if (r.get(actual_col) or "").strip().lower() not in done
            and (r.get(actual_col) or "").strip()
        ]
        log.info("%s round %d/%d: %d domains not in output",
                 label, rnd, COMPLETION_ROUNDS, len(missing))
        if not missing:
            log.info("✓ All domains in scope accounted for.")
            return
        run_pipeline(session, missing, batch_size, base,
                     output_path, input_fieldnames, all_headers, label=label)


# ══════════════════════════════════════════════════════════════════════════════
# Error retry — strip error rows, re-process those domains
# ══════════════════════════════════════════════════════════════════════════════
def run_error_retry(
    session: requests.Session,
    output_path: Path,
    input_fieldnames: list[str],
    all_headers: list[str],
    actual_col: str,
    base: str,
    batch_size: int,
) -> int:
    """
    1. Read output CSV.
    2. Separate good rows (no error) from error rows.
    3. Rewrite output with only good rows.
    4. Re-process error domains via pipeline (they append as new rows).

    Returns number of error rows found (0 = nothing to do).
    """
    if not output_path.exists():
        return 0

    with open(output_path, encoding="utf-8", newline="") as f:
        all_output_rows: list[dict] = list(csv.DictReader(f))

    error_col  = _col("error")
    good_rows  = [r for r in all_output_rows if not (r.get(error_col) or "").strip()]
    error_rows = [r for r in all_output_rows if     (r.get(error_col) or "").strip()]

    if not error_rows:
        log.info("Error retry: no error rows found.")
        return 0

    # "not found in report" is a definitive result — the domain is simply not indexed
    # in Keys.so's group report. Keep these rows as-is; retrying them would cause POST 400
    # (all-unindexed batches are rejected) and slow sequential fallback (10-40s per domain).
    NOT_FOUND = "not found in report"
    keep_rows  = [r for r in error_rows if (r.get(error_col) or "").strip() == NOT_FOUND]
    retry_rows = [r for r in error_rows if (r.get(error_col) or "").strip() != NOT_FOUND]

    if not retry_rows:
        log.info("Error retry: %d 'not found in report' rows — valid results, no retry.",
                 len(keep_rows))
        return 0

    log.info("Error retry: %d transient errors → retry  |  %d 'not found' kept as-is",
             len(retry_rows), len(keep_rows))

    # Rewrite output keeping good rows AND keep_rows ("not found" = valid final result)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=all_headers, extrasaction="ignore")
        w.writeheader()
        for row in good_rows + keep_rows:
            w.writerow(row)

    # Reconstruct (domain, original_input_row) for transient-error domains only
    keyso_col_set = set(all_headers) - set(input_fieldnames)
    pairs: list[tuple[str, dict]] = []
    seen: set[str] = set()
    for row in retry_rows:
        domain = (row.get(actual_col) or "").strip().lower()
        if domain and domain not in seen:
            seen.add(domain)
            orig_row = {k: v for k, v in row.items() if k not in keyso_col_set}
            pairs.append((domain, orig_row))

    run_pipeline(session, pairs, batch_size, base,
                 output_path, input_fieldnames, all_headers, label="error-retry")

    return len(retry_rows)


# ══════════════════════════════════════════════════════════════════════════════
# Final stats
# ══════════════════════════════════════════════════════════════════════════════
def _print_final_count(
    output_path: Path,
    scope_rows: list[dict],
    actual_col: str,
) -> None:
    done = read_done_set(output_path, actual_col)
    scope_domains = {
        (r.get(actual_col) or "").strip().lower()
        for r in scope_rows
        if (r.get(actual_col) or "").strip()
    }
    in_output  = len(scope_domains & done)
    still_miss = len(scope_domains - done)
    log.info("── Final count ──────────────────────────────────────")
    log.info("  In scope (donors.csv):  %d", len(scope_domains))
    log.info("  In output:              %d", in_output)
    log.info("  Still missing:          %d", still_miss)
    if still_miss:
        log.warning("  %d domains still missing. Re-run to retry.", still_miss)


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Keys.so batch domain parser — true pipeline mode.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input",      required=True,  help="Input CSV path")
    parser.add_argument("--output",     default="output/donors_keyso.csv",
                        help="Output CSV path")
    parser.add_argument("--base",       default="msk",  help="Keys.so region base")
    parser.add_argument("--limit",      type=int, default=0,
                        help="Max NEW domains to process this run (0 = all)")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Domains per API batch (max ~100; 200+ returns HTTP 410)")
    parser.add_argument("--domain-col", default="Домен",
                        help="Input CSV column with domain names")
    parser.add_argument("--no-completion-check", action="store_true",
                        help="Skip completion check and error retry (useful for quick tests)")
    args = parser.parse_args()

    start_t = time.monotonic()

    # ── Auth ──────────────────────────────────────────────────────────────────
    token = os.getenv("KEYSSO_TOKEN", "").strip()
    if not token:
        log.error("KEYSSO_TOKEN not set. Add it to .env file.")
        sys.exit(1)

    input_path  = Path(args.input)
    output_path = Path(args.output)

    # ── Read input CSV ────────────────────────────────────────────────────────
    with open(input_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        input_fieldnames: list[str] = list(reader.fieldnames or [])
        all_input_rows: list[dict]  = list(reader)

    actual_col = find_col(input_fieldnames, args.domain_col)
    if actual_col is None:
        log.error("Domain column '%s' not found. Available: %s",
                  args.domain_col, input_fieldnames)
        sys.exit(1)

    # ── Output headers ────────────────────────────────────────────────────────
    keyso_headers = [_col(c) for c in KEYSO_METRICS + SERVICE_COLS]
    all_headers   = input_fieldnames + keyso_headers

    # ── Dedup: all domains already in output (good + error) are ignored ───────
    # Error rows will be picked up later by run_error_retry.
    done = read_done_set(output_path, actual_col)
    log.info("Already in output (skipping): %d domains", len(done))

    # Optimisation: skip first len(done) rows (sequential order assumption),
    # then set-filter the remainder for safety.
    offset         = len(done)
    pending_pairs: list[tuple[str, dict]] = [
        ((r.get(actual_col) or "").strip().lower(), r)
        for r in all_input_rows[offset:]
        if (r.get(actual_col) or "").strip().lower() not in done
        and (r.get(actual_col) or "").strip()
    ]

    if args.limit > 0:
        pending_pairs = pending_pairs[: args.limit]

    log.info("Domains to process this run: %d  (offset=%d, limit=%s)",
             len(pending_pairs), offset, args.limit or "all")

    # ── Prepare output file ───────────────────────────────────────────────────
    ensure_header(output_path, all_headers)
    session = make_session(token)

    # ── 1. Main processing ────────────────────────────────────────────────────
    if pending_pairs:
        run_pipeline(session, pending_pairs, args.batch_size, args.base,
                     output_path, input_fieldnames, all_headers)
    else:
        log.info("No new domains — skipping main processing.")

    if args.no_completion_check:
        log.info("Skipping completion check and error retry (--no-completion-check).")
        _print_final_count(output_path, all_input_rows[:offset + len(pending_pairs)], actual_col)
        log.info("Затрачено %s", _fmt_elapsed(time.monotonic() - start_t))
        return

    # Scope for all checks: rows we intended to process this run
    scope = all_input_rows[: offset + len(pending_pairs)] if args.limit > 0 else all_input_rows

    # ── 2. Completion check — domains not in output at all ────────────────────
    run_completion_check(
        session, scope, actual_col, args.base, args.batch_size,
        output_path, input_fieldnames, all_headers,
        label="Completion check",
    )

    # ── 3. Error retry — re-process rows that have any error ─────────────────
    n_errors = run_error_retry(
        session, output_path, input_fieldnames, all_headers,
        actual_col, args.base, args.batch_size,
    )

    # ── 4. Final completion check after error retry ───────────────────────────
    if n_errors > 0:
        log.info("Running final completion check after error retry…")
        run_completion_check(
            session, scope, actual_col, args.base, args.batch_size,
            output_path, input_fieldnames, all_headers,
            label="Final completion check",
        )

    _print_final_count(output_path, scope, actual_col)
    log.info("Затрачено %s", _fmt_elapsed(time.monotonic() - start_t))


if __name__ == "__main__":
    main()
