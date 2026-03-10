#!/usr/bin/env python3
import argparse
import time
from pathlib import Path

from common import (
    DEFAULT_API_BASE_URL,
    build_url,
    ensure_results_dir,
    http_request,
    insert_verification_run,
    isoformat,
    percentile,
    relative_to_repo,
    safe_div,
    timestamp_label,
    utcnow,
    write_csv,
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuously probe a TrackSante health endpoint.")
    parser.add_argument("--base-url", default=DEFAULT_API_BASE_URL)
    parser.add_argument("--endpoint", default="/api/health")
    parser.add_argument("--interval-seconds", type=float, default=60.0)
    parser.add_argument("--checks", type=int)
    parser.add_argument("--duration-seconds", type=float)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--label", default=timestamp_label())
    parser.add_argument("--skip-db-record", action="store_true")
    return parser.parse_args()


def count_downtime_events(rows: list[dict]) -> int:
    downtime_events = 0
    was_down = False
    for row in rows:
        is_down = not row["ok"]
        if is_down and not was_down:
            downtime_events += 1
        was_down = is_down
    return downtime_events


def main() -> int:
    args = parse_args()
    if args.checks is None and args.duration_seconds is None:
        args.checks = 10

    url = build_url(args.base_url, args.endpoint)
    started_at = utcnow()
    run_dir = ensure_results_dir("server_uptime", args.label)

    rows: list[dict] = []
    deadline = time.monotonic() + args.duration_seconds if args.duration_seconds else None

    try:
        while True:
            result = http_request(
                method="GET",
                url=url,
                timeout_seconds=args.timeout_seconds,
            )
            rows.append(result)
            print(
                f"[{result['timestamp']}] status={result['status_code']} "
                f"ok={result['ok']} duration_ms={result['duration_ms']}"
            )

            if args.checks is not None and len(rows) >= args.checks:
                break
            if deadline is not None and time.monotonic() >= deadline:
                break
            time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        print("Interrupted; writing partial results.")

    durations = [row["duration_ms"] for row in rows if row["duration_ms"] is not None]
    successful_requests = sum(1 for row in rows if row["ok"])
    failed_requests = len(rows) - successful_requests
    summary = {
        "test_key": "server_uptime",
        "scenario": args.endpoint,
        "status": "ok" if failed_requests == 0 else "degraded",
        "target": url,
        "started_at": isoformat(started_at),
        "finished_at": isoformat(),
        "total_requests": len(rows),
        "successful_requests": successful_requests,
        "failed_requests": failed_requests,
        "success_rate": round(safe_div(successful_requests, len(rows)), 6),
        "p50_ms": round(percentile(durations, 0.50), 3) if durations else None,
        "p95_ms": round(percentile(durations, 0.95), 3) if durations else None,
        "downtime_events": count_downtime_events(rows),
        "artifacts_path": relative_to_repo(run_dir),
        "samples_file": str(Path(relative_to_repo(run_dir)) / "samples.csv"),
    }

    write_csv(
        run_dir / "samples.csv",
        ["timestamp", "method", "url", "status_code", "ok", "duration_ms", "error", "body"],
        rows,
    )
    write_json(run_dir / "summary.json", summary)

    if not args.skip_db_record:
        insert_verification_run(summary)

    print(f"Summary written to {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
