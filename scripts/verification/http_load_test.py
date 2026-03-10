#!/usr/bin/env python3
import argparse
import concurrent.futures
import json
from pathlib import Path

from common import (
    DEFAULT_API_BASE_URL,
    build_url,
    ensure_results_dir,
    http_request,
    insert_verification_run,
    isoformat,
    parse_headers,
    percentile,
    relative_to_repo,
    safe_div,
    timestamp_label,
    utcnow,
    write_csv,
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TrackSante HTTP latency and concurrency checks.")
    parser.add_argument("--base-url", default=DEFAULT_API_BASE_URL)
    parser.add_argument("--path", default="/api/health")
    parser.add_argument("--url")
    parser.add_argument("--method", default="GET")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--ladder", help="Comma-separated concurrency steps, e.g. 5,10,20,40")
    parser.add_argument("--requests-per-step", type=int, default=50)
    parser.add_argument("--max-p95-ms", type=float, default=2000.0)
    parser.add_argument("--max-error-rate", type=float, default=0.01)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--json-body")
    parser.add_argument("--json-body-file")
    parser.add_argument("--header", action="append", default=[])
    parser.add_argument("--label", default=timestamp_label())
    parser.add_argument("--skip-db-record", action="store_true")
    return parser.parse_args()


def load_json_body(args: argparse.Namespace) -> object | None:
    if args.json_body_file:
        return json.loads(Path(args.json_body_file).read_text(encoding="utf-8"))
    if args.json_body:
        return json.loads(args.json_body)
    return None


def run_batch(
    *,
    url: str,
    method: str,
    request_count: int,
    concurrency: int,
    headers: dict[str, str],
    json_body: object | None,
    timeout_seconds: float,
    step_name: str,
) -> dict:
    rows: list[dict] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [
            pool.submit(
                http_request,
                method=method,
                url=url,
                headers=headers,
                json_body=json_body,
                timeout_seconds=timeout_seconds,
            )
            for _ in range(request_count)
        ]
        for index, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            row = future.result()
            row["request_index"] = index
            row["step"] = step_name
            rows.append(row)

    durations = [row["duration_ms"] for row in rows if row["duration_ms"] is not None]
    successful_requests = sum(1 for row in rows if row["ok"])
    failed_requests = len(rows) - successful_requests
    p50_ms = round(percentile(durations, 0.50), 3) if durations else None
    p95_ms = round(percentile(durations, 0.95), 3) if durations else None
    success_rate = round(safe_div(successful_requests, len(rows)), 6)

    return {
        "rows": sorted(rows, key=lambda row: row["request_index"]),
        "request_count": len(rows),
        "successful_requests": successful_requests,
        "failed_requests": failed_requests,
        "success_rate": success_rate,
        "error_rate": round(1.0 - success_rate, 6),
        "p50_ms": p50_ms,
        "p95_ms": p95_ms,
        "concurrency": concurrency,
    }


def main() -> int:
    args = parse_args()
    started_at = utcnow()
    test_key = "concurrent_capacity" if args.ladder else "response_time"
    run_dir = ensure_results_dir(test_key, args.label)

    url = args.url or build_url(args.base_url, args.path)
    headers = parse_headers(args.header)
    json_body = load_json_body(args)

    request_rows: list[dict] = []
    summary: dict

    if args.ladder:
        steps = [int(part.strip()) for part in args.ladder.split(",") if part.strip()]
        step_summaries: list[dict] = []
        sustainable_concurrency = 0
        sustainable_snapshot: dict | None = None

        for step in steps:
            batch = run_batch(
                url=url,
                method=args.method,
                request_count=args.requests_per_step,
                concurrency=step,
                headers=headers,
                json_body=json_body,
                timeout_seconds=args.timeout_seconds,
                step_name=f"c{step}",
            )
            request_rows.extend(batch["rows"])
            del batch["rows"]
            batch["threshold_ok"] = (
                batch["error_rate"] <= args.max_error_rate
                and (batch["p95_ms"] is None or batch["p95_ms"] <= args.max_p95_ms)
            )
            step_summaries.append(batch)
            if batch["threshold_ok"]:
                sustainable_concurrency = step
                sustainable_snapshot = batch
            else:
                break

        summary = {
            "test_key": test_key,
            "scenario": args.path,
            "status": "ok" if sustainable_concurrency == max(steps) else "threshold_reached",
            "target": url,
            "started_at": isoformat(started_at),
            "finished_at": isoformat(),
            "total_requests": sum(step["request_count"] for step in step_summaries),
            "successful_requests": sum(step["successful_requests"] for step in step_summaries),
            "failed_requests": sum(step["failed_requests"] for step in step_summaries),
            "success_rate": sustainable_snapshot["success_rate"] if sustainable_snapshot else 0.0,
            "p50_ms": sustainable_snapshot["p50_ms"] if sustainable_snapshot else None,
            "p95_ms": sustainable_snapshot["p95_ms"] if sustainable_snapshot else None,
            "max_concurrency": max(steps),
            "sustainable_concurrency": sustainable_concurrency,
            "artifacts_path": relative_to_repo(run_dir),
            "thresholds": {
                "max_p95_ms": args.max_p95_ms,
                "max_error_rate": args.max_error_rate,
            },
            "steps": step_summaries,
        }
    else:
        batch = run_batch(
            url=url,
            method=args.method,
            request_count=args.requests,
            concurrency=args.concurrency,
            headers=headers,
            json_body=json_body,
            timeout_seconds=args.timeout_seconds,
            step_name=f"c{args.concurrency}",
        )
        request_rows.extend(batch["rows"])
        del batch["rows"]

        summary = {
            "test_key": test_key,
            "scenario": args.path,
            "status": "ok" if batch["failed_requests"] == 0 else "degraded",
            "target": url,
            "started_at": isoformat(started_at),
            "finished_at": isoformat(),
            "total_requests": batch["request_count"],
            "successful_requests": batch["successful_requests"],
            "failed_requests": batch["failed_requests"],
            "success_rate": batch["success_rate"],
            "p50_ms": batch["p50_ms"],
            "p95_ms": batch["p95_ms"],
            "max_concurrency": args.concurrency,
            "artifacts_path": relative_to_repo(run_dir),
            "batch": batch,
        }

    write_csv(
        run_dir / "requests.csv",
        ["step", "request_index", "timestamp", "method", "url", "status_code", "ok", "duration_ms", "error", "body"],
        request_rows,
    )
    write_json(run_dir / "summary.json", summary)

    if not args.skip_db_record:
        insert_verification_run(summary)

    print(f"Summary written to {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
