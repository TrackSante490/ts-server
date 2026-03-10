#!/usr/bin/env python3
import argparse
import concurrent.futures
import hashlib
import json

from common import (
    DEFAULT_API_BASE_URL,
    build_url,
    create_verification_user,
    ensure_results_dir,
    http_request,
    insert_verification_run,
    isoformat,
    relative_to_repo,
    run_psql_scalar,
    safe_div,
    timestamp_label,
    utcnow,
    write_csv,
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify sensor ingest acknowledgement and storage.")
    parser.add_argument("--base-url", default=DEFAULT_API_BASE_URL)
    parser.add_argument("--packets", type=int, default=25)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--platform", default="verification")
    parser.add_argument("--kind", default="verification_ingest")
    parser.add_argument("--device-external-id")
    parser.add_argument("--label", default=timestamp_label())
    parser.add_argument("--skip-db-record", action="store_true")
    return parser.parse_args()


def payload_for(label: str, index: int) -> dict:
    marker = f"{label}:{index}"
    checksum = hashlib.sha256(marker.encode("utf-8")).hexdigest()[:16]
    return {
        "marker": marker,
        "packet_index": index,
        "checksum": checksum,
        "value": index * 7,
    }


def main() -> int:
    args = parse_args()
    started_at = utcnow()
    run_dir = ensure_results_dir("ingest_success", args.label)
    base_url = args.base_url

    user_id = create_verification_user(args.label)
    device_external_id = args.device_external_id or f"verification-{args.label}"

    register_resp = http_request(
        method="POST",
        url=build_url(base_url, "/api/devices/register"),
        json_body={
            "user_id": user_id,
            "device_external_id": device_external_id,
            "platform": args.platform,
            "capabilities": {"verification": True},
        },
    )
    if not register_resp["ok"]:
        raise RuntimeError(f"Device registration failed: {register_resp}")

    start_session_resp = http_request(
        method="POST",
        url=build_url(base_url, "/api/sessions/start"),
        json_body={
            "user_id": user_id,
            "device_external_id": device_external_id,
            "meta": {"source": "verification_ingest", "label": args.label},
        },
    )
    if not start_session_resp["ok"]:
        raise RuntimeError(f"Session creation failed: {start_session_resp}")
    session_id = start_session_resp["body"]["session_id"]

    packets = [
        {
            "device_external_id": device_external_id,
            "session_id": session_id,
            "kind": args.kind,
            "seq": index,
            "data": payload_for(args.label, index),
        }
        for index in range(1, args.packets + 1)
    ]

    rows: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [
            pool.submit(
                http_request,
                method="POST",
                url=build_url(base_url, "/api/sensors/events"),
                json_body=packet,
            )
            for packet in packets
        ]
        for packet, future in zip(packets, futures):
            row = future.result()
            row["seq"] = packet["seq"]
            row["packet"] = json.dumps(packet["data"], sort_keys=True)
            rows.append(row)

    stored_ack_count = sum(
        1
        for row in rows
        if row["ok"] and isinstance(row["body"], dict) and row["body"].get("stored") is True
    )
    db_count = int(
        run_psql_scalar(
            f"""
            SELECT count(*)
            FROM sensor_events se
            JOIN devices d ON d.id = se.device_id
            WHERE d.device_external_id = '{device_external_id}'
              AND se.session_id = '{session_id}'::uuid
              AND se.kind = '{args.kind}'
              AND se.seq BETWEEN 1 AND {args.packets};
            """
        )
        or "0"
    )
    verified_successes = min(stored_ack_count, db_count)

    summary = {
        "test_key": "ingest_success",
        "scenario": args.kind,
        "status": "ok" if verified_successes == args.packets else "degraded",
        "target": build_url(base_url, "/api/sensors/events"),
        "started_at": isoformat(started_at),
        "finished_at": isoformat(),
        "total_requests": args.packets,
        "successful_requests": stored_ack_count,
        "failed_requests": args.packets - stored_ack_count,
        "success_rate": round(safe_div(verified_successes, args.packets), 6),
        "records_expected": args.packets,
        "records_verified": db_count,
        "artifacts_path": relative_to_repo(run_dir),
        "device_external_id": device_external_id,
        "session_id": session_id,
        "user_id": user_id,
    }

    write_json(run_dir / "packets.json", packets)
    write_csv(
        run_dir / "responses.csv",
        ["seq", "timestamp", "method", "url", "status_code", "ok", "duration_ms", "error", "packet", "body"],
        sorted(rows, key=lambda row: row["seq"]),
    )
    write_json(run_dir / "summary.json", summary)

    if not args.skip_db_record:
        insert_verification_run(summary)

    print(f"Summary written to {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
