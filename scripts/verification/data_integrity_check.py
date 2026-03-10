#!/usr/bin/env python3
import argparse
import hashlib

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
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify retrieved telemetry matches the original payloads.")
    parser.add_argument("--base-url", default=DEFAULT_API_BASE_URL)
    parser.add_argument("--packets", type=int, default=20)
    parser.add_argument("--wait-seconds", type=float, default=5.0)
    parser.add_argument("--platform", default="verification")
    parser.add_argument("--kind", default="verification_integrity")
    parser.add_argument("--device-external-id")
    parser.add_argument("--label", default=timestamp_label())
    parser.add_argument("--skip-db-record", action="store_true")
    return parser.parse_args()


def payload_for(label: str, index: int) -> dict:
    marker = f"{label}:{index}"
    return {
        "marker": marker,
        "checksum": hashlib.sha256(marker.encode("utf-8")).hexdigest(),
        "reading": index * 11,
    }


def main() -> int:
    args = parse_args()
    started_at = utcnow()
    run_dir = ensure_results_dir("data_integrity", args.label)
    base_url = args.base_url

    user_id = create_verification_user(args.label)
    device_external_id = args.device_external_id or f"verification-integrity-{args.label}"

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
            "meta": {"source": "verification_integrity", "label": args.label},
        },
    )
    if not start_session_resp["ok"]:
        raise RuntimeError(f"Session creation failed: {start_session_resp}")
    session_id = start_session_resp["body"]["session_id"]

    packets = []
    for index in range(1, args.packets + 1):
        packet = {
            "device_external_id": device_external_id,
            "session_id": session_id,
            "kind": args.kind,
            "seq": index,
            "data": payload_for(args.label, index),
        }
        response = http_request(
            method="POST",
            url=build_url(base_url, "/api/sensors/events"),
            json_body=packet,
        )
        if not response["ok"]:
            raise RuntimeError(f"Ingest failed at seq={index}: {response}")
        packets.append(packet)

    if args.wait_seconds > 0:
        import time

        time.sleep(args.wait_seconds)

    retrieve_resp = http_request(
        method="GET",
        url=build_url(
            base_url,
            f"/api/sensors/last?device_external_id={device_external_id}&kind={args.kind}&n={args.packets}",
        ),
    )
    if not retrieve_resp["ok"] or not isinstance(retrieve_resp["body"], list):
        raise RuntimeError(f"Retrieval failed: {retrieve_resp}")

    retrieved_by_seq = {
        int(item["seq"]): item.get("data", {})
        for item in retrieve_resp["body"]
        if item.get("seq") is not None
    }
    exact_matches = sum(
        1
        for packet in packets
        if retrieved_by_seq.get(packet["seq"]) == packet["data"]
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

    exact_match_rate = round(safe_div(exact_matches, args.packets), 6)
    summary = {
        "test_key": "data_integrity",
        "scenario": args.kind,
        "status": "ok" if exact_matches == args.packets and db_count == args.packets else "degraded",
        "target": build_url(base_url, "/api/sensors/last"),
        "started_at": isoformat(started_at),
        "finished_at": isoformat(),
        "total_requests": args.packets,
        "records_expected": args.packets,
        "records_verified": exact_matches,
        "successful_requests": exact_matches,
        "failed_requests": args.packets - exact_matches,
        "success_rate": exact_match_rate,
        "exact_match_rate": exact_match_rate,
        "artifacts_path": relative_to_repo(run_dir),
        "device_external_id": device_external_id,
        "session_id": session_id,
        "user_id": user_id,
        "db_count": db_count,
    }

    write_json(run_dir / "sent_packets.json", packets)
    write_json(run_dir / "retrieved_packets.json", retrieve_resp["body"])
    write_json(run_dir / "summary.json", summary)

    if not args.skip_db_record:
        insert_verification_run(summary)

    print(f"Summary written to {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
