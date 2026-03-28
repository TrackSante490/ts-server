#!/usr/bin/env python3
import argparse
import json
import math
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
API_DIR = REPO_ROOT / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from sensor_public_model import load_public_model, summarize_public_artifact_training  # noqa: E402


def _rank_key(summary: dict) -> tuple[float, float, float]:
    score = float(summary.get("score") or math.inf)
    source_files = -float(summary.get("source_files") or 0)
    total_train_windows = -float(summary.get("total_train_windows") or 0)
    return (score, source_files, total_train_windows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate public model artifacts and pick the best one.")
    parser.add_argument("artifacts", nargs="+", help="Paths to .pt artifacts to evaluate.")
    parser.add_argument("--summary-out", help="Optional path to write a JSON comparison summary.")
    args = parser.parse_args()

    comparisons = []
    for raw_path in args.artifacts:
        path = Path(raw_path)
        artifact = load_public_model(path)
        summary = summarize_public_artifact_training(artifact)
        summary["artifact_path"] = str(path)
        comparisons.append(summary)

    ready = [item for item in comparisons if item.get("ready")]
    best = min(ready, key=_rank_key) if ready else None

    output = {
        "artifact_count": len(comparisons),
        "ready_count": len(ready),
        "best_artifact_path": best.get("artifact_path") if best else None,
        "best_score": best.get("score") if best else None,
        "comparisons": comparisons,
    }
    if args.summary_out:
        summary_path = Path(args.summary_out)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

    if best:
        print(best["artifact_path"])
        return 0

    print("", end="")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
