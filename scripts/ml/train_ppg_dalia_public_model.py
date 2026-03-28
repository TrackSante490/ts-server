#!/usr/bin/env python3
import argparse
import logging
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
API_DIR = REPO_ROOT / "api"
SCRIPT_DIR = Path(__file__).resolve().parent
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from sensor_public_model import (  # noqa: E402
    PPG_DALIA_DATASET_URL,
    download_ppg_dalia_dataset,
    train_ppg_dalia_public_model,
)
import train_bidmc_public_model as common  # noqa: E402


LOGGER = logging.getLogger("ppg_dalia_public_model.train")
common.LOGGER = LOGGER


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Train a PyTorch public anomaly model from the PPG-DaLiA dataset."
    )
    parser.add_argument(
        "--dataset-dir",
        help="Path to an extracted PPG-DaLiA dataset directory containing subject pickle files.",
    )
    parser.add_argument(
        "--download-dir",
        help="Directory to download and extract the PPG-DaLiA dataset into. If omitted, --dataset-dir is required.",
    )
    parser.add_argument(
        "--output",
        default=str(REPO_ROOT / "api" / "models" / "ppg_dalia_public_model.pt"),
        help="Path to write the trained PyTorch artifact.",
    )
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--step-seconds", type=int, default=30)
    parser.add_argument("--quality-profile", choices=sorted(common.QUALITY_PROFILES), default="balanced")
    parser.add_argument("--epochs", type=int)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--learning-rate", type=float)
    parser.add_argument("--noise-std", type=float)
    parser.add_argument("--validation-fraction", type=float)
    parser.add_argument("--log-interval", type=int)
    parser.add_argument("--device", choices=["auto", "cpu", "cuda"])
    parser.add_argument("--weight-decay", type=float)
    parser.add_argument("--patience", type=int)
    parser.add_argument("--min-delta", type=float)
    parser.add_argument("--scheduler-factor", type=float)
    parser.add_argument("--scheduler-patience", type=int)
    parser.add_argument("--progress", choices=["auto", "tqdm", "plain", "off"], default="auto")
    parser.add_argument(
        "--report-dir",
        help="Directory to write CSV/JSON logs and training graphs into. Defaults next to the output artifact.",
    )
    parser.add_argument(
        "--log-file",
        help="Path to write the persistent training log. Defaults to <report-dir>/training.log.",
    )
    parser.add_argument("--amp", dest="use_amp", action="store_true")
    parser.add_argument("--no-amp", dest="use_amp", action="store_false")
    parser.set_defaults(use_amp=None)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    try:
        import torch  # noqa: F401
    except Exception as exc:
        parser.error(f"PyTorch must be installed to train this model: {exc}")

    training_options = common._resolve_training_options(args)
    report_dir = common._report_dir(args.output, args.report_dir)
    log_file = Path(args.log_file) if args.log_file else report_dir / "training.log"
    common._setup_logging(log_file)
    progress = common._ProgressTracker(args.progress)

    dataset_dir = args.dataset_dir
    if not dataset_dir:
        if not args.download_dir:
            parser.error("Either --dataset-dir or --download-dir is required.")
        print(f"Downloading PPG-DaLiA dataset from {PPG_DALIA_DATASET_URL} ...")
        dataset_dir = str(download_ppg_dalia_dataset(args.download_dir))

    LOGGER.info(
        "starting training profile=%s device=%s output=%s report_dir=%s log_file=%s",
        args.quality_profile,
        training_options["device"],
        args.output,
        report_dir,
        log_file,
    )
    with progress.redirect_context:
        artifact = train_ppg_dalia_public_model(
            dataset_dir=dataset_dir,
            output_path=args.output,
            window_seconds=args.window_seconds,
            step_seconds=args.step_seconds,
            epochs=training_options["epochs"],
            batch_size=training_options["batch_size"],
            learning_rate=training_options["learning_rate"],
            noise_std=training_options["noise_std"],
            validation_fraction=training_options["validation_fraction"],
            log_interval=training_options["log_interval"],
            seed=args.seed,
            device=training_options["device"],
            weight_decay=training_options["weight_decay"],
            patience=training_options["patience"],
            min_delta=training_options["min_delta"],
            scheduler_factor=training_options["scheduler_factor"],
            scheduler_patience=training_options["scheduler_patience"],
            use_amp=training_options["use_amp"],
            progress_callback=lambda event: (progress.handle(event), common._log_training_event(event)),
        )
    progress.close()
    report_paths = common._write_training_reports(artifact, report_dir)

    model_names = ", ".join(sorted((artifact.get("models") or {}).keys()))
    print(f"Trained {artifact.get('model_name')} from {artifact.get('source_files')} source files.")
    print(f"Available subset models: {model_names}")
    print(f"Artifact written to: {args.output}")
    print(f"Training reports written to: {report_dir}")
    print(f"Training log written to: {log_file}")
    for report_path in report_paths:
        print(f" - {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
