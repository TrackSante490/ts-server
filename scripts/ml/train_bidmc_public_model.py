#!/usr/bin/env python3
import argparse
import csv
from contextlib import nullcontext
import json
import logging
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
API_DIR = REPO_ROOT / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from sensor_public_model import (  # noqa: E402
    BIDMC_DATASET_URL,
    download_bidmc_dataset,
    train_bidmc_public_model,
)

try:
    from tqdm.auto import tqdm
    from tqdm.contrib.logging import logging_redirect_tqdm
except Exception:
    tqdm = None
    logging_redirect_tqdm = None

LOGGER = logging.getLogger("bidmc_public_model.train")
QUALITY_PROFILES: dict[str, dict[str, Any]] = {
    "fast": {
        "epochs": 60,
        "batch_size": 128,
        "learning_rate": 1e-3,
        "noise_std": 0.05,
        "validation_fraction": 0.15,
        "log_interval": 5,
        "device": "auto",
        "weight_decay": 0.0,
        "patience": 10,
        "min_delta": 1e-4,
        "scheduler_factor": 0.5,
        "scheduler_patience": 4,
        "use_amp": True,
    },
    "balanced": {
        "epochs": 160,
        "batch_size": 128,
        "learning_rate": 7.5e-4,
        "noise_std": 0.03,
        "validation_fraction": 0.2,
        "log_interval": 5,
        "device": "auto",
        "weight_decay": 1e-5,
        "patience": 20,
        "min_delta": 1e-4,
        "scheduler_factor": 0.5,
        "scheduler_patience": 6,
        "use_amp": True,
    },
    "max": {
        "epochs": 320,
        "batch_size": 256,
        "learning_rate": 5e-4,
        "noise_std": 0.02,
        "validation_fraction": 0.25,
        "log_interval": 1,
        "device": "auto",
        "weight_decay": 1e-4,
        "patience": 35,
        "min_delta": 5e-5,
        "scheduler_factor": 0.4,
        "scheduler_patience": 8,
        "use_amp": True,
    },
}


def _setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)


def _progress_bar(completed: int, total: int, width: int = 20) -> str:
    total = max(1, total)
    completed = max(0, min(completed, total))
    filled = int(round((completed / total) * width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


class _ProgressTracker:
    def __init__(self, style: str):
        self.style = style
        self._bars: dict[str, Any] = {}
        self._order: list[str] = []
        self._enabled = (
            style == "tqdm"
            or (style == "auto" and tqdm is not None and sys.stderr.isatty())
        )
        if style == "tqdm" and tqdm is None:
            LOGGER.warning("tqdm progress was requested but tqdm is not installed; falling back to plain logs.")
            self._enabled = False

    @property
    def redirect_context(self):
        if self._enabled and logging_redirect_tqdm is not None:
            return logging_redirect_tqdm()
        return nullcontext()

    def handle(self, event: dict[str, Any]) -> None:
        if not self._enabled or tqdm is None:
            return

        event_type = str(event.get("event") or "")
        subset = str(event.get("subset") or "training")
        if subset not in self._bars:
            self._order.append(subset)
            position = len(self._order) - 1
            self._bars[subset] = tqdm(
                total=int(event.get("total_epochs") or 1),
                desc=subset,
                position=position,
                dynamic_ncols=True,
                leave=True,
            )

        bar = self._bars[subset]
        if event_type == "epoch":
            epoch = int(event.get("epoch") or 0)
            delta = epoch - int(bar.n)
            if delta > 0:
                bar.update(delta)
            postfix = {
                "train": f"{float(event.get('train_loss') or 0.0):.4f}",
            }
            validation_loss = event.get("validation_loss")
            if validation_loss is not None:
                postfix["val"] = f"{float(validation_loss):.4f}"
            learning_rate = event.get("learning_rate")
            if learning_rate is not None:
                postfix["lr"] = f"{float(learning_rate):.2e}"
            bar.set_postfix(postfix, refresh=False)
            return

        if event_type == "subset_complete":
            completed_epochs = int(event.get("completed_epochs") or 0)
            if completed_epochs and completed_epochs < int(bar.total):
                bar.total = completed_epochs
            delta = completed_epochs - int(bar.n)
            if delta > 0:
                bar.update(delta)
            bar.set_postfix(
                {
                    "best_epoch": int(event.get("best_epoch") or 0),
                    "best_val": f"{float(event.get('best_validation_loss') or 0.0):.4f}",
                },
                refresh=False,
            )
            bar.refresh()
            bar.close()
            self._bars.pop(subset, None)

    def close(self) -> None:
        for bar in list(self._bars.values()):
            bar.close()
        self._bars.clear()


def _log_training_event(event: dict[str, Any]) -> None:
    event_type = str(event.get("event") or "")
    subset_name = str(event.get("subset") or "training")
    if event_type == "epoch":
        epoch = int(event.get("epoch") or 0)
        total_epochs = int(event.get("total_epochs") or 1)
        train_loss = float(event.get("train_loss") or 0.0)
        validation_loss = event.get("validation_loss")
        learning_rate = event.get("learning_rate")
        progress = _progress_bar(epoch, total_epochs)
        if validation_loss is None:
            LOGGER.info(
                "subset=%s %s epoch=%s/%s train_loss=%.6f lr=%.2e device=%s",
                subset_name,
                progress,
                epoch,
                total_epochs,
                train_loss,
                float(learning_rate or 0.0),
                event.get("device") or "unknown",
            )
            return
        LOGGER.info(
            "subset=%s %s epoch=%s/%s train_loss=%.6f validation_loss=%.6f lr=%.2e device=%s",
            subset_name,
            progress,
            epoch,
            total_epochs,
            train_loss,
            float(validation_loss),
            float(learning_rate or 0.0),
            event.get("device") or "unknown",
        )
        return

    LOGGER.info(
        "subset=%s completed_epochs=%s/%s best_epoch=%s best_validation_loss=%s early_stopped=%s device=%s",
        subset_name,
        int(event.get("completed_epochs") or 0),
        int(event.get("total_epochs") or 1),
        int(event.get("best_epoch") or 0),
        f"{float(event.get('best_validation_loss') or 0.0):.6f}",
        bool(event.get("early_stopped")),
        event.get("device") or "unknown",
    )


def _report_dir(output_path: str, explicit_dir: str | None) -> Path:
    if explicit_dir:
        return Path(explicit_dir)
    output_file = Path(output_path)
    return output_file.parent / f"{output_file.stem}_reports"


def _artifact_summary(artifact: dict) -> dict:
    models = artifact.get("models") or {}
    summary = {
        "model_name": artifact.get("model_name"),
        "model_format": artifact.get("model_format"),
        "dataset_name": artifact.get("dataset_name"),
        "dataset_url": artifact.get("dataset_url"),
        "source_files": artifact.get("source_files"),
        "window_seconds": artifact.get("window_seconds"),
        "step_seconds": artifact.get("step_seconds"),
        "training": artifact.get("training") or {},
        "subsets": {},
    }
    for subset_name, model in sorted(models.items()):
        summary["subsets"][subset_name] = {
            "metrics": model.get("metrics") or [],
            "train_windows": model.get("train_windows"),
            "validation_windows": model.get("validation_windows"),
            "train_loss": model.get("train_loss"),
            "validation_loss": model.get("validation_loss"),
            "best_epoch": model.get("best_epoch"),
            "threshold": model.get("threshold"),
            "threshold_source": model.get("threshold_source"),
            "distance_p95": model.get("distance_p95"),
            "distance_p98": model.get("distance_p98"),
        }
    return summary


def _write_training_summary(artifact: dict, report_dir: Path) -> Path:
    output_path = report_dir / "training_summary.json"
    output_path.write_text(json.dumps(_artifact_summary(artifact), indent=2), encoding="utf-8")
    return output_path


def _write_subset_metrics_csv(artifact: dict, report_dir: Path) -> Path:
    output_path = report_dir / "subset_metrics.csv"
    models = artifact.get("models") or {}
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "subset",
                "metrics",
                "train_windows",
                "validation_windows",
                "best_epoch",
                "train_loss",
                "validation_loss",
                "threshold",
                "threshold_source",
                "distance_p95",
                "distance_p98",
            ]
        )
        for subset_name, model in sorted(models.items()):
            writer.writerow(
                [
                    subset_name,
                    ",".join(model.get("metrics") or []),
                    model.get("train_windows"),
                    model.get("validation_windows"),
                    model.get("best_epoch"),
                    model.get("train_loss"),
                    model.get("validation_loss"),
                    model.get("threshold"),
                    model.get("threshold_source"),
                    model.get("distance_p95"),
                    model.get("distance_p98"),
                ]
            )
    return output_path


def _write_loss_history_csv(artifact: dict, report_dir: Path) -> Path:
    output_path = report_dir / "loss_history.csv"
    reports = artifact.get("_reports") or {}
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["subset", "epoch", "train_loss", "validation_loss"])
        for subset_name, report in sorted(reports.items()):
            for row in report.get("loss_history") or []:
                writer.writerow(
                    [
                        subset_name,
                        row.get("epoch"),
                        row.get("train_loss"),
                        row.get("validation_loss"),
                    ]
                )
    return output_path


def _write_error_summary_csv(artifact: dict, report_dir: Path) -> Path:
    output_path = report_dir / "reconstruction_errors.csv"
    reports = artifact.get("_reports") or {}
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["subset", "source", "sample_index", "reconstruction_error"])
        for subset_name, report in sorted(reports.items()):
            for source_name in ("train_errors", "validation_errors"):
                source_errors = report.get(source_name) or []
                source_label = source_name.removesuffix("_errors")
                for index, error in enumerate(source_errors, start=1):
                    writer.writerow([subset_name, source_label, index, error])
    return output_path


def _write_plots(artifact: dict, report_dir: Path) -> list[Path]:
    try:
        import matplotlib.pyplot as plt
    except Exception as exc:
        LOGGER.warning("Skipping training plots because matplotlib is unavailable: %s", exc)
        return []

    reports = artifact.get("_reports") or {}
    models = artifact.get("models") or {}
    subset_names = sorted(reports)
    if not subset_names:
        return []

    output_paths: list[Path] = []
    columns = 2
    rows = max(1, (len(subset_names) + columns - 1) // columns)

    loss_figure, loss_axes = plt.subplots(rows, columns, figsize=(12, max(4, rows * 3.5)))
    loss_axes_list = list(getattr(loss_axes, "flat", [loss_axes]))
    for axis, subset_name in zip(loss_axes_list, subset_names):
        history = reports[subset_name].get("loss_history") or []
        epochs = [row.get("epoch") for row in history]
        train_losses = [row.get("train_loss") for row in history]
        validation_losses = [row.get("validation_loss") for row in history]
        axis.plot(epochs, train_losses, label="train", linewidth=1.8)
        if any(value is not None for value in validation_losses):
            axis.plot(epochs, validation_losses, label="validation", linewidth=1.5)
        axis.set_title(subset_name)
        axis.set_xlabel("Epoch")
        axis.set_ylabel("Loss")
        axis.grid(alpha=0.25)
        axis.legend()
    for axis in loss_axes_list[len(subset_names):]:
        axis.axis("off")
    loss_figure.tight_layout()
    loss_path = report_dir / "loss_curves.png"
    loss_figure.savefig(loss_path, dpi=180, bbox_inches="tight")
    plt.close(loss_figure)
    output_paths.append(loss_path)

    error_figure, error_axes = plt.subplots(rows, columns, figsize=(12, max(4, rows * 3.5)))
    error_axes_list = list(getattr(error_axes, "flat", [error_axes]))
    for axis, subset_name in zip(error_axes_list, subset_names):
        report = reports[subset_name]
        model = models.get(subset_name) or {}
        validation_errors = report.get("validation_errors") or []
        train_errors = report.get("train_errors") or []
        errors = validation_errors or train_errors
        source_label = "validation" if validation_errors else "train"
        if errors:
            bins = min(30, max(10, int(len(errors) ** 0.5)))
            axis.hist(errors, bins=bins, color="#2f6db3", alpha=0.8)
        axis.axvline(float(model.get("threshold") or 0.0), color="#c0392b", linestyle="--", linewidth=1.8, label="threshold")
        axis.set_title(f"{subset_name} ({source_label})")
        axis.set_xlabel("Reconstruction error")
        axis.set_ylabel("Count")
        axis.grid(alpha=0.2)
        axis.legend()
    for axis in error_axes_list[len(subset_names):]:
        axis.axis("off")
    error_figure.tight_layout()
    error_path = report_dir / "reconstruction_error_histograms.png"
    error_figure.savefig(error_path, dpi=180, bbox_inches="tight")
    plt.close(error_figure)
    output_paths.append(error_path)

    return output_paths


def _write_training_reports(artifact: dict, report_dir: Path) -> list[Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    output_paths = [
        _write_training_summary(artifact, report_dir),
        _write_subset_metrics_csv(artifact, report_dir),
        _write_loss_history_csv(artifact, report_dir),
        _write_error_summary_csv(artifact, report_dir),
    ]
    output_paths.extend(_write_plots(artifact, report_dir))
    return output_paths


def _resolved_option(args, name: str, overrides: dict[str, Any]):
    value = getattr(args, name)
    if value is not None:
        return value
    return overrides[name]


def _resolve_training_options(args) -> dict[str, Any]:
    profile = QUALITY_PROFILES[args.quality_profile]
    return {
        "epochs": int(_resolved_option(args, "epochs", profile)),
        "batch_size": int(_resolved_option(args, "batch_size", profile)),
        "learning_rate": float(_resolved_option(args, "learning_rate", profile)),
        "noise_std": float(_resolved_option(args, "noise_std", profile)),
        "validation_fraction": float(_resolved_option(args, "validation_fraction", profile)),
        "log_interval": int(_resolved_option(args, "log_interval", profile)),
        "device": str(_resolved_option(args, "device", profile)),
        "weight_decay": float(_resolved_option(args, "weight_decay", profile)),
        "patience": int(_resolved_option(args, "patience", profile)),
        "min_delta": float(_resolved_option(args, "min_delta", profile)),
        "scheduler_factor": float(_resolved_option(args, "scheduler_factor", profile)),
        "scheduler_patience": int(_resolved_option(args, "scheduler_patience", profile)),
        "use_amp": bool(_resolved_option(args, "use_amp", profile)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Train a PyTorch public anomaly model from the BIDMC PPG and Respiration Dataset."
    )
    parser.add_argument(
        "--dataset-dir",
        help="Path to an extracted BIDMC dataset directory containing *_Numerics.csv files.",
    )
    parser.add_argument(
        "--download-dir",
        help="Directory to download and extract the BIDMC dataset into. If omitted, --dataset-dir is required.",
    )
    parser.add_argument(
        "--output",
        default=str(REPO_ROOT / "api" / "models" / "bidmc_public_model.pt"),
        help="Path to write the trained PyTorch artifact.",
    )
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--step-seconds", type=int, default=30)
    parser.add_argument("--quality-profile", choices=sorted(QUALITY_PROFILES), default="balanced")
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

    training_options = _resolve_training_options(args)
    report_dir = _report_dir(args.output, args.report_dir)
    log_file = Path(args.log_file) if args.log_file else report_dir / "training.log"
    _setup_logging(log_file)
    progress = _ProgressTracker(args.progress)

    dataset_dir = args.dataset_dir
    if not dataset_dir:
        if not args.download_dir:
            parser.error("Either --dataset-dir or --download-dir is required.")
        print(f"Downloading BIDMC dataset from {BIDMC_DATASET_URL} ...")
        dataset_dir = str(download_bidmc_dataset(args.download_dir))

    LOGGER.info(
        "starting training profile=%s device=%s output=%s report_dir=%s log_file=%s",
        args.quality_profile,
        training_options["device"],
        args.output,
        report_dir,
        log_file,
    )
    with progress.redirect_context:
        artifact = train_bidmc_public_model(
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
            progress_callback=lambda event: (progress.handle(event), _log_training_event(event)),
        )
    progress.close()
    report_paths = _write_training_reports(artifact, report_dir)

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
