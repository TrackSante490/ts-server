import csv
from datetime import datetime
import math
import pickle
import random
import statistics
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Callable

try:
    import torch
    from torch import nn, optim
    from torch.utils.data import DataLoader, TensorDataset
except Exception:
    torch = None
    nn = None
    optim = None
    DataLoader = None
    TensorDataset = None


BIDMC_DATASET_URL = "https://physionet.org/content/bidmc/get-zip/1.0.0/"
BIDMC_DATASET_NAME = "bidmc-ppg-and-respiration-1.0.0"
BIDMC_METRIC_COLUMNS = {
    "heart_rate": "HR",
    "respiratory_rate": "RESP",
    "spo2": "SpO2",
}
BIDMC_MODEL_SUBSETS: dict[str, tuple[str, ...]] = {
    "hr_rr_spo2": ("heart_rate", "respiratory_rate", "spo2"),
    "hr_spo2": ("heart_rate", "spo2"),
    "hr_rr": ("heart_rate", "respiratory_rate"),
    "rr_spo2": ("respiratory_rate", "spo2"),
    "hr": ("heart_rate",),
    "spo2": ("spo2",),
    "rr": ("respiratory_rate",),
}
BIDMC_FEATURE_NAMES = ("latest", "mean", "min", "max", "span")

PPG_DALIA_DATASET_URL = "https://cdn.uci-ics-mlr-prod.aws.uci.edu/495/ppg%2Bdalia.zip"
PPG_DALIA_DATASET_NAME = "ppg-dalia"
PPG_DALIA_MODEL_SUBSETS: dict[str, tuple[str, ...]] = {
    "hr_temperature": ("heart_rate", "temperature"),
    "heart_rate": ("heart_rate",),
    "temperature": ("temperature",),
}
PPG_DALIA_FEATURE_NAMES = BIDMC_FEATURE_NAMES

OCCUPANCY_DETECTION_DATASET_URL = "https://archive.ics.uci.edu/static/public/357/occupancy%2Bdetection.zip"
OCCUPANCY_DETECTION_DATASET_NAME = "occupancy-detection"
OCCUPANCY_DETECTION_MODEL_SUBSETS: dict[str, tuple[str, ...]] = {
    "ambient_temperature_humidity_co2": ("ambient_temperature", "humidity", "co2"),
    "ambient_temperature_humidity": ("ambient_temperature", "humidity"),
    "ambient_temperature_co2": ("ambient_temperature", "co2"),
    "humidity_co2": ("humidity", "co2"),
    "ambient_temperature": ("ambient_temperature",),
    "humidity": ("humidity",),
    "co2": ("co2",),
}
OCCUPANCY_FEATURE_NAMES = BIDMC_FEATURE_NAMES

PUBLIC_INFERENCE_DISABLED_SUBSETS: dict[str, set[str]] = {
    BIDMC_DATASET_NAME: {"hr"},
    OCCUPANCY_DETECTION_DATASET_NAME: {"ambient_temperature", "humidity", "co2"},
}


class _SubsetAutoencoder(nn.Module if nn is not None else object):
    def __init__(self, input_dim: int, hidden_dim: int, latent_dim: int):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, latent_dim),
            nn.ReLU(),
        )
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, input_dim),
        )

    def forward(self, x):
        return self.decoder(self.encoder(x))


def _require_torch() -> None:
    if torch is None or nn is None or optim is None or DataLoader is None or TensorDataset is None:
        raise RuntimeError("PyTorch is required for training and loading the public sensor model.")


def _as_float(value: str | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)) and len(value) == 1:
        value = value[0]
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            value = value.item()
        except Exception:
            pass
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = float(raw)
    except ValueError:
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * max(0.0, min(1.0, quantile))
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _download_zip_dataset(dataset_url: str, target_dir: str | Path, *, zip_name: str) -> Path:
    target_path = Path(target_dir)
    target_path.mkdir(parents=True, exist_ok=True)
    zip_path = target_path / zip_name
    with urllib.request.urlopen(dataset_url) as response, zip_path.open("wb") as output_handle:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            output_handle.write(chunk)
    return zip_path


def _extract_zip(zip_path: str | Path, output_dir: str | Path) -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as archive:
        archive.extractall(output_path)
    return output_path


def _iter_bidmc_numeric_files(dataset_dir: Path) -> list[Path]:
    return sorted(dataset_dir.rglob("*_Numerics.csv"))


def _load_bidmc_rows(csv_path: Path) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, skipinitialspace=True)
        for raw_row in reader:
            row: dict[str, float] = {}
            time_value = _as_float(raw_row.get("Time [s]"))
            if time_value is None:
                continue
            row["time_seconds"] = time_value
            for metric, column in BIDMC_METRIC_COLUMNS.items():
                metric_value = _as_float(raw_row.get(column))
                if metric_value is not None:
                    row[metric] = metric_value
            if len(row) > 1:
                rows.append(row)
    return rows


def _window_metric_summary(rows: list[dict[str, float]], metric: str) -> dict[str, float] | None:
    values = [float(row[metric]) for row in rows if metric in row]
    if not values:
        return None
    return {
        "latest": values[-1],
        "mean": statistics.fmean(values),
        "min": min(values),
        "max": max(values),
        "span": max(values) - min(values),
    }


def _window_feature_vectors(
    rows: list[dict[str, float]],
    *,
    window_seconds: int,
    step_seconds: int,
    metrics: tuple[str, ...] | list[str] | None = None,
) -> list[dict[str, Any]]:
    if not rows:
        return []

    selected_metrics = tuple(metrics or BIDMC_METRIC_COLUMNS.keys())
    deltas = [
        max(0.0, float(rows[index + 1]["time_seconds"]) - float(rows[index]["time_seconds"]))
        for index in range(len(rows) - 1)
        if float(rows[index + 1]["time_seconds"]) > float(rows[index]["time_seconds"])
    ]
    median_delta = statistics.median(deltas) if deltas else 1.0
    expected_points = max(1, int(round(window_seconds / max(median_delta, 1e-6))))
    min_points = max(5, int(math.ceil(expected_points * 0.6)))
    windows: list[dict[str, Any]] = []
    start_idx = 0
    end_idx = 0
    total_rows = len(rows)

    while start_idx < total_rows:
        start_time = rows[start_idx]["time_seconds"]
        window_end = start_time + window_seconds
        while end_idx < total_rows and rows[end_idx]["time_seconds"] < window_end:
            end_idx += 1

        window_rows = rows[start_idx:end_idx]
        if len(window_rows) >= min_points:
            metrics: dict[str, dict[str, float]] = {}
            for metric in selected_metrics:
                summary = _window_metric_summary(window_rows, metric)
                if summary is not None:
                    metrics[metric] = summary
            if metrics:
                windows.append(
                    {
                        "metrics": metrics,
                        "window_started_at": start_time,
                        "window_ended_at": window_rows[-1]["time_seconds"],
                    }
                )

        next_time = start_time + step_seconds
        while start_idx < total_rows and rows[start_idx]["time_seconds"] < next_time:
            start_idx += 1
        if end_idx < start_idx:
            end_idx = start_idx

    return windows


def _feature_vector_from_metrics(
    metric_summaries: dict[str, dict[str, float]],
    metrics: tuple[str, ...],
) -> tuple[list[str], list[float]] | None:
    feature_names: list[str] = []
    feature_values: list[float] = []
    for metric in metrics:
        summary = metric_summaries.get(metric)
        if summary is None:
            return None
        for feature_name in BIDMC_FEATURE_NAMES:
            value = summary.get(feature_name)
            if value is None:
                return None
            feature_names.append(f"{metric}.{feature_name}")
            feature_values.append(float(value))
    return feature_names, feature_values


def _subset_dimensions(input_dim: int) -> tuple[int, int]:
    hidden_dim = max(8, input_dim * 2)
    latent_dim = max(2, input_dim // 2)
    return hidden_dim, latent_dim


def _build_autoencoder(input_dim: int, hidden_dim: int | None = None, latent_dim: int | None = None):
    _require_torch()
    resolved_hidden, resolved_latent = _subset_dimensions(input_dim)
    if hidden_dim is not None:
        resolved_hidden = hidden_dim
    if latent_dim is not None:
        resolved_latent = latent_dim
    return _SubsetAutoencoder(input_dim, resolved_hidden, resolved_latent)


def _clone_state_dict(model) -> dict[str, Any]:
    return {key: value.detach().cpu().clone() for key, value in model.state_dict().items()}


def _resolve_training_device(device: str | None) -> str:
    _require_torch()
    requested = (device or "auto").strip().lower()
    if requested == "auto":
        return "cuda" if torch.cuda.is_available() else "cpu"
    if requested not in {"cpu", "cuda"}:
        raise ValueError("device must be one of: auto, cpu, cuda")
    if requested == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA was requested for training, but no CUDA device is available.")
    return requested


def _split_train_validation(
    normalized,
    *,
    validation_fraction: float,
    seed: int,
):
    sample_count = int(normalized.shape[0])
    if sample_count < 8 or validation_fraction <= 0:
        return normalized, None

    validation_count = int(round(sample_count * validation_fraction))
    validation_count = max(1, min(validation_count, sample_count - 1))
    if validation_count <= 0 or validation_count >= sample_count:
        return normalized, None

    generator = torch.Generator().manual_seed(seed)
    permutation = torch.randperm(sample_count, generator=generator)
    validation_indices = permutation[:validation_count]
    train_indices = permutation[validation_count:]
    return normalized[train_indices], normalized[validation_indices]


def _mean_reconstruction_errors(model, features, *, device, amp_enabled: bool):
    if features is None or int(features.shape[0]) == 0:
        return None, []
    features = features.to(device)
    with torch.no_grad():
        with torch.autocast(device_type=device.type, dtype=torch.float16, enabled=amp_enabled):
            reconstructed = model(features)
            feature_errors = (reconstructed - features).pow(2)
        sample_errors = feature_errors.mean(dim=1)
    return float(sample_errors.mean().item()), [float(value) for value in sample_errors.tolist()]


def _should_log_epoch(epoch: int, total_epochs: int, log_interval: int) -> bool:
    if epoch == 1 or epoch == total_epochs:
        return True
    if log_interval <= 0:
        return False
    return epoch % log_interval == 0


def _fit_subset_model(
    subset_name: str,
    vectors: list[list[float]],
    feature_names: list[str],
    *,
    epochs: int,
    batch_size: int,
    learning_rate: float,
    noise_std: float,
    validation_fraction: float,
    seed: int,
    log_interval: int,
    device: str,
    weight_decay: float,
    patience: int,
    min_delta: float,
    scheduler_factor: float,
    scheduler_patience: int,
    use_amp: bool,
    progress_callback: Callable[[dict[str, Any]], None] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    _require_torch()
    features = torch.tensor(vectors, dtype=torch.float32)
    mean = features.mean(dim=0)
    std = features.std(dim=0, unbiased=False)
    std = torch.where(std < 1e-6, torch.ones_like(std), std)
    normalized = (features - mean) / std
    train_features, validation_features = _split_train_validation(
        normalized,
        validation_fraction=validation_fraction,
        seed=seed,
    )

    resolved_device = torch.device(device)
    amp_enabled = bool(use_amp and resolved_device.type == "cuda")
    hidden_dim, latent_dim = _subset_dimensions(train_features.shape[1])
    model = _build_autoencoder(train_features.shape[1], hidden_dim=hidden_dim, latent_dim=latent_dim).to(resolved_device)
    dataset = TensorDataset(train_features)
    loader = DataLoader(
        dataset,
        batch_size=min(batch_size, len(dataset)),
        shuffle=True,
        pin_memory=resolved_device.type == "cuda",
    )
    optimizer = optim.Adam(model.parameters(), lr=learning_rate, weight_decay=weight_decay)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(
        optimizer,
        mode="min",
        factor=max(0.1, min(0.95, scheduler_factor)),
        patience=max(1, scheduler_patience),
    )
    loss_fn = nn.MSELoss()
    scaler = torch.amp.GradScaler("cuda", enabled=amp_enabled)

    model.train()
    final_train_loss = 0.0
    final_validation_loss = None
    best_monitor = math.inf
    best_epoch = 1
    best_state = _clone_state_dict(model)
    loss_history: list[dict[str, float | int | None]] = []
    no_improvement_epochs = 0
    early_stopped = False

    for epoch in range(1, max(1, epochs) + 1):
        batch_losses: list[float] = []
        for (batch,) in loader:
            batch = batch.to(resolved_device, non_blocking=resolved_device.type == "cuda")
            optimizer.zero_grad(set_to_none=True)
            noisy_batch = batch + (torch.randn_like(batch) * noise_std)
            with torch.autocast(device_type=resolved_device.type, dtype=torch.float16, enabled=amp_enabled):
                reconstructed = model(noisy_batch)
                loss = loss_fn(reconstructed, batch)
            scaler.scale(loss).backward()
            scaler.step(optimizer)
            scaler.update()
            batch_losses.append(float(loss.item()))

        final_train_loss = sum(batch_losses) / len(batch_losses) if batch_losses else 0.0
        validation_loss, _ = _mean_reconstruction_errors(
            model,
            validation_features,
            device=resolved_device,
            amp_enabled=amp_enabled,
        )
        final_validation_loss = validation_loss
        monitored_loss = validation_loss if validation_loss is not None else final_train_loss
        scheduler.step(monitored_loss)
        if monitored_loss < (best_monitor - max(0.0, min_delta)):
            best_monitor = monitored_loss
            best_epoch = epoch
            best_state = _clone_state_dict(model)
            no_improvement_epochs = 0
        else:
            no_improvement_epochs += 1

        loss_history.append(
            {
                "epoch": epoch,
                "train_loss": final_train_loss,
                "validation_loss": validation_loss,
            }
        )
        if progress_callback and _should_log_epoch(epoch, max(1, epochs), log_interval):
            progress_callback(
                {
                    "event": "epoch",
                    "subset": subset_name,
                    "epoch": epoch,
                    "total_epochs": max(1, epochs),
                    "train_loss": final_train_loss,
                    "validation_loss": validation_loss,
                    "learning_rate": float(optimizer.param_groups[0]["lr"]),
                    "device": resolved_device.type,
                }
            )

        if patience > 0 and no_improvement_epochs >= patience:
            early_stopped = True
            break

    model.load_state_dict(best_state)
    model.eval()
    _, train_errors = _mean_reconstruction_errors(
        model,
        train_features,
        device=resolved_device,
        amp_enabled=amp_enabled,
    )
    _, validation_errors = _mean_reconstruction_errors(
        model,
        validation_features,
        device=resolved_device,
        amp_enabled=amp_enabled,
    )
    threshold_errors = validation_errors or train_errors

    epochs_completed = len(loss_history)
    threshold = max(0.01, _percentile(threshold_errors, 0.98))
    artifact_model = {
        "feature_names": feature_names,
        "architecture": {
            "input_dim": train_features.shape[1],
            "hidden_dim": hidden_dim,
            "latent_dim": latent_dim,
        },
        "normalization": {
            "mean": mean.tolist(),
            "std": std.tolist(),
        },
        "threshold": threshold,
        "threshold_source": "validation" if validation_errors else "train",
        "distance_p95": _percentile(threshold_errors, 0.95),
        "distance_p98": _percentile(threshold_errors, 0.98),
        "train_loss": final_train_loss,
        "validation_loss": final_validation_loss,
        "best_epoch": best_epoch,
        "best_validation_loss": None if math.isinf(best_monitor) else best_monitor,
        "epochs_completed": epochs_completed,
        "early_stopped": early_stopped,
        "train_windows": int(train_features.shape[0]),
        "validation_windows": int(validation_features.shape[0]) if validation_features is not None else 0,
        "device": resolved_device.type,
        "amp_enabled": amp_enabled,
        "weight_decay": weight_decay,
        "state_dict": best_state,
    }
    report = {
        "loss_history": loss_history,
        "train_errors": train_errors,
        "validation_errors": validation_errors,
    }
    if progress_callback:
        progress_callback(
            {
                "event": "subset_complete",
                "subset": subset_name,
                "completed_epochs": epochs_completed,
                "total_epochs": max(1, epochs),
                "best_epoch": best_epoch,
                "best_validation_loss": None if math.isinf(best_monitor) else best_monitor,
                "early_stopped": early_stopped,
                "device": resolved_device.type,
            }
        )
    return artifact_model, report


def _train_public_metric_artifact(
    *,
    subset_vectors: dict[str, list[list[float]]],
    subset_features: dict[str, list[str]],
    metric_subsets: dict[str, tuple[str, ...]],
    dataset_name: str,
    dataset_url: str,
    model_name: str,
    model_group: str,
    output_path: str | Path,
    source_files: int,
    window_seconds: int,
    step_seconds: int,
    epochs: int,
    batch_size: int,
    learning_rate: float,
    noise_std: float,
    validation_fraction: float,
    log_interval: int,
    seed: int,
    device: str,
    weight_decay: float,
    patience: int,
    min_delta: float,
    scheduler_factor: float,
    scheduler_patience: int,
    use_amp: bool,
    progress_callback: Callable[[dict[str, Any]], None] | None,
) -> dict[str, Any]:
    models: dict[str, Any] = {}
    reports: dict[str, Any] = {}
    for subset_name, vectors in subset_vectors.items():
        if not vectors:
            continue
        feature_names = subset_features.get(subset_name)
        if not feature_names:
            continue
        model_payload, report_payload = _fit_subset_model(
            subset_name,
            vectors,
            feature_names,
            epochs=epochs,
            batch_size=batch_size,
            learning_rate=learning_rate,
            noise_std=noise_std,
            validation_fraction=validation_fraction,
            seed=seed,
            log_interval=log_interval,
            device=device,
            weight_decay=weight_decay,
            patience=patience,
            min_delta=min_delta,
            scheduler_factor=scheduler_factor,
            scheduler_patience=scheduler_patience,
            use_amp=use_amp,
            progress_callback=progress_callback,
        )
        models[subset_name] = {
            "metrics": list(metric_subsets[subset_name]),
            **model_payload,
        }
        reports[subset_name] = report_payload

    if not models:
        raise ValueError(f"Could not train any {dataset_name} subset models from the provided dataset")

    artifact = {
        "schema_version": 2,
        "model_format": "pytorch",
        "model_name": model_name,
        "model_group": model_group,
        "dataset_name": dataset_name,
        "dataset_url": dataset_url,
        "window_seconds": window_seconds,
        "step_seconds": step_seconds,
        "source_files": source_files,
        "training": {
            "epochs": epochs,
            "batch_size": batch_size,
            "learning_rate": learning_rate,
            "noise_std": noise_std,
            "validation_fraction": validation_fraction,
            "log_interval": log_interval,
            "seed": seed,
            "device": device,
            "weight_decay": weight_decay,
            "patience": patience,
            "min_delta": min_delta,
            "scheduler_factor": scheduler_factor,
            "scheduler_patience": scheduler_patience,
            "use_amp": bool(use_amp and device == "cuda"),
        },
        "models": models,
        "_reports": reports,
    }

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    torch.save({key: value for key, value in artifact.items() if key != "_reports"}, output_file)
    return artifact


def train_bidmc_public_model(
    dataset_dir: str | Path,
    output_path: str | Path,
    *,
    window_seconds: int = 60,
    step_seconds: int = 30,
    epochs: int = 120,
    batch_size: int = 64,
    learning_rate: float = 1e-3,
    noise_std: float = 0.05,
    validation_fraction: float = 0.2,
    log_interval: int = 10,
    seed: int = 42,
    device: str = "auto",
    weight_decay: float = 0.0,
    patience: int = 0,
    min_delta: float = 0.0,
    scheduler_factor: float = 0.5,
    scheduler_patience: int = 8,
    use_amp: bool = True,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    _require_torch()
    torch.manual_seed(seed)
    random.seed(seed)
    resolved_device = _resolve_training_device(device)

    source_dir = Path(dataset_dir)
    numeric_files = _iter_bidmc_numeric_files(source_dir)
    if not numeric_files:
        raise FileNotFoundError(f"No BIDMC numerics CSV files found under {source_dir}")

    subset_vectors: dict[str, list[list[float]]] = {subset: [] for subset in BIDMC_MODEL_SUBSETS}
    subset_features: dict[str, list[str]] = {}
    source_files = 0

    for numeric_file in numeric_files:
        rows = _load_bidmc_rows(numeric_file)
        if not rows:
            continue
        source_files += 1
        windows = _window_feature_vectors(rows, window_seconds=window_seconds, step_seconds=step_seconds)
        for window in windows:
            metric_summaries = window["metrics"]
            for subset_name, metrics in BIDMC_MODEL_SUBSETS.items():
                feature_vector = _feature_vector_from_metrics(metric_summaries, metrics)
                if feature_vector is None:
                    continue
                feature_names, values = feature_vector
                subset_features[subset_name] = feature_names
                subset_vectors[subset_name].append(values)

    return _train_public_metric_artifact(
        subset_vectors=subset_vectors,
        subset_features=subset_features,
        metric_subsets=BIDMC_MODEL_SUBSETS,
        dataset_name=BIDMC_DATASET_NAME,
        dataset_url=BIDMC_DATASET_URL,
        model_name="bidmc-public-autoencoder-v1",
        model_group="physiology",
        output_path=output_path,
        source_files=source_files,
        window_seconds=window_seconds,
        step_seconds=step_seconds,
        epochs=epochs,
        batch_size=batch_size,
        learning_rate=learning_rate,
        noise_std=noise_std,
        validation_fraction=validation_fraction,
        log_interval=log_interval,
        seed=seed,
        device=resolved_device,
        weight_decay=weight_decay,
        patience=patience,
        min_delta=min_delta,
        scheduler_factor=scheduler_factor,
        scheduler_patience=scheduler_patience,
        use_amp=use_amp,
        progress_callback=progress_callback,
    )


def _iter_ppg_dalia_subject_pickles(dataset_dir: Path) -> list[Path]:
    return sorted(dataset_dir.rglob("S*.pkl"))


def _load_ppg_dalia_rows(pkl_path: Path) -> list[dict[str, float]]:
    with pkl_path.open("rb") as handle:
        try:
            payload = pickle.load(handle, encoding="latin1")
        except TypeError:
            payload = pickle.load(handle)

    if not isinstance(payload, dict):
        return []

    data_root = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    labels = data_root.get("label")
    signals = data_root.get("signal") or {}
    wrist = signals.get("wrist") if isinstance(signals, dict) else {}
    wrist_temp = wrist.get("TEMP") if isinstance(wrist, dict) else None

    if labels is None:
        return []

    raw_labels = list(labels)
    if not raw_labels:
        return []

    wrist_temp_values: list[float] = []
    if wrist_temp is not None:
        for raw_value in list(wrist_temp):
            if isinstance(raw_value, (list, tuple)):
                candidate = raw_value[0] if raw_value else None
            else:
                candidate = raw_value
            parsed = _as_float(candidate)
            if parsed is not None:
                wrist_temp_values.append(parsed)

    rows: list[dict[str, float]] = []
    temp_rate_hz = 4.0
    label_step_seconds = 2.0
    label_window_seconds = 8.0
    for index, raw_label in enumerate(raw_labels):
        parsed_label = _as_float(raw_label)
        if parsed_label is None:
            continue
        row = {
            "time_seconds": index * label_step_seconds,
            "heart_rate": float(parsed_label),
        }
        if wrist_temp_values:
            start_index = int(index * label_step_seconds * temp_rate_hz)
            end_index = int((index * label_step_seconds + label_window_seconds) * temp_rate_hz)
            temp_segment = wrist_temp_values[start_index:end_index]
            if temp_segment:
                row["temperature"] = statistics.fmean(temp_segment)
        rows.append(row)
    return rows


def train_ppg_dalia_public_model(
    dataset_dir: str | Path,
    output_path: str | Path,
    *,
    window_seconds: int = 60,
    step_seconds: int = 30,
    epochs: int = 120,
    batch_size: int = 64,
    learning_rate: float = 1e-3,
    noise_std: float = 0.05,
    validation_fraction: float = 0.2,
    log_interval: int = 10,
    seed: int = 42,
    device: str = "auto",
    weight_decay: float = 0.0,
    patience: int = 0,
    min_delta: float = 0.0,
    scheduler_factor: float = 0.5,
    scheduler_patience: int = 8,
    use_amp: bool = True,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    _require_torch()
    torch.manual_seed(seed)
    random.seed(seed)
    resolved_device = _resolve_training_device(device)

    source_dir = Path(dataset_dir)
    subject_pickles = _iter_ppg_dalia_subject_pickles(source_dir)
    if not subject_pickles:
        raise FileNotFoundError(f"No PPG-DaLiA subject pickle files were found under {source_dir}")

    subset_vectors: dict[str, list[list[float]]] = {subset: [] for subset in PPG_DALIA_MODEL_SUBSETS}
    subset_features: dict[str, list[str]] = {}
    source_files = 0

    for subject_pickle in subject_pickles:
        rows = _load_ppg_dalia_rows(subject_pickle)
        if not rows:
            continue
        source_files += 1
        windows = _window_feature_vectors(
            rows,
            window_seconds=window_seconds,
            step_seconds=step_seconds,
            metrics=("heart_rate", "temperature"),
        )
        for window in windows:
            metric_summaries = window["metrics"]
            for subset_name, metrics in PPG_DALIA_MODEL_SUBSETS.items():
                feature_vector = _feature_vector_from_metrics(metric_summaries, metrics)
                if feature_vector is None:
                    continue
                feature_names, values = feature_vector
                subset_features[subset_name] = feature_names
                subset_vectors[subset_name].append(values)

    return _train_public_metric_artifact(
        subset_vectors=subset_vectors,
        subset_features=subset_features,
        metric_subsets=PPG_DALIA_MODEL_SUBSETS,
        dataset_name=PPG_DALIA_DATASET_NAME,
        dataset_url=PPG_DALIA_DATASET_URL,
        model_name="ppg-dalia-public-autoencoder-v1",
        model_group="physiology",
        output_path=output_path,
        source_files=source_files,
        window_seconds=window_seconds,
        step_seconds=step_seconds,
        epochs=epochs,
        batch_size=batch_size,
        learning_rate=learning_rate,
        noise_std=noise_std,
        validation_fraction=validation_fraction,
        log_interval=log_interval,
        seed=seed,
        device=resolved_device,
        weight_decay=weight_decay,
        patience=patience,
        min_delta=min_delta,
        scheduler_factor=scheduler_factor,
        scheduler_patience=scheduler_patience,
        use_amp=use_amp,
        progress_callback=progress_callback,
    )


def _iter_occupancy_csv_files(dataset_dir: Path) -> list[Path]:
    candidates = list(dataset_dir.rglob("*.txt")) + list(dataset_dir.rglob("*.csv"))
    return sorted(path for path in candidates if path.is_file())


def _load_occupancy_rows(csv_path: Path) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            return rows
        normalized_header = [str(value).strip().strip('"').lower() for value in header]
        for raw_values in reader:
            values = [str(value).strip().strip('"') for value in raw_values]
            if not values:
                continue

            # UCI's occupancy files include a leading row index in the data rows
            # even though the header starts at "date".
            if len(values) == len(normalized_header) + 1 and values[1]:
                values = values[1:]

            row_map = {
                normalized_header[index]: values[index]
                for index in range(min(len(normalized_header), len(values)))
            }

            date_raw = str(row_map.get("date") or "").strip()
            if not date_raw:
                continue

            parsed_dt = None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
                try:
                    parsed_dt = datetime.strptime(date_raw, fmt)
                    break
                except ValueError:
                    continue
            if parsed_dt is None:
                continue

            ambient_temperature = _as_float(row_map.get("temperature"))
            humidity = _as_float(row_map.get("humidity"))
            co2 = _as_float(row_map.get("co2"))
            if ambient_temperature is None and humidity is None and co2 is None:
                continue
            rows.append(
                {
                    "_datetime": parsed_dt,
                    "ambient_temperature": ambient_temperature,
                    "humidity": humidity,
                    "co2": co2,
                }
            )

    if not rows:
        return []

    rows.sort(key=lambda row: row["_datetime"])
    start_dt = rows[0]["_datetime"]
    output_rows: list[dict[str, float]] = []
    for row in rows:
        output_row = {
            "time_seconds": max(0.0, (row["_datetime"] - start_dt).total_seconds()),
        }
        for metric in ("ambient_temperature", "humidity", "co2"):
            value = row.get(metric)
            if value is not None:
                output_row[metric] = float(value)
        output_rows.append(output_row)
    return output_rows


def train_occupancy_public_model(
    dataset_dir: str | Path,
    output_path: str | Path,
    *,
    window_seconds: int = 1800,
    step_seconds: int = 600,
    epochs: int = 120,
    batch_size: int = 64,
    learning_rate: float = 1e-3,
    noise_std: float = 0.05,
    validation_fraction: float = 0.2,
    log_interval: int = 10,
    seed: int = 42,
    device: str = "auto",
    weight_decay: float = 0.0,
    patience: int = 0,
    min_delta: float = 0.0,
    scheduler_factor: float = 0.5,
    scheduler_patience: int = 8,
    use_amp: bool = True,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    _require_torch()
    torch.manual_seed(seed)
    random.seed(seed)
    resolved_device = _resolve_training_device(device)

    source_dir = Path(dataset_dir)
    occupancy_files = _iter_occupancy_csv_files(source_dir)
    if not occupancy_files:
        raise FileNotFoundError(f"No occupancy CSV files were found under {source_dir}")

    subset_vectors: dict[str, list[list[float]]] = {subset: [] for subset in OCCUPANCY_DETECTION_MODEL_SUBSETS}
    subset_features: dict[str, list[str]] = {}
    source_files = 0

    for occupancy_file in occupancy_files:
        rows = _load_occupancy_rows(occupancy_file)
        if not rows:
            continue
        source_files += 1
        windows = _window_feature_vectors(
            rows,
            window_seconds=window_seconds,
            step_seconds=step_seconds,
            metrics=("ambient_temperature", "humidity", "co2"),
        )
        for window in windows:
            metric_summaries = window["metrics"]
            for subset_name, metrics in OCCUPANCY_DETECTION_MODEL_SUBSETS.items():
                feature_vector = _feature_vector_from_metrics(metric_summaries, metrics)
                if feature_vector is None:
                    continue
                feature_names, values = feature_vector
                subset_features[subset_name] = feature_names
                subset_vectors[subset_name].append(values)

    return _train_public_metric_artifact(
        subset_vectors=subset_vectors,
        subset_features=subset_features,
        metric_subsets=OCCUPANCY_DETECTION_MODEL_SUBSETS,
        dataset_name=OCCUPANCY_DETECTION_DATASET_NAME,
        dataset_url=OCCUPANCY_DETECTION_DATASET_URL,
        model_name="environment-context-public-autoencoder-v1",
        model_group="environment",
        output_path=output_path,
        source_files=source_files,
        window_seconds=window_seconds,
        step_seconds=step_seconds,
        epochs=epochs,
        batch_size=batch_size,
        learning_rate=learning_rate,
        noise_std=noise_std,
        validation_fraction=validation_fraction,
        log_interval=log_interval,
        seed=seed,
        device=resolved_device,
        weight_decay=weight_decay,
        patience=patience,
        min_delta=min_delta,
        scheduler_factor=scheduler_factor,
        scheduler_patience=scheduler_patience,
        use_amp=use_amp,
        progress_callback=progress_callback,
    )


def download_bidmc_dataset(target_dir: str | Path) -> Path:
    target_path = Path(target_dir)
    zip_path = _download_zip_dataset(BIDMC_DATASET_URL, target_path, zip_name="bidmc_dataset.zip")
    _extract_zip(zip_path, target_path)

    for child in target_path.iterdir():
        if child.is_dir() and child.name.startswith("bidmc"):
            return child
    raise FileNotFoundError("Downloaded BIDMC archive did not contain an extracted dataset directory")


def download_ppg_dalia_dataset(target_dir: str | Path) -> Path:
    target_path = Path(target_dir)
    zip_path = _download_zip_dataset(PPG_DALIA_DATASET_URL, target_path, zip_name="ppg_dalia_dataset.zip")
    extracted_outer = _extract_zip(zip_path, target_path / "outer")
    inner_zip = extracted_outer / "data.zip"
    if not inner_zip.exists():
        raise FileNotFoundError("Downloaded PPG-DaLiA archive did not contain data.zip")
    extracted_data = _extract_zip(inner_zip, target_path / "data")
    if any(extracted_data.rglob("S*.pkl")):
        for candidate in extracted_data.rglob("S1.pkl"):
            return candidate.parent.parent if candidate.parent.name.startswith("S") else candidate.parent
        return extracted_data
    for child in extracted_data.rglob("*"):
        if child.is_dir() and child.name.startswith("S"):
            parent = child.parent
            return parent if any(parent.glob("S*")) else extracted_data
    raise FileNotFoundError("Extracted PPG-DaLiA dataset did not contain subject folders")


def download_occupancy_detection_dataset(target_dir: str | Path) -> Path:
    target_path = Path(target_dir)
    zip_path = _download_zip_dataset(
        OCCUPANCY_DETECTION_DATASET_URL,
        target_path,
        zip_name="occupancy_detection_dataset.zip",
    )
    extracted = _extract_zip(zip_path, target_path / "data")
    if _iter_occupancy_csv_files(extracted):
        return extracted
    raise FileNotFoundError("Downloaded occupancy detection archive did not contain CSV files")


def load_public_model(model_path: str | Path) -> dict[str, Any] | None:
    path = Path(model_path)
    if not path.exists():
        return None
    if torch is None:
        return {
            "load_error": "missing_dependency",
            "message": "PyTorch is not installed, so the public model cannot be loaded.",
            "model_path": str(path),
        }
    try:
        artifact = torch.load(path, map_location="cpu", weights_only=False)
    except TypeError:
        artifact = torch.load(path, map_location="cpu")
    if not isinstance(artifact, dict):
        raise ValueError("Unsupported public model artifact")
    return artifact


def summarize_public_artifact_training(artifact: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(artifact, dict):
        return {
            "ready": False,
            "score": math.inf,
            "dataset_name": None,
            "model_name": None,
            "source_files": 0,
            "subsets": [],
        }

    models = artifact.get("models")
    if not isinstance(models, dict) or not models:
        return {
            "ready": False,
            "score": math.inf,
            "dataset_name": artifact.get("dataset_name"),
            "model_name": artifact.get("model_name"),
            "source_files": int(artifact.get("source_files") or 0),
            "subsets": [],
        }

    weighted_score = 0.0
    total_weight = 0.0
    total_train_windows = 0
    subset_rows: list[dict[str, Any]] = []
    for subset_name, model in sorted(models.items()):
        if not isinstance(model, dict):
            continue
        metrics = [str(metric) for metric in (model.get("metrics") or [])]
        best_validation_loss = model.get("best_validation_loss")
        validation_loss = model.get("validation_loss")
        train_loss = model.get("train_loss")

        candidate_loss = None
        for value in (best_validation_loss, validation_loss, train_loss):
            if isinstance(value, (int, float)) and math.isfinite(float(value)):
                candidate_loss = float(value)
                break
        if candidate_loss is None:
            continue

        metric_weight = max(1, len(metrics))
        weighted_score += candidate_loss * metric_weight
        total_weight += metric_weight
        total_train_windows += int(model.get("train_windows") or 0)
        subset_rows.append(
            {
                "subset": subset_name,
                "metrics": metrics,
                "metric_weight": metric_weight,
                "candidate_loss": candidate_loss,
                "best_validation_loss": best_validation_loss,
                "validation_loss": validation_loss,
                "train_loss": train_loss,
                "train_windows": int(model.get("train_windows") or 0),
            }
        )

    aggregate_score = (weighted_score / total_weight) if total_weight else math.inf
    return {
        "ready": bool(total_weight),
        "score": aggregate_score,
        "dataset_name": artifact.get("dataset_name"),
        "model_name": artifact.get("model_name"),
        "model_group": artifact.get("model_group"),
        "source_files": int(artifact.get("source_files") or 0),
        "total_train_windows": total_train_windows,
        "subsets": subset_rows,
    }


def _subset_candidate_loss(subset_model: dict[str, Any] | None) -> float | None:
    if not isinstance(subset_model, dict):
        return None
    for key in ("best_validation_loss", "validation_loss", "train_loss"):
        value = subset_model.get(key)
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            return float(value)
    return None


def _subset_inference_policy(
    artifact: dict[str, Any],
    subset_name: str,
    subset_model: dict[str, Any],
) -> tuple[bool, int, str | None]:
    dataset_name = str(artifact.get("dataset_name") or "")
    disabled = PUBLIC_INFERENCE_DISABLED_SUBSETS.get(dataset_name) or set()
    if subset_name in disabled:
        return False, 0, "disabled_subset"

    metrics = tuple(str(metric) for metric in (subset_model.get("metrics") or []))
    metric_count = len(metrics)
    priority = metric_count * 100

    candidate_loss = _subset_candidate_loss(subset_model)
    if candidate_loss is not None:
        if candidate_loss <= 0.02:
            priority += 20
        elif candidate_loss <= 0.1:
            priority += 10
        elif candidate_loss >= 0.5:
            priority -= 20

    if dataset_name == PPG_DALIA_DATASET_NAME and subset_name == "heart_rate":
        priority += 15
    if dataset_name == BIDMC_DATASET_NAME and subset_name == "hr_spo2":
        priority += 10
    if dataset_name == OCCUPANCY_DETECTION_DATASET_NAME and metric_count >= 2:
        priority += 10

    return True, priority, None


def _runtime_model_for_subset(artifact: dict[str, Any], subset_name: str, subset_model: dict[str, Any]):
    _require_torch()
    runtime_cache = artifact.setdefault("_runtime_models", {})
    cached = runtime_cache.get(subset_name)
    if cached is not None:
        return cached

    architecture = subset_model.get("architecture") or {}
    model = _build_autoencoder(
        int(architecture.get("input_dim") or 0),
        hidden_dim=int(architecture.get("hidden_dim") or 0),
        latent_dim=int(architecture.get("latent_dim") or 0),
    )
    state_dict = subset_model.get("state_dict") or {}
    model.load_state_dict(state_dict)
    model.eval()
    runtime_cache[subset_name] = model
    return model


def score_public_model(
    artifact: dict[str, Any] | None,
    metric_summaries: dict[str, dict[str, float]],
) -> dict[str, Any]:
    if not artifact:
        return {"ready": False, "status": "missing_artifact"}
    if artifact.get("load_error"):
        return {
            "ready": False,
            "status": str(artifact.get("load_error")),
            "message": artifact.get("message"),
        }
    if torch is None:
        return {
            "ready": False,
            "status": "missing_dependency",
            "message": "PyTorch is not installed.",
        }

    models = artifact.get("models")
    if not isinstance(models, dict) or not models:
        return {"ready": False, "status": "invalid_artifact"}

    selected_name = None
    selected_model = None
    selected_vector = None
    selected_priority = 0
    selected_training_quality = None
    filtered_subsets: list[dict[str, Any]] = []
    for subset_name, subset_model in sorted(
        models.items(),
        key=lambda item: len(item[1].get("metrics") or []),
        reverse=True,
    ):
        metrics = tuple(str(metric) for metric in (subset_model.get("metrics") or []))
        feature_vector = _feature_vector_from_metrics(metric_summaries, metrics)
        if feature_vector is None:
            continue
        enabled, inference_priority, disabled_reason = _subset_inference_policy(artifact, subset_name, subset_model)
        if not enabled:
            filtered_subsets.append(
                {
                    "subset": subset_name,
                    "metrics": list(metrics),
                    "reason": disabled_reason or "disabled",
                }
            )
            continue
        selected_name = subset_name
        selected_model = subset_model
        selected_vector = feature_vector
        selected_priority = inference_priority
        selected_training_quality = _subset_candidate_loss(subset_model)
        break

    if selected_model is None or selected_vector is None or selected_name is None:
        return {
            "ready": False,
            "status": "policy_filtered" if filtered_subsets else "insufficient_metrics",
            "available_metrics": sorted(metric_summaries.keys()),
            "filtered_subsets": filtered_subsets,
        }

    feature_names, values = selected_vector
    normalization = selected_model.get("normalization") or {}
    mean = torch.tensor(normalization.get("mean") or [], dtype=torch.float32)
    std = torch.tensor(normalization.get("std") or [], dtype=torch.float32)
    if mean.numel() != len(values) or std.numel() != len(values):
        return {"ready": False, "status": "invalid_artifact"}

    std = torch.where(std < 1e-6, torch.ones_like(std), std)
    vector = torch.tensor(values, dtype=torch.float32)
    normalized = (vector - mean) / std
    model = _runtime_model_for_subset(artifact, selected_name, selected_model)

    with torch.no_grad():
        reconstructed = model(normalized.unsqueeze(0)).squeeze(0)
        feature_errors = (reconstructed - normalized).pow(2)
        reconstruction_error = float(feature_errors.mean().item())

    threshold = max(1e-6, float(selected_model.get("threshold") or 0.0))
    score = min(1.0, reconstruction_error / (threshold * 1.5))

    metric_contributors: dict[str, list[float]] = {}
    for index, feature_name in enumerate(feature_names):
        metric_name = feature_name.split(".", 1)[0]
        metric_contributors.setdefault(metric_name, []).append(float(feature_errors[index].item()))

    contributors = [
        {
            "metric": metric,
            "reconstruction_error": round(sum(errors) / len(errors), 4),
        }
        for metric, errors in metric_contributors.items()
    ]
    contributors.sort(key=lambda item: item["reconstruction_error"], reverse=True)

    return {
        "ready": True,
        "status": "ok",
        "dataset_name": artifact.get("dataset_name"),
        "model_name": artifact.get("model_name"),
        "model_group": artifact.get("model_group"),
        "format": artifact.get("model_format"),
        "subset": selected_name,
        "metrics": list(selected_model.get("metrics") or []),
        "metric_count": len(selected_model.get("metrics") or []),
        "distance": round(reconstruction_error, 4),
        "threshold": round(threshold, 4),
        "score": round(score, 4),
        "training_quality": round(float(selected_training_quality), 6) if selected_training_quality is not None else None,
        "inference_priority": int(selected_priority),
        "contributors": contributors[:3],
        "train_windows": int(selected_model.get("train_windows") or 0),
    }
