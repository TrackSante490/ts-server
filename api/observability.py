import time
from typing import Final

from prometheus_client import Counter, Gauge, Histogram


def _label(value: str | None, default: str = "unknown") -> str:
    cleaned = (value or "").strip()
    return cleaned or default


HTTP_REQUESTS_TOTAL: Final = Counter(
    "tracksante_http_requests_total",
    "Total HTTP requests handled by the TrackSante API.",
    ["method", "route", "status"],
)

HTTP_REQUEST_DURATION_SECONDS: Final = Histogram(
    "tracksante_http_request_duration_seconds",
    "HTTP request duration for the TrackSante API.",
    ["method", "route"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)

DEPENDENCY_UP: Final = Gauge(
    "tracksante_dependency_up",
    "Dependency reachability from the API process.",
    ["service"],
)

DEPENDENCY_PROBE_DURATION_SECONDS: Final = Gauge(
    "tracksante_dependency_probe_duration_seconds",
    "Most recent dependency probe duration in seconds.",
    ["service"],
)

DEPENDENCY_LAST_PROBE_TIMESTAMP_SECONDS: Final = Gauge(
    "tracksante_dependency_last_probe_timestamp_seconds",
    "Unix timestamp of the most recent dependency probe.",
    ["service"],
)

SENSOR_EVENTS_TOTAL: Final = Counter(
    "tracksante_sensor_events_total",
    "Sensor events received by the API.",
    ["kind", "result"],
)

CHAT_REQUESTS_TOTAL: Final = Counter(
    "tracksante_chat_requests_total",
    "Chat API requests handled by the TrackSante backend.",
    ["result", "next_step", "recommended_sensor"],
)

CHAT_REQUEST_DURATION_SECONDS: Final = Histogram(
    "tracksante_chat_request_duration_seconds",
    "Duration of end-to-end /api/chat requests.",
    ["result"],
    buckets=(0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 40, 60, 90, 120),
)

RAG_CHAT_TURNS_TOTAL: Final = Counter(
    "tracksante_rag_chat_turns_total",
    "RAG chat turns processed through the API chat path.",
    ["result", "next_step", "recommended_sensor"],
)

RAG_CHAT_TURN_DURATION_SECONDS: Final = Histogram(
    "tracksante_rag_chat_turn_duration_seconds",
    "Duration of chat turns routed through rag_agent.chat_once.",
    ["result"],
    buckets=(0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 40, 60, 90, 120),
)

RAG_ERRORS_TOTAL: Final = Counter(
    "tracksante_rag_errors_total",
    "Errors raised while processing RAG chat turns.",
    ["stage"],
)


def observe_http_request(method: str, route: str, status: int, duration_seconds: float) -> None:
    method_label = _label(method, "UNKNOWN")
    route_label = _label(route, "/unknown")
    status_label = str(status)
    HTTP_REQUESTS_TOTAL.labels(method=method_label, route=route_label, status=status_label).inc()
    HTTP_REQUEST_DURATION_SECONDS.labels(method=method_label, route=route_label).observe(duration_seconds)


def observe_dependency_probe(service: str, up: bool, duration_seconds: float) -> None:
    service_label = _label(service)
    DEPENDENCY_UP.labels(service=service_label).set(1 if up else 0)
    DEPENDENCY_PROBE_DURATION_SECONDS.labels(service=service_label).set(duration_seconds)
    DEPENDENCY_LAST_PROBE_TIMESTAMP_SECONDS.labels(service=service_label).set(time.time())


def observe_sensor_event(kind: str | None, result: str) -> None:
    SENSOR_EVENTS_TOTAL.labels(kind=_label(kind), result=_label(result)).inc()


def observe_chat_request(
    *,
    result: str,
    duration_seconds: float,
    next_step: str | None = None,
    recommended_sensor: str | None = None,
) -> None:
    result_label = _label(result)
    CHAT_REQUESTS_TOTAL.labels(
        result=result_label,
        next_step=_label(next_step),
        recommended_sensor=_label(recommended_sensor),
    ).inc()
    CHAT_REQUEST_DURATION_SECONDS.labels(result=result_label).observe(duration_seconds)


def observe_rag_chat_turn(
    *,
    result: str,
    duration_seconds: float,
    next_step: str | None = None,
    recommended_sensor: str | None = None,
) -> None:
    result_label = _label(result)
    RAG_CHAT_TURNS_TOTAL.labels(
        result=result_label,
        next_step=_label(next_step),
        recommended_sensor=_label(recommended_sensor),
    ).inc()
    RAG_CHAT_TURN_DURATION_SECONDS.labels(result=result_label).observe(duration_seconds)


def observe_rag_error(stage: str) -> None:
    RAG_ERRORS_TOTAL.labels(stage=_label(stage)).inc()
