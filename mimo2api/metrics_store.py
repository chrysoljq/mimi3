import asyncio
import json
import os
import sqlite3
import time
from typing import Any

from fastapi import WebSocket

from .gateway_state import state

METRICS_BUCKET_SECONDS = max(60, int(os.getenv("MIMO_METRICS_BUCKET_SECONDS", "300")))
METRICS_RETENTION_DAYS = max(1, int(os.getenv("MIMO_METRICS_RETENTION_DAYS", "90")))
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
METRICS_DB_PATH = os.getenv("MIMO_METRICS_DB_PATH", os.path.join(ROOT_DIR, "gateway_metrics.db"))


def node_label(ws: WebSocket) -> str:
    return ws.client.host if ws.client else "Unknown"


def _bump_counter(bucket: dict[str, Any], key: str, amount: int = 1) -> None:
    bucket[key] = bucket.get(key, 0) + amount


def _ensure_route_metrics(route_key: str) -> dict[str, Any]:
    routes = state.metrics["routes"]
    if route_key not in routes:
        routes[route_key] = {
            "requests_total": 0,
            "requests_succeeded": 0,
            "requests_failed": 0,
            "streaming_requests": 0,
            "non_streaming_requests": 0,
            "request_latency_sum_ms": 0.0,
            "request_first_byte_latency_sum_ms": 0.0,
            "status_codes": {},
            "tokens": {
                "requests_with_usage": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        }
    return routes[route_key]


def _ensure_node_metrics(node_key: str) -> dict[str, Any]:
    nodes = state.metrics["nodes"]
    if node_key not in nodes:
        nodes[node_key] = {
            "attempts_total": 0,
            "attempts_succeeded": 0,
            "attempts_failed": 0,
            "latency_sum_ms": 0.0,
            "first_byte_latency_sum_ms": 0.0,
            "status_codes": {},
        }
    return nodes[node_key]


def record_request_started(route_key: str, is_streaming: bool) -> None:
    metrics = state.metrics
    route_metrics = _ensure_route_metrics(route_key)

    metrics["requests_total"] += 1
    route_metrics["requests_total"] += 1

    if is_streaming:
        metrics["streaming_requests"] += 1
        route_metrics["streaming_requests"] += 1
    else:
        metrics["non_streaming_requests"] += 1
        route_metrics["non_streaming_requests"] += 1


def record_attempt_started(target_ws: WebSocket) -> None:
    metrics = state.metrics
    metrics["attempts_total"] += 1
    _ensure_node_metrics(node_label(target_ws))["attempts_total"] += 1


def record_attempt_finished(
    *,
    target_ws: WebSocket,
    status_code: int,
    first_byte_latency_ms: float,
    success: bool,
) -> None:
    metrics = state.metrics
    node_metrics = _ensure_node_metrics(node_label(target_ws))

    if success:
        metrics["attempts_succeeded"] += 1
        node_metrics["attempts_succeeded"] += 1
    else:
        metrics["attempts_failed"] += 1
        node_metrics["attempts_failed"] += 1

    node_metrics["first_byte_latency_sum_ms"] += first_byte_latency_ms
    _bump_counter(node_metrics["status_codes"], str(status_code))


def record_usage(route_key: str, usage: dict[str, Any] | None) -> None:
    if not isinstance(usage, dict):
        return

    prompt_tokens = int(
        usage.get("prompt_tokens", usage.get("input_tokens", 0)) or 0
    )
    completion_tokens = int(
        usage.get("completion_tokens", usage.get("output_tokens", 0)) or 0
    )
    total_tokens = int(
        usage.get("total_tokens", prompt_tokens + completion_tokens) or 0
    )

    metrics = state.metrics["tokens"]
    route_tokens = _ensure_route_metrics(route_key)["tokens"]

    metrics["requests_with_usage"] += 1
    metrics["prompt_tokens"] += prompt_tokens
    metrics["completion_tokens"] += completion_tokens
    metrics["total_tokens"] += total_tokens

    route_tokens["requests_with_usage"] += 1
    route_tokens["prompt_tokens"] += prompt_tokens
    route_tokens["completion_tokens"] += completion_tokens
    route_tokens["total_tokens"] += total_tokens


def percentile_from_samples(samples: list[float], ratio: float) -> float:
    if not samples:
        return 0.0
    if len(samples) == 1:
        return round(samples[0], 2)
    index = max(0, min(len(samples) - 1, int((len(samples) - 1) * ratio)))
    return round(samples[index], 2)


def summarize_status_codes(status_codes: dict[str, int]) -> dict[str, int]:
    return {key: status_codes[key] for key in sorted(status_codes)}


def build_latency_summary(sample_deque: Any, total_sum_ms: float, total_count: int) -> dict[str, float]:
    samples = sorted(float(v) for v in sample_deque)
    return {
        "avg_ms": round(total_sum_ms / total_count, 2) if total_count else 0.0,
        "p50_ms": percentile_from_samples(samples, 0.50),
        "p95_ms": percentile_from_samples(samples, 0.95),
        "p99_ms": percentile_from_samples(samples, 0.99),
        "sample_count": len(samples),
    }


def build_success_summary(total: int, succeeded: int, failed: int) -> dict[str, float | int]:
    return {
        "total": total,
        "succeeded": succeeded,
        "failed": failed,
        "success_rate": round((succeeded / total) * 100, 2) if total else 0.0,
    }


def extract_usage_from_sse_chunk(chunk_body: str) -> dict[str, Any] | None:
    if "\"usage\"" not in chunk_body:
        return None

    for raw_line in chunk_body.splitlines():
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if not payload or payload == "[DONE]" or "\"usage\"" not in payload:
            continue
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        usage = data.get("usage")
        if isinstance(usage, dict):
            return usage
    return None


def record_request_finished(
    *,
    route_key: str,
    status_code: int,
    started_at: float,
    first_byte_at: float | None,
    success: bool,
    usage: dict[str, Any] | None = None,
) -> None:
    metrics = state.metrics
    route_metrics = _ensure_route_metrics(route_key)
    total_latency_ms = (time.monotonic() - started_at) * 1000
    first_byte_latency_ms = ((first_byte_at or started_at) - started_at) * 1000

    if success:
        metrics["requests_succeeded"] += 1
        route_metrics["requests_succeeded"] += 1
    else:
        metrics["requests_failed"] += 1
        route_metrics["requests_failed"] += 1

    metrics["request_latency_sum_ms"] += total_latency_ms
    metrics["request_first_byte_latency_sum_ms"] += first_byte_latency_ms
    metrics["request_latency_samples_ms"].append(total_latency_ms)
    metrics["request_first_byte_samples_ms"].append(first_byte_latency_ms)

    route_metrics["request_latency_sum_ms"] += total_latency_ms
    route_metrics["request_first_byte_latency_sum_ms"] += first_byte_latency_ms

    _bump_counter(metrics["status_codes"], str(status_code))
    _bump_counter(route_metrics["status_codes"], str(status_code))

    if usage:
        record_usage(route_key, usage)


def capture_metrics_snapshot() -> dict[str, Any]:
    routes: dict[str, Any] = {}
    for route_key, route_metrics in state.metrics["routes"].items():
        routes[route_key] = {
            "requests_total": int(route_metrics["requests_total"]),
            "requests_succeeded": int(route_metrics["requests_succeeded"]),
            "requests_failed": int(route_metrics["requests_failed"]),
            "request_latency_sum_ms": float(route_metrics["request_latency_sum_ms"]),
        }

    return {
        "captured_at": int(time.time()),
        "gateway": {
            "requests_total": int(state.metrics["requests_total"]),
            "requests_succeeded": int(state.metrics["requests_succeeded"]),
            "requests_failed": int(state.metrics["requests_failed"]),
            "request_latency_sum_ms": float(state.metrics["request_latency_sum_ms"]),
        },
        "routes": routes,
    }


def subtract_snapshot_component(current: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, Any]:
    previous = previous or {}
    requests_total = max(0, int(current.get("requests_total", 0)) - int(previous.get("requests_total", 0)))
    requests_succeeded = max(0, int(current.get("requests_succeeded", 0)) - int(previous.get("requests_succeeded", 0)))
    requests_failed = max(0, int(current.get("requests_failed", 0)) - int(previous.get("requests_failed", 0)))
    latency_sum_ms = max(0.0, float(current.get("request_latency_sum_ms", 0.0)) - float(previous.get("request_latency_sum_ms", 0.0)))
    return {
        "requests_total": requests_total,
        "requests_succeeded": requests_succeeded,
        "requests_failed": requests_failed,
        "avg_latency_ms": round(latency_sum_ms / requests_total, 2) if requests_total else 0.0,
        "success_rate": round((requests_succeeded / requests_total) * 100, 2) if requests_total else 0.0,
    }


def classify_component_status(total_requests: int, success_rate: float, avg_latency_ms: float) -> str:
    if total_requests <= 0:
        return "no_data"
    if success_rate >= 99 and avg_latency_ms <= 3000:
        return "operational"
    if success_rate >= 95 and avg_latency_ms <= 8000:
        return "degraded"
    return "major_outage"


def build_history_rows(bucket_start: int, current_snapshot: dict[str, Any], previous_snapshot: dict[str, Any] | None) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []

    gateway_delta = subtract_snapshot_component(
        current_snapshot["gateway"],
        previous_snapshot["gateway"] if previous_snapshot else None,
    )
    rows.append((
        "gateway",
        "gateway",
        bucket_start,
        gateway_delta["requests_total"],
        gateway_delta["requests_succeeded"],
        gateway_delta["requests_failed"],
        gateway_delta["success_rate"],
        gateway_delta["avg_latency_ms"],
        classify_component_status(
            gateway_delta["requests_total"],
            gateway_delta["success_rate"],
            gateway_delta["avg_latency_ms"],
        ),
    ))

    previous_routes = previous_snapshot["routes"] if previous_snapshot else {}
    route_keys = sorted(set(current_snapshot["routes"]) | set(previous_routes))
    for route_key in route_keys:
        route_delta = subtract_snapshot_component(
            current_snapshot["routes"].get(route_key, {}),
            previous_routes.get(route_key),
        )
        rows.append((
            "route",
            route_key,
            bucket_start,
            route_delta["requests_total"],
            route_delta["requests_succeeded"],
            route_delta["requests_failed"],
            route_delta["success_rate"],
            route_delta["avg_latency_ms"],
            classify_component_status(
                route_delta["requests_total"],
                route_delta["success_rate"],
                route_delta["avg_latency_ms"],
            ),
        ))

    return rows


def init_metrics_db() -> None:
    conn = sqlite3.connect(METRICS_DB_PATH, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS status_history (
                component_type TEXT NOT NULL,
                component_key TEXT NOT NULL,
                bucket_start INTEGER NOT NULL,
                requests_total INTEGER NOT NULL,
                requests_succeeded INTEGER NOT NULL,
                requests_failed INTEGER NOT NULL,
                success_rate REAL NOT NULL,
                avg_latency_ms REAL NOT NULL,
                status TEXT NOT NULL,
                PRIMARY KEY (component_type, component_key, bucket_start)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_status_history_bucket
            ON status_history (bucket_start)
            """
        )
        conn.commit()
    finally:
        conn.close()


def write_history_rows(rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    conn = sqlite3.connect(METRICS_DB_PATH, timeout=30)
    try:
        conn.executemany(
            """
            INSERT INTO status_history (
                component_type,
                component_key,
                bucket_start,
                requests_total,
                requests_succeeded,
                requests_failed,
                success_rate,
                avg_latency_ms,
                status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(component_type, component_key, bucket_start) DO UPDATE SET
                requests_total = excluded.requests_total,
                requests_succeeded = excluded.requests_succeeded,
                requests_failed = excluded.requests_failed,
                success_rate = excluded.success_rate,
                avg_latency_ms = excluded.avg_latency_ms,
                status = excluded.status
            """,
            rows,
        )
        retention_cutoff = int(time.time()) - METRICS_RETENTION_DAYS * 86400
        conn.execute("DELETE FROM status_history WHERE bucket_start < ?", (retention_cutoff,))
        conn.commit()
    finally:
        conn.close()


async def flush_history_bucket(bucket_start: int) -> None:
    current_snapshot = capture_metrics_snapshot()
    rows = build_history_rows(bucket_start, current_snapshot, state.metrics_history_last_snapshot)
    state.metrics_history_last_snapshot = current_snapshot
    await asyncio.to_thread(write_history_rows, rows)


async def metrics_history_worker() -> None:
    state.metrics_history_last_snapshot = capture_metrics_snapshot()
    try:
        while True:
            now = time.time()
            next_bucket_start = (int(now) // METRICS_BUCKET_SECONDS + 1) * METRICS_BUCKET_SECONDS
            await asyncio.sleep(max(1, next_bucket_start - now))
            await flush_history_bucket(next_bucket_start - METRICS_BUCKET_SECONDS)
    except asyncio.CancelledError:
        current_bucket_start = (int(time.time()) // METRICS_BUCKET_SECONDS) * METRICS_BUCKET_SECONDS
        await flush_history_bucket(current_bucket_start)
        raise


def load_status_history(hours: int) -> dict[str, Any]:
    now = int(time.time())
    bucket_span = max(1, hours * 3600)
    latest_complete_bucket = max(0, (now // METRICS_BUCKET_SECONDS) * METRICS_BUCKET_SECONDS - METRICS_BUCKET_SECONDS)
    start_bucket = max(0, latest_complete_bucket - bucket_span + METRICS_BUCKET_SECONDS)
    since_ts = start_bucket
    conn = sqlite3.connect(METRICS_DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                component_type,
                component_key,
                bucket_start,
                requests_total,
                requests_succeeded,
                requests_failed,
                success_rate,
                avg_latency_ms,
                status
            FROM status_history
            WHERE bucket_start >= ?
            ORDER BY component_type, component_key, bucket_start
            """,
            (since_ts,),
        ).fetchall()
    finally:
        conn.close()

    components: dict[tuple[str, str], dict[str, Any]] = {}
    default_component_keys = [("gateway", "gateway")] + [("route", route_key) for route_key in sorted(state.metrics["routes"])]
    for component_type, component_key in default_component_keys:
        components[(component_type, component_key)] = {
            "component_type": component_type,
            "component_key": component_key,
            "display_name": "Gateway" if component_type == "gateway" else component_key,
            "points": [],
            "totals": {
                "requests_total": 0,
                "requests_succeeded": 0,
                "requests_failed": 0,
            },
        }

    for row in rows:
        key = (row["component_type"], row["component_key"])
        component = components[key]
        component["points"].append({
            "bucket_start": row["bucket_start"],
            "bucket_end": row["bucket_start"] + METRICS_BUCKET_SECONDS,
            "status": row["status"],
            "requests_total": row["requests_total"],
            "requests_succeeded": row["requests_succeeded"],
            "requests_failed": row["requests_failed"],
            "success_rate": round(float(row["success_rate"]), 2),
            "avg_latency_ms": round(float(row["avg_latency_ms"]), 2),
        })
        component["totals"]["requests_total"] += int(row["requests_total"])
        component["totals"]["requests_succeeded"] += int(row["requests_succeeded"])
        component["totals"]["requests_failed"] += int(row["requests_failed"])

    history_components = []
    bucket_starts = list(range(start_bucket, latest_complete_bucket + 1, METRICS_BUCKET_SECONDS)) if latest_complete_bucket >= start_bucket else []
    for component in components.values():
        totals = component.pop("totals")
        point_map = {point["bucket_start"]: point for point in component["points"]}
        component["points"] = [
            point_map.get(bucket_start, {
                "bucket_start": bucket_start,
                "bucket_end": bucket_start + METRICS_BUCKET_SECONDS,
                "status": "no_data",
                "requests_total": 0,
                "requests_succeeded": 0,
                "requests_failed": 0,
                "success_rate": 0.0,
                "avg_latency_ms": 0.0,
            })
            for bucket_start in bucket_starts
        ]
        total = totals["requests_total"]
        component["uptime_percentage"] = round((totals["requests_succeeded"] / total) * 100, 2) if total else 0.0
        component["summary"] = totals
        history_components.append(component)

    history_components.sort(key=lambda item: (item["component_type"], item["component_key"]))
    return {
        "bucket_seconds": METRICS_BUCKET_SECONDS,
        "hours": hours,
        "generated_at": now,
        "bucket_starts": bucket_starts,
        "components": history_components,
    }


def build_gateway_stats(background_tasks_count: int) -> dict[str, Any]:
    now = time.time()
    nodes: list[dict[str, Any]] = []
    available_clients = 0
    metrics = state.metrics

    for index, client in enumerate(state.active_clients):
        cooldown_until = state.client_cooldowns.get(id(client), 0)
        is_available = cooldown_until <= now
        if is_available:
            available_clients += 1

        tracked_req_ids = state.ws_to_req_ids.get(id(client), set())
        node_key = node_label(client)
        node_metrics = metrics["nodes"].get(node_key, {})
        node_attempt_total = int(node_metrics.get("attempts_total", 0))
        nodes.append({
            "index": index,
            "node": node_key,
            "client_id": id(client),
            "available": is_available,
            "cooldown_until": int(cooldown_until) if cooldown_until > now else 0,
            "cooldown_remaining_seconds": max(0, int(cooldown_until - now)),
            "pending_requests": len(tracked_req_ids),
            "attempts": build_success_summary(
                node_attempt_total,
                int(node_metrics.get("attempts_succeeded", 0)),
                int(node_metrics.get("attempts_failed", 0)),
            ),
            "avg_first_byte_latency_ms": round(
                float(node_metrics.get("first_byte_latency_sum_ms", 0.0)) / node_attempt_total,
                2,
            ) if node_attempt_total else 0.0,
            "status_codes": summarize_status_codes(node_metrics.get("status_codes", {})),
        })

    routes: dict[str, Any] = {}
    for route_key, route_metrics in metrics["routes"].items():
        route_total = int(route_metrics["requests_total"])
        route_tokens = route_metrics["tokens"]
        routes[route_key] = {
            "requests": build_success_summary(
                route_total,
                int(route_metrics["requests_succeeded"]),
                int(route_metrics["requests_failed"]),
            ),
            "streaming_requests": int(route_metrics["streaming_requests"]),
            "non_streaming_requests": int(route_metrics["non_streaming_requests"]),
            "avg_latency_ms": round(float(route_metrics["request_latency_sum_ms"]) / route_total, 2) if route_total else 0.0,
            "avg_first_byte_latency_ms": round(
                float(route_metrics["request_first_byte_latency_sum_ms"]) / route_total,
                2,
            ) if route_total else 0.0,
            "status_codes": summarize_status_codes(route_metrics["status_codes"]),
            "tokens": {
                "requests_with_usage": int(route_tokens["requests_with_usage"]),
                "prompt_tokens": int(route_tokens["prompt_tokens"]),
                "completion_tokens": int(route_tokens["completion_tokens"]),
                "total_tokens": int(route_tokens["total_tokens"]),
            },
        }

    request_total = int(metrics["requests_total"])
    attempt_total = int(metrics["attempts_total"])
    token_metrics = metrics["tokens"]
    return {
        "uptime_seconds": int(now - state.metrics_started_at),
        "active_clients": len(state.active_clients),
        "available_clients": available_clients,
        "cooldown_clients": len(state.active_clients) - available_clients,
        "pending_requests": len(state.pending_queues),
        "tracked_ws_request_sets": len(state.ws_to_req_ids),
        "background_tasks": background_tasks_count,
        "current_client_index": state.current_client_index,
        "requests": build_success_summary(
            request_total,
            int(metrics["requests_succeeded"]),
            int(metrics["requests_failed"]),
        ),
        "attempts": build_success_summary(
            attempt_total,
            int(metrics["attempts_succeeded"]),
            int(metrics["attempts_failed"]),
        ),
        "latency": build_latency_summary(
            metrics["request_latency_samples_ms"],
            float(metrics["request_latency_sum_ms"]),
            request_total,
        ),
        "first_byte_latency": build_latency_summary(
            metrics["request_first_byte_samples_ms"],
            float(metrics["request_first_byte_latency_sum_ms"]),
            request_total,
        ),
        "status_codes": summarize_status_codes(metrics["status_codes"]),
        "streaming_requests": int(metrics["streaming_requests"]),
        "non_streaming_requests": int(metrics["non_streaming_requests"]),
        "tokens": {
            "requests_with_usage": int(token_metrics["requests_with_usage"]),
            "prompt_tokens": int(token_metrics["prompt_tokens"]),
            "completion_tokens": int(token_metrics["completion_tokens"]),
            "total_tokens": int(token_metrics["total_tokens"]),
        },
        "routes": routes,
        "nodes": nodes,
    }
