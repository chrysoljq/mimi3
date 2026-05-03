import asyncio
import json
import time
from collections import deque
from typing import Any, Dict, List
from fastapi import WebSocket

METRICS_SNAPSHOT_PATH = None  # 延迟初始化，在 metrics_store 中设置

class GatewayState:
    def __init__(self):
        self.active_clients: List[WebSocket] = []
        self.pending_queues: Dict[str, asyncio.Queue] = {}
        self.ws_to_req_ids: Dict[int, set] = {}  # id(ws) -> {req_id, ...}
        self.current_client_index: int = 0
        self.rebuild_event: asyncio.Event = asyncio.Event()
        self.client_cooldowns: Dict[int, float] = {}
        self.metrics_started_at: float = time.time()
        self.metrics_history_last_snapshot: Dict[str, Any] | None = None
        self.metrics: Dict[str, Any] = self._default_metrics()
        self.recent_errors: deque = deque(maxlen=200)

    @staticmethod
    def _default_metrics() -> Dict[str, Any]:
        return {
            "requests_total": 0,
            "requests_succeeded": 0,
            "requests_failed": 0,
            "streaming_requests": 0,
            "non_streaming_requests": 0,
            "attempts_total": 0,
            "attempts_succeeded": 0,
            "attempts_failed": 0,
            "request_latency_sum_ms": 0.0,
            "request_first_byte_latency_sum_ms": 0.0,
            "request_latency_samples_ms": deque(maxlen=2048),
            "request_first_byte_samples_ms": deque(maxlen=2048),
            "status_codes": {},
            "routes": {},
            "nodes": {},
            "tokens": {
                "requests_with_usage": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        }

state = GatewayState()
