import asyncio
from typing import Dict, List
from fastapi import WebSocket

class GatewayState:
    def __init__(self):
        self.active_clients: List[WebSocket] = []
        self.pending_queues: Dict[str, asyncio.Queue] = {}
        self.ws_to_req_ids: Dict[int, set] = {}  # id(ws) -> {req_id, ...}
        self.current_client_index: int = 0
        self.rebuild_event: asyncio.Event = asyncio.Event()
        self.client_cooldowns: Dict[int, float] = {}

state = GatewayState()
