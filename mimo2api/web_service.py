import asyncio
import base64
import binascii
import fcntl
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn
import os
from pathlib import Path

MODEL_MAPPING_FILE = Path(__file__).parent.parent / "model_mapping.json"

# 引入 Manager 长驻协程任务
from .manager import start_manager_tasks, trigger_rebuild

# Responses API 转换器
from .responses_converter import convert_request as responses_convert_request
from .responses_converter import convert_response as responses_convert_response
from .responses_converter import ResponsesStreamConverter
from .audio_helpers import (
    AudioSpeechRequest,
    audio_media_type,
    extract_audio_payload,
    map_openai_tts_model,
    map_openai_tts_voice,
)
from .metrics_store import (
    METRICS_BUCKET_SECONDS,
    METRICS_RETENTION_DAYS,
    build_gateway_stats,
    extract_usage_from_sse_chunk,
    init_metrics_db,
    load_status_history,
    metrics_history_worker,
    node_label,
    record_attempt_finished,
    record_attempt_started,
    record_request_finished,
    record_request_started,
)

# 配置基础日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

manager_bg_task = None
metrics_persist_task = None
single_process_lock_file = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global manager_bg_task, metrics_persist_task
    logger.info("🚀 正在拉起挂后台的 Claw 账号守护线程...")
    acquire_single_process_lock()
    await asyncio.to_thread(init_metrics_db)
    manager_bg_task = asyncio.create_task(start_manager_tasks())
    metrics_persist_task = asyncio.create_task(metrics_history_worker())
    yield
    if manager_bg_task:
        manager_bg_task.cancel()
    if metrics_persist_task:
        metrics_persist_task.cancel()
        try:
            await metrics_persist_task
        except asyncio.CancelledError:
            pass
    release_single_process_lock()

app = FastAPI(lifespan=lifespan)

# 全局状态从 gateway_state 引入
from .gateway_state import state

# 注入前面拆分出的 WebUI 独立路由
from .ui_router import router as ui_router
app.include_router(ui_router)

RETRYABLE_STATUS_CODES = {401, 403, 429}
NODE_RESPONSE_TIMEOUT = 30
STREAM_CHUNK_TIMEOUT = 60
STREAM_KEEPALIVE_INTERVAL = 25  # 秒，需小于 Cloudflare 超时 (~100s)
QUEUE_DRAIN_TIMEOUT = 5
DEFAULT_GATEWAY_ERROR = "Gateway Error: 所有节点请求失败"
NODE_401_COOLDOWN_SECONDS = int(os.getenv("MIMO_NODE_401_COOLDOWN_SECONDS", "900"))
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESS_LOCK_PATH = os.getenv("MIMO_PROCESS_LOCK_PATH", os.path.join(ROOT_DIR, "mimo2api.lock"))

# 后台 fire-and-forget 任务集合，防止 GC 回收和 "exception was never retrieved" 警告
_background_tasks: set[asyncio.Task] = set()


def _track_task(task: asyncio.Task) -> None:
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


def acquire_single_process_lock() -> None:
    global single_process_lock_file
    if single_process_lock_file is not None:
        return

    lock_file = open(PROCESS_LOCK_PATH, "w", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        lock_file.close()
        raise RuntimeError(
            "mimo2api 当前仅支持单进程运行，检测到另一个网关进程已持有锁。"
            "请使用单 worker 部署，或引入外部队列后再做多进程扩展。"
        ) from exc

    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(os.getpid()))
    lock_file.flush()
    single_process_lock_file = lock_file


def release_single_process_lock() -> None:
    global single_process_lock_file
    if single_process_lock_file is None:
        return
    try:
        fcntl.flock(single_process_lock_file.fileno(), fcntl.LOCK_UN)
    finally:
        single_process_lock_file.close()
        single_process_lock_file = None


@dataclass(slots=True)
class RetryState:
    status_code: int = 502
    response_text: str = DEFAULT_GATEWAY_ERROR


@dataclass(slots=True)
class ForwardAttempt:
    req_id: str
    queue: asyncio.Queue
    target_ws: WebSocket
    first_msg: dict[str, Any]
    attempt_number: int

@app.post("/api/rebuild")
async def api_rebuild():
    """手动触发所有 Claw 节点强制销毁重建（用于更新 bridge.py 等场景）"""
    trigger_rebuild()
    logger.info("🔔 手动重建信号已发送")
    return JSONResponse(content={"ok": True, "message": "重建信号已发送，所有节点将在当前循环结束后立即重建"})


@app.get("/api/stats")
async def api_stats():
    return JSONResponse(content=build_gateway_stats(len(_background_tasks)))


@app.get("/api/status/history")
async def api_status_history(hours: int = 24):
    hours = max(1, min(hours, 24 * METRICS_RETENTION_DAYS))
    return JSONResponse(content=await asyncio.to_thread(load_status_history, hours))


# ── 模型映射 ──────────────────────────────────────────────────────

def load_model_mapping() -> dict[str, str]:
    """读取模型映射文件，返回 {源模型: 目标模型} 字典"""
    if not MODEL_MAPPING_FILE.exists():
        return {}
    try:
        return json.loads(MODEL_MAPPING_FILE.read_text("utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_model_mapping(mapping: dict[str, str]) -> None:
    """原子写入模型映射文件"""
    tmp = MODEL_MAPPING_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), "utf-8")
    tmp.rename(MODEL_MAPPING_FILE)


def apply_model_mapping(body_text: str) -> str:
    """如果请求体中有模型需要映射，替换后返回新 body；否则原样返回"""
    mapping = load_model_mapping()
    if not mapping:
        return body_text
    try:
        data = json.loads(body_text)
    except (json.JSONDecodeError, AttributeError):
        return body_text
    original_model = data.get("model")
    if original_model and original_model in mapping:
        data["model"] = mapping[original_model]
        logger.info(f"🔀 模型映射: {original_model} → {data['model']}")
        return json.dumps(data, ensure_ascii=False)
    return body_text


@app.get("/api/model_mapping")
async def api_get_model_mapping():
    return JSONResponse(content=load_model_mapping())


@app.put("/api/model_mapping")
async def api_put_model_mapping(request: Request):
    body = await request.body()
    try:
        new_mapping = json.loads(body.decode("utf-8", "ignore").lstrip("\ufeff"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse({"error": "请求体不是合法 JSON"}, status_code=400)
    if not isinstance(new_mapping, dict):
        return JSONResponse({"error": "映射必须是 JSON 对象"}, status_code=400)
    save_model_mapping(new_mapping)
    logger.info(f"📝 模型映射已更新: {new_mapping}")
    return JSONResponse(content=new_mapping)


@app.delete("/api/model_mapping/{model_name:path}")
async def api_delete_model_mapping(model_name: str):
    mapping = load_model_mapping()
    if model_name in mapping:
        del mapping[model_name]
        save_model_mapping(mapping)
        return JSONResponse({"ok": True, "deleted": model_name})
    return JSONResponse({"error": f"模型 {model_name} 不在映射中"}, status_code=404)

@app.websocket("/ws")
async def ws_tunnel(ws: WebSocket):
    await ws.accept()
    client_addr = f"{ws.client.host}:{ws.client.port}" if ws.client else "Unknown"
    state.active_clients.append(ws)
    state.client_cooldowns.pop(id(ws), None)
    logger.info(f"✅ 内网节点已接入: {client_addr}。当前在线节点数: {len(state.active_clients)}")
    
    try:
        while True:
            # 接收内网传回的结果
            msg = await ws.receive_text()
            data = json.loads(msg)

            # 唤醒对应 ID 的 HTTP 请求队列
            req_id = data.get("req_id")
            if req_id and req_id in state.pending_queues:
                state.pending_queues[req_id].put_nowait(data)
            # 被抛弃或已结束的请求由于已被剔除，直接丢弃不再记录警告，避免刷屏

    except WebSocketDisconnect:
        logger.warning(f"❌ 内网节点主动断开: {client_addr}")
    except Exception as e:
        logger.error(f"❌ 内网节点异常断开: {client_addr}, 错误: {e}")
    finally:
        # 清理断开的连接
        if ws in state.active_clients:
            state.active_clients.remove(ws)
        state.client_cooldowns.pop(id(ws), None)
        # 清理该节点的所有孤儿队列，防止内存泄漏和请求卡死
        orphan_ids = state.ws_to_req_ids.pop(id(ws), set())
        for orphan_id in orphan_ids:
            q = state.pending_queues.pop(orphan_id, None)
            if q is not None:
                try:
                    q.put_nowait({"type": "error", "body": "节点断开连接"})
                except asyncio.QueueFull:
                    pass
        if orphan_ids:
            logger.warning(f"🧹 节点断开，已清理 {len(orphan_ids)} 个孤儿请求队列")
        if state.current_client_index >= len(state.active_clients):
            state.current_client_index = 0
        logger.info(f"当前在线节点数: {len(state.active_clients)}")


# 轮询获取下一个可用的客户端
def get_next_client() -> WebSocket | None:
    if not state.active_clients:
        return None

    now = time.time()
    available_clients: list[WebSocket] = []
    for client in state.active_clients:
        cooldown_until = state.client_cooldowns.get(id(client), 0)
        if cooldown_until <= now:
            available_clients.append(client)

    if not available_clients:
        return None

    if state.current_client_index >= len(available_clients):
        state.current_client_index = 0
    client = available_clients[state.current_client_index]
    state.current_client_index = (state.current_client_index + 1) % len(available_clients)
    return client


def create_pending_request() -> tuple[str, asyncio.Queue]:
    req_id = str(uuid.uuid4())
    queue: asyncio.Queue = asyncio.Queue()
    state.pending_queues[req_id] = queue
    return req_id, queue


def cleanup_pending_request(req_id: str, ws: WebSocket | None = None) -> None:
    state.pending_queues.pop(req_id, None)
    if ws is not None:
        req_ids = state.ws_to_req_ids.get(id(ws))
        if req_ids is not None:
            req_ids.discard(req_id)
            if not req_ids:
                state.ws_to_req_ids.pop(id(ws), None)


def cooldown_client(ws: WebSocket, seconds: int, reason: str) -> None:
    cooldown_until = time.time() + max(seconds, 0)
    state.client_cooldowns[id(ws)] = cooldown_until
    logger.warning(
        f"⛔ 节点 {node_label(ws)} 因 {reason} 进入冷却 {seconds}s，"
        f"冷却结束时间戳: {int(cooldown_until)}"
    )


async def drain_and_close(req_id: str, queue: asyncio.Queue) -> None:
    try:
        while True:
            msg = await asyncio.wait_for(queue.get(), timeout=QUEUE_DRAIN_TIMEOUT)
            if msg.get("type") in ["finish", "error"]:
                break
    except asyncio.TimeoutError:
        logger.debug(f"排空节点队列超时，直接清理 [{req_id[:8]}]")
    except Exception as exc:
        logger.debug(f"排空节点队列失败 [{req_id[:8]}]: {exc}")
    finally:
        cleanup_pending_request(req_id)


def should_retry_status(status_code: int) -> bool:
    return status_code in RETRYABLE_STATUS_CODES or status_code >= 500


def build_ws_payload(req_id: str, method: str, path: str, body: str) -> str:
    return json.dumps({
        "req_id": req_id,
        "method": method,
        "path": path,
        "body": body,
    })


async def dispatch_to_node(*, method: str, path: str, body: str, log_label: str, attempt_number: int) -> ForwardAttempt | None:
    req_id, queue = create_pending_request()
    target_ws = get_next_client()
    if not target_ws:
        cleanup_pending_request(req_id)
        return None

    # 追踪 req_id 归属到哪个 WS 节点，用于断连时清理孤儿队列
    state.ws_to_req_ids.setdefault(id(target_ws), set()).add(req_id)

    ws_payload = build_ws_payload(req_id, method, path, body)
    attempt_started_at = time.monotonic()
    record_attempt_started(target_ws)

    try:
        await target_ws.send_text(ws_payload)
        logger.debug(
            f"👉 {log_label} [{req_id[:8]}] ({method} {path}) -> 节点: {node_label(target_ws)} "
            f"(尝试 {attempt_number})"
        )
    except RuntimeError:
        record_attempt_finished(
            target_ws=target_ws,
            status_code=0,
            first_byte_latency_ms=(time.monotonic() - attempt_started_at) * 1000,
            success=False,
        )
        logger.warning(f"⚠️ {log_label} 转发失败，节点状态异常，尝试切换...")
        cleanup_pending_request(req_id, target_ws)
        if target_ws in state.active_clients:
            state.active_clients.remove(target_ws)
        state.client_cooldowns.pop(id(target_ws), None)
        state.ws_to_req_ids.pop(id(target_ws), None)
        return None

    try:
        first_msg = await asyncio.wait_for(queue.get(), timeout=NODE_RESPONSE_TIMEOUT)
    except asyncio.TimeoutError:
        record_attempt_finished(
            target_ws=target_ws,
            status_code=504,
            first_byte_latency_ms=(time.monotonic() - attempt_started_at) * 1000,
            success=False,
        )
        raise

    record_attempt_finished(
        target_ws=target_ws,
        status_code=int(first_msg.get("status", 200)),
        first_byte_latency_ms=(time.monotonic() - attempt_started_at) * 1000,
        success=first_msg.get("type") != "error" and not should_retry_status(int(first_msg.get("status", 200))),
    )
    return ForwardAttempt(
        req_id=req_id,
        queue=queue,
        target_ws=target_ws,
        first_msg=first_msg,
        attempt_number=attempt_number,
    )


async def prepare_forward_attempt(
    *,
    method: str,
    path: str,
    body: str,
    log_label: str,
    retry_state: RetryState,
    attempt_number: int,
) -> ForwardAttempt | None:
    attempt = await dispatch_to_node(
        method=method,
        path=path,
        body=body,
        log_label=log_label,
        attempt_number=attempt_number,
    )
    if attempt is None:
        return None

    first_msg = attempt.first_msg
    if first_msg.get("type") == "error":
        error_text = first_msg.get("body") or "节点返回错误"
        logger.warning(f"⚠️ {log_label} 节点返回内部错误: {error_text}，尝试切换...")
        retry_state.response_text = f"Gateway Error: {error_text}"
        cleanup_pending_request(attempt.req_id)
        return None

    status_code = first_msg.get("status", 200)
    if status_code == 401:
        cooldown_client(attempt.target_ws, NODE_401_COOLDOWN_SECONDS, "401 Unauthorized")
        retry_state.status_code = 401
        retry_state.response_text = "Gateway Error: 节点鉴权失败 (401)，已临时跳过该节点"

    if should_retry_status(status_code):
        logger.warning(
            f"⚠️ {log_label} 节点返回状态码 {status_code}，触发自动重试 "
            f"(当前 attempt={attempt_number})..."
        )
        retry_state.status_code = status_code
        _track_task(asyncio.create_task(drain_and_close(attempt.req_id, attempt.queue)))
        return None

    return attempt


def normalize_response_headers(headers: dict | None) -> tuple[str, dict]:
    response_headers = dict(headers or {})
    content_type = response_headers.pop("content-type", "application/json")
    response_headers.pop("content-length", None)
    response_headers.pop("transfer-encoding", None)
    response_headers.pop("content-encoding", None)  # bridge已自动解压，移除避免客户端重复解压
    response_headers.pop("connection", None)
    return content_type, response_headers


async def collect_response_body(current_req_id: str, current_queue: asyncio.Queue, timeout: int = 120) -> str:
    chunks: list[str] = []
    try:
        while True:
            msg = await asyncio.wait_for(current_queue.get(), timeout=timeout)
            if msg.get("type") == "finish":
                break
            if msg.get("type") == "error":
                raise RuntimeError(msg.get("body") or "节点返回错误")
            if msg.get("type") == "chunk":
                chunks.append(msg.get("body", ""))
    finally:
        state.pending_queues.pop(current_req_id, None)
    return "".join(chunks)


@app.post("/v1/audio/speech")
async def audio_speech_handler(payload: AudioSpeechRequest):
    if not state.active_clients:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    input_text = payload.input.strip()
    if not input_text:
        return JSONResponse({"error": {"message": "`input` 不能为空"}}, status_code=400)

    instructions = payload.instructions
    response_format = payload.response_format.lower()
    messages = []
    if isinstance(instructions, str) and instructions.strip():
        messages.append({"role": "user", "content": instructions})
    messages.append({"role": "assistant", "content": input_text})

    mimo_payload = {
        "model": map_openai_tts_model(payload.model),
        "messages": messages,
        "audio": {
            "format": response_format,
            "voice": map_openai_tts_voice(payload.voice),
        },
    }

    body_text = json.dumps(mimo_payload, ensure_ascii=False)
    max_retries = len(state.active_clients)
    retry_state = RetryState()
    route_key = "/v1/audio/speech"
    request_started_at = time.monotonic()
    record_request_started(route_key, is_streaming=False)

    for attempt in range(max_retries):
        req_id = "unknown"
        try:
            prepared = await prepare_forward_attempt(
                method="POST",
                path="/v1/chat/completions",
                body=body_text,
                log_label="TTS 映射请求",
                retry_state=retry_state,
                attempt_number=attempt + 1,
            )
            if prepared is None:
                continue
            req_id = prepared.req_id
            queue = prepared.queue
            first_msg = prepared.first_msg
            first_byte_at = time.monotonic()

            raw_body = await collect_response_body(req_id, queue)
            status_code = first_msg.get("status", 200)
            if status_code >= 400:
                try:
                    _model = json.loads(body_text).get("model", "未指定")
                except Exception:
                    _model = "解析失败"
                logger.warning(f"⚠️ 上游返回 {status_code} [{req_id[:8]}] model={_model}, 响应: {raw_body[:300]}, 请求体: {body_text[:200]}")
                content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))
                record_request_finished(
                    route_key=route_key,
                    status_code=status_code,
                    started_at=request_started_at,
                    first_byte_at=first_byte_at,
                    success=False,
                )
                return Response(raw_body, status_code=status_code, media_type=content_type, headers=response_headers)

            try:
                response_json = json.loads(raw_body)
            except json.JSONDecodeError:
                logger.error(f"⚠️ TTS 上游响应不是合法 JSON [{req_id[:8]}]")
                record_request_finished(
                    route_key=route_key,
                    status_code=502,
                    started_at=request_started_at,
                    first_byte_at=first_byte_at,
                    success=False,
                )
                return JSONResponse({"error": {"message": "上游 TTS 返回了非法 JSON"}}, status_code=502)

            audio_b64, actual_format = extract_audio_payload(response_json)
            if not audio_b64:
                logger.error(f"⚠️ TTS 上游响应中未找到音频数据 [{req_id[:8]}]")
                record_request_finished(
                    route_key=route_key,
                    status_code=502,
                    started_at=request_started_at,
                    first_byte_at=first_byte_at,
                    success=False,
                )
                return JSONResponse({"error": {"message": "上游 TTS 响应里没有音频数据"}}, status_code=502)

            try:
                audio_bytes = base64.b64decode(audio_b64, validate=True)
            except binascii.Error:
                try:
                    audio_bytes = base64.b64decode(audio_b64)
                except binascii.Error:
                    logger.error(f"⚠️ TTS 音频 Base64 解码失败 [{req_id[:8]}]")
                    record_request_finished(
                        route_key=route_key,
                        status_code=502,
                        started_at=request_started_at,
                        first_byte_at=first_byte_at,
                        success=False,
                    )
                    return JSONResponse({"error": {"message": "上游 TTS 音频数据损坏"}}, status_code=502)

            final_format = (actual_format or response_format).lower()
            record_request_finished(
                route_key=route_key,
                status_code=200,
                started_at=request_started_at,
                first_byte_at=first_byte_at,
                success=True,
            )
            return Response(audio_bytes, media_type=audio_media_type(final_format))

        except asyncio.TimeoutError:
            logger.error(f"⚠️ TTS 请求等待内网节点超时 (30s) [{req_id[:8]}]，尝试切换...")
            retry_state.status_code = 504
            retry_state.response_text = "Gateway Error: 请求内网节点超时 (30s)"
            cleanup_pending_request(req_id)
            continue
        except RuntimeError as exc:
            logger.error(f"⚠️ TTS 响应收集失败 [{req_id[:8]}]: {exc}")
            retry_state.status_code = 502
            retry_state.response_text = f"Gateway Error: {exc}"
            continue

    record_request_finished(
        route_key=route_key,
        status_code=retry_state.status_code,
        started_at=request_started_at,
        first_byte_at=None,
        success=False,
    )
    return Response(retry_state.response_text, status_code=retry_state.status_code)

@app.post("/v1/responses")
async def responses_handler(request: Request):
    """将 OpenAI Responses API 请求转换为 Chat Completions 格式后转发。"""
    if not state.active_clients:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    body = await request.body()
    try:
        req_body = json.loads(body.decode("utf-8", "ignore").lstrip("\ufeff"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        logger.warning(f"⚠️ Responses 请求体不是合法 JSON: {body[:500]}")
        return JSONResponse({"error": {"message": "请求体不是合法 JSON"}}, status_code=400)

    # 转换请求格式
    try:
        chat_req = responses_convert_request(req_body)
    except Exception as exc:
        logger.error(f"⚠️ Responses 请求转换失败: {exc}, 请求体: {body[:500]}")
        return JSONResponse({"error": {"message": f"请求格式转换失败: {exc}"}}, status_code=400)

    model = chat_req.get("model", "")
    is_streaming = chat_req.get("stream", False) is True
    # responses API 默认流式
    if "stream" not in req_body:
        is_streaming = True
        chat_req["stream"] = True

    chat_body_text = json.dumps(chat_req, ensure_ascii=False)
    max_retries = len(state.active_clients)
    retry_state = RetryState()
    route_key = "/v1/responses"
    request_started_at = time.monotonic()
    record_request_started(route_key, is_streaming=is_streaming)

    for attempt in range(max_retries):
        req_id = "unknown"
        try:
            prepared = await prepare_forward_attempt(
                method="POST",
                path="/v1/chat/completions",
                body=chat_body_text,
                log_label="Responses 映射请求",
                retry_state=retry_state,
                attempt_number=attempt + 1,
            )
            if prepared is None:
                continue
            req_id = prepared.req_id
            queue = prepared.queue
            first_msg = prepared.first_msg
            status_code = first_msg.get("status", 200)
            first_byte_at = time.monotonic()

            if status_code >= 400:
                content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))
                raw_body = await collect_response_body(req_id, queue)
                record_request_finished(
                    route_key=route_key,
                    status_code=status_code,
                    started_at=request_started_at,
                    first_byte_at=first_byte_at,
                    success=False,
                )
                return Response(raw_body, status_code=status_code, media_type=content_type, headers=response_headers)

            if is_streaming:
                # 流式: 将 chat/completions SSE chunks 转换为 responses API events
                converter = ResponsesStreamConverter(model=model)

                async def responses_stream_generator(current_req_id, current_queue):
                    last_data_time = time.monotonic()
                    stream_succeeded = False

                    async def _do_keepalive():
                        await asyncio.sleep(STREAM_KEEPALIVE_INTERVAL)
                        return b": keep-alive\n\n"

                    data_task = asyncio.ensure_future(current_queue.get())
                    keepalive_task = asyncio.ensure_future(_do_keepalive())

                    try:
                        while True:
                            done, _ = await asyncio.wait(
                                {data_task, keepalive_task}, return_when=asyncio.FIRST_COMPLETED
                            )

                            if keepalive_task in done:
                                elapsed = time.monotonic() - last_data_time
                                if elapsed > STREAM_CHUNK_TIMEOUT:
                                    logger.warning(
                                        f"⚠️ Responses 流式 {elapsed:.0f}s 无数据，节点可能已断开 "
                                        f"[{current_req_id[:8]}]"
                                    )
                                    break
                                yield keepalive_task.result()
                                keepalive_task = asyncio.ensure_future(_do_keepalive())
                                continue

                            # 数据到达
                            last_data_time = time.monotonic()
                            data_task = asyncio.ensure_future(current_queue.get())
                            msg = done.pop().result()
                            if msg.get("type") == "finish":
                                stream_succeeded = True
                                for evt in converter.finalize():
                                    yield evt.encode("utf-8")
                                break
                            elif msg.get("type") == "error":
                                error_text = msg.get("body", "节点返回错误")
                                err_evt = f"event: error\ndata: {json.dumps({'type': 'error', 'message': error_text})}\n\n"
                                yield err_evt.encode("utf-8")
                                break
                            elif msg.get("type") == "chunk":
                                chunk_body = msg.get("body", "")
                                for line in chunk_body.split("\n"):
                                    for evt in converter.process_chunk(line):
                                        yield evt.encode("utf-8")
                    except (asyncio.TimeoutError, TimeoutError):
                        logger.error(f"⚠️ Responses 流式传输中断超时 [{current_req_id[:8]}]")
                        for evt in converter.finalize():
                            yield evt.encode("utf-8")
                    finally:
                        data_task.cancel()
                        keepalive_task.cancel()
                        await asyncio.gather(data_task, keepalive_task, return_exceptions=True)
                        cleanup_pending_request(current_req_id)
                        usage_obj = getattr(converter, "_usage", None)
                        usage_data = usage_obj.model_dump() if usage_obj is not None else None
                        record_request_finished(
                            route_key=route_key,
                            status_code=status_code if stream_succeeded else 502,
                            started_at=request_started_at,
                            first_byte_at=first_byte_at,
                            success=stream_succeeded,
                            usage=usage_data,
                        )

                log_fn = logger.debug if status_code == 200 else logger.info
                log_fn(f"👈 建立 Responses 流式管道 [{req_id[:8]}]")
                return StreamingResponse(
                    responses_stream_generator(req_id, queue),
                    status_code=status_code,
                    media_type="text/event-stream",
                    headers={"cache-control": "no-cache", "x-accel-buffering": "no"},
                )
            else:
                # 非流式: 收集完整响应后转换
                raw_body = await collect_response_body(req_id, queue)
                try:
                    chat_resp = json.loads(raw_body)
                except json.JSONDecodeError:
                    logger.error(f"⚠️ Responses 上游响应不是合法 JSON [{req_id[:8]}]")
                    record_request_finished(
                        route_key=route_key,
                        status_code=502,
                        started_at=request_started_at,
                        first_byte_at=first_byte_at,
                        success=False,
                    )
                    return JSONResponse({"error": {"message": "上游返回了非法 JSON"}}, status_code=502)

                responses_resp = responses_convert_response(chat_resp)
                record_request_finished(
                    route_key=route_key,
                    status_code=status_code,
                    started_at=request_started_at,
                    first_byte_at=first_byte_at,
                    success=True,
                    usage=chat_resp.get("usage"),
                )
                return JSONResponse(content=responses_resp)

        except asyncio.TimeoutError:
            logger.error(f"⚠️ Responses 请求等待内网节点超时 (30s) [{req_id[:8]}]，尝试切换...")
            retry_state.status_code = 504
            retry_state.response_text = "Gateway Error: 请求内网节点超时 (30s)"
            cleanup_pending_request(req_id)
            continue

    record_request_finished(
        route_key=route_key,
        status_code=retry_state.status_code,
        started_at=request_started_at,
        first_byte_at=None,
        success=False,
    )
    return Response(retry_state.response_text, status_code=retry_state.status_code)

# (id, display_name, context_length, max_output)
_MODELS = [
    ("mimo-v2.5-pro", "MiMo V2.5 Pro", 1048576, 131072),
    ("mimo-v2.5", "MiMo V2.5", 1048576, 131072),
    ("mimo-v2.5-tts", "MiMo V2.5 TTS", 8192, 8192),
    ("mimo-v2-pro", "MiMo V2 Pro", 1048576, 131072),
    ("mimo-v2-flash", "MiMo V2 Flash", 256000, 131072),
    ("mimo-v2-omni", "MiMo V2 Omni", 256000, 131072),
    ("mimo-v2.5-tts-voicedesign", "MiMo V2.5 TTS VoiceDesign", 8192, 8192),
    ("mimo-v2.5-tts-voiceclone", "MiMo V2.5 TTS VoiceClone", 8192, 8192),
    ("mimo-v2-tts", "MiMo V2 TTS", 8192, 8192),
]


@app.get("/v1/models")
async def get_models():
    data = [
        {
            "id": m[0], "object": "model", "created": 1700000000,
            "owned_by": "mimo", "context_length": m[2], "max_tokens": m[2],
        } for m in _MODELS
    ]
    return JSONResponse(content={"object": "list", "data": data})


@app.get("/anthropic/v1/models")
async def get_anthropic_models():
    data = [
        {
            "id": m[0], "display_name": m[1], "created_at": "2025-01-01T00:00:00Z",
            "type": "model", "max_input_tokens": m[2], "max_tokens": m[3],
        } for m in _MODELS
    ]
    return JSONResponse(content={"data": data, "has_more": False, "first_id": data[0]["id"], "last_id": data[-1]["id"]})

@app.post("/v1/chat/completions")
async def chat_completions_handler(request: Request):
    return await _forward_request(request, "/v1/chat/completions")

@app.post("/anthropic/v1/messages")
async def anthropic_messages_handler(request: Request):
    return await _forward_request(request, "/anthropic/v1/messages")

async def _forward_request(request: Request, path: str):
    """通用请求转发：将 HTTP 请求通过 WS 透传给内网节点。"""
    if not state.active_clients:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    body = await request.body()
    method = request.method

    max_retries = len(state.active_clients)
    if max_retries == 0:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    retry_state = RetryState()
    body_text = body.decode("utf-8", "ignore").lstrip("\ufeff")
    body_text = apply_model_mapping(body_text)
    route_key = path
    request_started_at = time.monotonic()

    is_streaming = False
    try:
        is_streaming = json.loads(body_text).get("stream", False) is True
    except (json.JSONDecodeError, AttributeError):
        pass
    record_request_started(route_key, is_streaming=is_streaming)

    for attempt in range(max_retries):
        req_id = "unknown"
        try:
            prepared = await prepare_forward_attempt(
                method=method,
                path=path,
                body=body_text,
                log_label="转发请求",
                retry_state=retry_state,
                attempt_number=attempt + 1,
            )
            if prepared is None:
                continue
            req_id = prepared.req_id
            queue = prepared.queue
            first_msg = prepared.first_msg
            status_code = first_msg.get("status", 200)
            first_byte_at = time.monotonic()

            content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))

            # 构造流式生成器，边收 WS 数据边吐给外部 HTTP
            async def stream_generator(current_req_id, current_queue, use_keepalive):
                last_data_time = time.monotonic()
                data_task = asyncio.ensure_future(current_queue.get())
                keepalive_task = None
                stream_succeeded = False
                usage_data = None

                async def _do_keepalive():
                    await asyncio.sleep(STREAM_KEEPALIVE_INTERVAL)
                    return b": keep-alive\n\n"

                if use_keepalive:
                    keepalive_task = asyncio.ensure_future(_do_keepalive())

                try:
                    while True:
                        pending = {data_task}
                        if keepalive_task is not None:
                            pending.add(keepalive_task)
                        done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

                        if keepalive_task is not None and keepalive_task in done:
                            elapsed = time.monotonic() - last_data_time
                            if elapsed > STREAM_CHUNK_TIMEOUT:
                                logger.warning(
                                    f"⚠️ 流式 {elapsed:.0f}s 无数据，节点可能已断开 "
                                    f"[{current_req_id[:8]}]"
                                )
                                break
                            yield keepalive_task.result()
                            keepalive_task = asyncio.ensure_future(_do_keepalive())
                            continue

                        # 数据到达
                        last_data_time = time.monotonic()
                        data_task = asyncio.ensure_future(current_queue.get())
                        msg = done.pop().result()
                        if msg.get("type") == "finish":
                            stream_succeeded = True
                            break
                        elif msg.get("type") == "chunk":
                            chunk_body = msg.get("body", "")
                            if usage_data is None:
                                usage_data = extract_usage_from_sse_chunk(chunk_body)
                            yield chunk_body.encode("utf-8")
                except (asyncio.TimeoutError, TimeoutError):
                    logger.error(f"⚠️ 流式传输意外中断超时 [{current_req_id[:8]}]")
                finally:
                    data_task.cancel()
                    if keepalive_task is not None:
                        keepalive_task.cancel()
                    await asyncio.gather(
                        *[t for t in (data_task, keepalive_task) if t is not None],
                        return_exceptions=True,
                    )
                    cleanup_pending_request(current_req_id)
                    final_success = stream_succeeded and status_code < 400
                    record_request_finished(
                        route_key=route_key,
                        status_code=status_code if stream_succeeded else 502,
                        started_at=request_started_at,
                        first_byte_at=first_byte_at,
                        success=final_success,
                        usage=usage_data,
                    )

            log_fn = logger.debug if status_code == 200 else logger.info
            log_fn(f"👈 建立流式响应管道 [{req_id[:8]}] - 状态码: {status_code}")
            if status_code >= 400:
                try:
                    _model = json.loads(body_text).get("model", "未指定")
                except Exception:
                    _model = "解析失败"
                logger.warning(f"⚠️ 上游返回 {status_code} [{req_id[:8]}] model={_model}, 请求体: {body_text[:500]}")
            return StreamingResponse(
                stream_generator(req_id, queue, use_keepalive=is_streaming),
                status_code=status_code,
                media_type=content_type,
                headers=response_headers,
            )
            
        except asyncio.TimeoutError:
            logger.error(f"⚠️ 请求等待内网节点超时 (30s) [{req_id[:8]}]，尝试切换...")
            retry_state.status_code = 504
            retry_state.response_text = "Gateway Error: 请求所有节点超时 (30s)"
            cleanup_pending_request(req_id)
            continue

    # 如果所有重试都失败，返回最后一次的状态
    record_request_finished(
        route_key=route_key,
        status_code=retry_state.status_code,
        started_at=request_started_at,
        first_byte_at=None,
        success=False,
    )
    return Response(retry_state.response_text, status_code=retry_state.status_code)

if __name__ == "__main__":
    logger.info("🚀 启动支持多节点的公网网关...")
    # 设置 ws_max_size 为无限制或足够大，允许接收包含 Base64 的大 JSON
    uvicorn.run(app, host="0.0.0.0", port=8000, ws_max_size=10**8)
