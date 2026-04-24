import asyncio
import base64
import binascii
import json
import uuid
import logging
import time
from dataclasses import dataclass
from typing import Any
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
import uvicorn
import os

# 引入 Manager 长驻协程任务
from .manager import start_manager_tasks, trigger_rebuild

# Responses API 转换器
from .responses_converter import convert_request as responses_convert_request
from .responses_converter import convert_response as responses_convert_response
from .responses_converter import ResponsesStreamConverter

# 配置基础日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

manager_bg_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global manager_bg_task
    logger.info("🚀 正在拉起挂后台的 Claw 账号守护线程...")
    manager_bg_task = asyncio.create_task(start_manager_tasks())
    yield
    if manager_bg_task:
        manager_bg_task.cancel()

app = FastAPI(lifespan=lifespan)

# 全局状态从 gateway_state 引入
from .gateway_state import state

# 注入前面拆分出的 WebUI 独立路由
from .ui_router import router as ui_router
app.include_router(ui_router)

RETRYABLE_STATUS_CODES = {401, 403, 429}
NODE_RESPONSE_TIMEOUT = 30
STREAM_CHUNK_TIMEOUT = 120
STREAM_KEEPALIVE_INTERVAL = 25  # 秒，需小于 Cloudflare 超时 (~100s)
QUEUE_DRAIN_TIMEOUT = 5
DEFAULT_GATEWAY_ERROR = "Gateway Error: 所有节点请求失败"
NODE_401_COOLDOWN_SECONDS = int(os.getenv("MIMO_NODE_401_COOLDOWN_SECONDS", "900"))

# 后台 fire-and-forget 任务集合，防止 GC 回收和 "exception was never retrieved" 警告
_background_tasks: set[asyncio.Task] = set()


def _track_task(task: asyncio.Task) -> None:
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


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


class AudioSpeechRequest(BaseModel):
    input: str = Field(min_length=1)
    model: str | None = None
    voice: str | None = None
    response_format: str = "wav"
    instructions: str | None = None

@app.post("/api/rebuild")
async def api_rebuild():
    """手动触发所有 Claw 节点强制销毁重建（用于更新 bridge.py 等场景）"""
    trigger_rebuild()
    logger.info("🔔 手动重建信号已发送")
    return JSONResponse(content={"ok": True, "message": "重建信号已发送，所有节点将在当前循环结束后立即重建"})

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


def cleanup_pending_request(req_id: str) -> None:
    state.pending_queues.pop(req_id, None)


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


def node_label(ws: WebSocket) -> str:
    return ws.client.host if ws.client else "Unknown"


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

    ws_payload = build_ws_payload(req_id, method, path, body)

    try:
        await target_ws.send_text(ws_payload)
        logger.info(
            f"👉 {log_label} [{req_id[:8]}] ({method} {path}) -> 节点: {node_label(target_ws)} "
            f"(尝试 {attempt_number})"
        )
    except RuntimeError:
        logger.warning(f"⚠️ {log_label} 转发失败，节点状态异常，尝试切换...")
        if target_ws in state.active_clients:
            state.active_clients.remove(target_ws)
        state.client_cooldowns.pop(id(target_ws), None)
        cleanup_pending_request(req_id)
        return None

    first_msg = await asyncio.wait_for(queue.get(), timeout=NODE_RESPONSE_TIMEOUT)
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


def map_openai_tts_voice(voice: str | None) -> str:
    default_voice_map = {
        "alloy": "mimo_default",
        "ash": "mimo_default",
        "ballad": "mimo_default",
        "coral": "mimo_default",
        "echo": "mimo_default",
        "fable": "mimo_default",
        "nova": "mimo_default",
        "onyx": "mimo_default",
        "sage": "mimo_default",
        "shimmer": "mimo_default",
        "verse": "mimo_default",
    }
    override_map_raw = os.getenv("MIMO_TTS_VOICE_MAP", "").strip()
    if override_map_raw:
        try:
            override_map = json.loads(override_map_raw)
            if isinstance(override_map, dict):
                default_voice_map.update({str(k): str(v) for k, v in override_map.items()})
        except json.JSONDecodeError:
            logger.warning("⚠️ MIMO_TTS_VOICE_MAP 不是合法 JSON，忽略自定义语音映射")

    if not voice:
        return "mimo_default"
    return default_voice_map.get(voice, voice)


def map_openai_tts_model(model: str | None) -> str:
    if not model:
        return "mimo-v2-tts"
    model_map = {
        "tts-1": "mimo-v2-tts",
        "tts-1-hd": "mimo-v2-tts",
        "gpt-4o-mini-tts": "mimo-v2-tts",
        "mimo-v2-tts": "mimo-v2-tts",
    }
    return model_map.get(model, "mimo-v2-tts")


def audio_media_type(audio_format: str) -> str:
    media_types = {
        "aac": "audio/aac",
        "flac": "audio/flac",
        "mp3": "audio/mpeg",
        "opus": "audio/ogg",
        "pcm": "audio/pcm",
        "wav": "audio/wav",
    }
    return media_types.get(audio_format.lower(), "application/octet-stream")


def pick_nested_value(data: Any, path: list[Any]) -> Any:
    current = data
    for key in path:
        if isinstance(key, int):
            if not isinstance(current, list) or key >= len(current):
                return None
            current = current[key]
        else:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
            if current is None:
                return None
    return current


def extract_audio_payload(data: Any) -> tuple[str | None, str | None]:
    # data 和 format 路径成对定义，避免交叉匹配
    audio_path_pairs = [
        (["audio", "data"], ["audio", "format"]),
        (["data", "audio", "data"], ["data", "audio", "format"]),
        (["choices", 0, "message", "audio", "data"], ["choices", 0, "message", "audio", "format"]),
        (["choices", 0, "audio", "data"], ["choices", 0, "audio", "format"]),
        (["output", "audio", "data"], ["output", "audio", "format"]),
    ]

    for data_path, fmt_path in audio_path_pairs:
        audio_b64 = pick_nested_value(data, data_path)
        if isinstance(audio_b64, str) and audio_b64:
            audio_format = pick_nested_value(data, fmt_path)
            return audio_b64, audio_format if isinstance(audio_format, str) else None

    if isinstance(data, dict):
        audio = data.get("audio")
        if isinstance(audio, dict):
            audio_b64 = audio.get("data")
            audio_format = audio.get("format")
            if isinstance(audio_b64, str) and audio_b64:
                return audio_b64, audio_format if isinstance(audio_format, str) else None
        for value in data.values():
            audio_b64, audio_format = extract_audio_payload(value)
            if audio_b64:
                return audio_b64, audio_format
    elif isinstance(data, list):
        for item in data:
            audio_b64, audio_format = extract_audio_payload(item)
            if audio_b64:
                return audio_b64, audio_format

    return None, None


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

            raw_body = await collect_response_body(req_id, queue)
            status_code = first_msg.get("status", 200)
            if status_code >= 400:
                content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))
                return Response(raw_body, status_code=status_code, media_type=content_type, headers=response_headers)

            try:
                response_json = json.loads(raw_body)
            except json.JSONDecodeError:
                logger.error(f"⚠️ TTS 上游响应不是合法 JSON [{req_id[:8]}]")
                return JSONResponse({"error": {"message": "上游 TTS 返回了非法 JSON"}}, status_code=502)

            audio_b64, actual_format = extract_audio_payload(response_json)
            if not audio_b64:
                logger.error(f"⚠️ TTS 上游响应中未找到音频数据 [{req_id[:8]}]")
                return JSONResponse({"error": {"message": "上游 TTS 响应里没有音频数据"}}, status_code=502)

            try:
                audio_bytes = base64.b64decode(audio_b64, validate=True)
            except binascii.Error:
                try:
                    audio_bytes = base64.b64decode(audio_b64)
                except binascii.Error:
                    logger.error(f"⚠️ TTS 音频 Base64 解码失败 [{req_id[:8]}]")
                    return JSONResponse({"error": {"message": "上游 TTS 音频数据损坏"}}, status_code=502)

            final_format = (actual_format or response_format).lower()
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

    return Response(retry_state.response_text, status_code=retry_state.status_code)

@app.post("/v1/responses")
async def responses_handler(request: Request):
    """将 OpenAI Responses API 请求转换为 Chat Completions 格式后转发。"""
    if not state.active_clients:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    body = await request.body()
    try:
        req_body = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse({"error": {"message": "请求体不是合法 JSON"}}, status_code=400)

    # 转换请求格式
    try:
        chat_req = responses_convert_request(req_body)
    except Exception as exc:
        logger.error(f"⚠️ Responses 请求转换失败: {exc}")
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

            if status_code >= 400:
                content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))
                raw_body = await collect_response_body(req_id, queue)
                return Response(raw_body, status_code=status_code, media_type=content_type, headers=response_headers)

            if is_streaming:
                # 流式: 将 chat/completions SSE chunks 转换为 responses API events
                converter = ResponsesStreamConverter(model=model)

                async def responses_stream_generator(current_req_id, current_queue):
                    # Responses 流式同样需要 keepalive 防止 Cloudflare 超时
                    last_data_time = time.monotonic()

                    async def _do_keepalive():
                        await asyncio.sleep(STREAM_KEEPALIVE_INTERVAL)
                        return b": keep-alive\n\n"

                    data_task = asyncio.ensure_future(current_queue.get())
                    keepalive_task = asyncio.ensure_future(_do_keepalive())

                    try:
                        while True:
                            pending = {data_task}
                            if keepalive_task is not None:
                                pending.add(keepalive_task)
                            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

                            if keepalive_task is not None and keepalive_task in done:
                                if time.monotonic() - last_data_time > STREAM_CHUNK_TIMEOUT:
                                    break
                                yield keepalive_task.result()
                                keepalive_task = asyncio.ensure_future(_do_keepalive())
                                # data_task 仍在 pending 中，下一轮继续复用
                                continue

                            # 真实数据到达，准备下一个 get() task
                            last_data_time = time.monotonic()
                            data_task = asyncio.ensure_future(current_queue.get())
                            msg = done.pop().result()
                            if msg.get("type") == "finish":
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
                        if keepalive_task is not None:
                            keepalive_task.cancel()
                        await asyncio.gather(
                            *[t for t in (data_task, keepalive_task) if t is not None],
                            return_exceptions=True,
                        )
                        cleanup_pending_request(current_req_id)

                logger.info(f"👈 建立 Responses 流式管道 [{req_id[:8]}]")
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
                    return JSONResponse({"error": {"message": "上游返回了非法 JSON"}}, status_code=502)

                responses_resp = responses_convert_response(chat_resp)
                return JSONResponse(content=responses_resp)

        except asyncio.TimeoutError:
            logger.error(f"⚠️ Responses 请求等待内网节点超时 (30s) [{req_id[:8]}]，尝试切换...")
            retry_state.status_code = 504
            retry_state.response_text = "Gateway Error: 请求内网节点超时 (30s)"
            cleanup_pending_request(req_id)
            continue

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
    body_text = body.decode("utf-8", "ignore")

    is_streaming = False
    try:
        is_streaming = json.loads(body_text).get("stream", False) is True
    except (json.JSONDecodeError, AttributeError):
        pass

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

            content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))

            # 构造流式生成器，边收 WS 数据边吐给外部 HTTP
            async def stream_generator(current_req_id, current_queue, use_keepalive):
                # keepalive 仅发送心跳注释，超时由 last_data_time 独立判断
                last_data_time = time.monotonic()
                keepalive_task = None
                data_task = asyncio.ensure_future(current_queue.get())

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
                            # keepalive 触发；检查真实数据是否已超时
                            if time.monotonic() - last_data_time > STREAM_CHUNK_TIMEOUT:
                                break
                            yield keepalive_task.result()
                            keepalive_task = asyncio.ensure_future(_do_keepalive())
                            # data_task 仍在 pending 中，下一轮继续复用
                            continue

                        # 真实数据到达，准备下一个 get() task
                        last_data_time = time.monotonic()
                        data_task = asyncio.ensure_future(current_queue.get())
                        msg = done.pop().result()
                        if msg.get("type") == "finish":
                            break
                        elif msg.get("type") == "chunk":
                            yield msg.get("body", "").encode("utf-8")
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

            logger.info(f"👈 建立流式响应管道 [{req_id[:8]}] - 状态码: {status_code}")
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
    return Response(retry_state.response_text, status_code=retry_state.status_code)

if __name__ == "__main__":
    logger.info("🚀 启动支持多节点的公网网关...")
    # 设置 ws_max_size 为无限制或足够大，允许接收包含 Base64 的大 JSON
    uvicorn.run(app, host="0.0.0.0", port=8000, ws_max_size=10**8)
