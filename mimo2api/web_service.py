import asyncio
import base64
import binascii
import json
import uuid
import logging
from typing import Any, Dict, List
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
import uvicorn
import os
import re

# 引入 Manager 长驻协程任务
from .manager import start_manager_tasks, trigger_rebuild

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
            logger.info(f"当前在线节点数: {len(state.active_clients)}")


# 轮询获取下一个可用的客户端
def get_next_client() -> WebSocket | None:
    if not state.active_clients:
        return None
    if state.current_client_index >= len(state.active_clients):
        state.current_client_index = 0
    client = state.active_clients[state.current_client_index]
    state.current_client_index = (state.current_client_index + 1) % len(state.active_clients)
    return client


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
    preferred_data_paths = [
        ["audio", "data"],
        ["data", "audio", "data"],
        ["choices", 0, "message", "audio", "data"],
        ["choices", 0, "audio", "data"],
        ["output", "audio", "data"],
    ]
    preferred_format_paths = [
        ["audio", "format"],
        ["data", "audio", "format"],
        ["choices", 0, "message", "audio", "format"],
        ["choices", 0, "audio", "format"],
        ["output", "audio", "format"],
    ]

    for path in preferred_data_paths:
        audio_b64 = pick_nested_value(data, path)
        if isinstance(audio_b64, str) and audio_b64:
            for fmt_path in preferred_format_paths:
                audio_format = pick_nested_value(data, fmt_path)
                if isinstance(audio_format, str) and audio_format:
                    return audio_b64, audio_format
            return audio_b64, None

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
async def audio_speech_handler(request: Request):
    if not state.active_clients:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return JSONResponse({"error": {"message": "请求体必须是合法 JSON"}}, status_code=400)

    input_text = payload.get("input")
    if not isinstance(input_text, str) or not input_text.strip():
        return JSONResponse({"error": {"message": "`input` 不能为空"}}, status_code=400)

    instructions = payload.get("instructions")
    response_format = str(payload.get("response_format") or "wav").lower()
    messages = []
    if isinstance(instructions, str) and instructions.strip():
        messages.append({"role": "user", "content": instructions})
    messages.append({"role": "assistant", "content": input_text})

    mimo_payload = {
        "model": map_openai_tts_model(payload.get("model")),
        "messages": messages,
        "audio": {
            "format": response_format,
            "voice": map_openai_tts_voice(payload.get("voice")),
        },
    }

    body_text = json.dumps(mimo_payload, ensure_ascii=False)
    max_retries = len(state.active_clients)
    last_status_code = 502
    last_response_text = "Gateway Error: 所有节点请求失败"

    for attempt in range(max_retries):
        req_id = str(uuid.uuid4())
        queue = asyncio.Queue()
        state.pending_queues[req_id] = queue

        ws_payload = json.dumps({
            "req_id": req_id,
            "method": "POST",
            "path": "/v1/chat/completions",
            "body": body_text,
        })

        target_ws = get_next_client()
        if not target_ws:
            state.pending_queues.pop(req_id, None)
            break

        try:
            await target_ws.send_text(ws_payload)
            logger.info(f"👉 TTS 映射请求 [{req_id[:8]}] -> 节点: {target_ws.client.host if target_ws.client else 'Unknown'} (尝试 {attempt + 1}/{max_retries})")
        except RuntimeError:
            logger.warning("⚠️ TTS 转发失败，节点状态异常，尝试切换...")
            if target_ws in state.active_clients:
                state.active_clients.remove(target_ws)
            state.pending_queues.pop(req_id, None)
            continue

        try:
            first_msg = await asyncio.wait_for(queue.get(), timeout=600)

            if first_msg.get("type") == "error":
                logger.warning(f"⚠️ TTS 节点返回内部错误: {first_msg.get('body')}，尝试切换...")
                last_response_text = f"Gateway Error: {first_msg.get('body')}"
                state.pending_queues.pop(req_id, None)
                continue

            status_code = first_msg.get("status", 200)
            if status_code in [401, 403, 429] or status_code >= 500:
                logger.warning(f"⚠️ TTS 节点返回状态码 {status_code}，触发自动重试 (当前 attempt={attempt+1}/{max_retries})...")
                last_status_code = status_code

                async def drain_and_close(r_id, q):
                    try:
                        while True:
                            msg = await asyncio.wait_for(q.get(), timeout=5)
                            if msg.get("type") in ["finish", "error"]:
                                break
                    except Exception:
                        pass
                    finally:
                        state.pending_queues.pop(r_id, None)

                asyncio.create_task(drain_and_close(req_id, queue))
                continue

            raw_body = await collect_response_body(req_id, queue)
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
            logger.error(f"⚠️ TTS 请求等待内网节点超时 (600s) [{req_id[:8]}]，尝试切换...")
            last_status_code = 504
            last_response_text = "Gateway Error: 请求内网节点超时 (600s)"
            state.pending_queues.pop(req_id, None)
            continue
        except RuntimeError as exc:
            logger.error(f"⚠️ TTS 响应收集失败 [{req_id[:8]}]: {exc}")
            last_status_code = 502
            last_response_text = f"Gateway Error: {exc}"
            continue

    return Response(last_response_text, status_code=last_status_code)

@app.get("/v1/models")
async def get_models():
    models_context = {
        "mimo-v2-pro": 1000000,
        "mimo-v2-flash": 256000,
        "mimo-v2-omni": 256000,
        "mimo-v2-tts": 8192
    }
    data = []
    for m, ctx in models_context.items():
        data.append({
            "id": m,
            "object": "model",
            "created": 1700000000,
            "owned_by": "mimo",
            "context_length": ctx,
            "max_tokens": ctx
        })
    return JSONResponse(content={"object": "list", "data": data})

@app.get("/anthropic/v1/models")
async def get_anthropic_models():
    models = [
        {"id": "mimo-v2-pro", "display_name": "MiMo V2 Pro", "model_context": 1000000},
        {"id": "mimo-v2-flash", "display_name": "MiMo V2 Flash", "model_context": 256000},
        {"id": "mimo-v2-omni", "display_name": "MiMo V2 Omni", "model_context": 256000},
    ]
    data = [
        {
            "id": m["id"], 
            "display_name": m["display_name"], 
            "created_at": "2025-01-01T00:00:00Z", 
            "type": "model", 
            "max_input_tokens": m["model_context"], 
            "max_tokens": m["model_context"],
        } for m in models
    ]
    return JSONResponse(content={"data": data, "has_more": False, "first_id": data[0]["id"], "last_id": data[-1]["id"]})

@app.api_route("/v1/chat/completions", methods=["GET", "POST", "PUT", "DELETE"])
async def chat_completions_handler(request: Request):
    return await _forward_request(request, "/v1/chat/completions")

@app.api_route("/anthropic/v1/messages", methods=["GET", "POST", "PUT", "DELETE"])
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

    last_status_code = 502
    last_response_text = "Gateway Error: 所有节点请求失败"

    for attempt in range(max_retries):
        req_id = str(uuid.uuid4())
        queue = asyncio.Queue()
        state.pending_queues[req_id] = queue

        ws_payload = json.dumps({
            "req_id": req_id,
            "method": method,
            "path": path,
            "body": body.decode("utf-8", "ignore")
        })

        target_ws = get_next_client()
        if not target_ws:
            state.pending_queues.pop(req_id, None)
            break

        try:
            await target_ws.send_text(ws_payload)
            logger.info(f"👉 转发请求 [{req_id[:8]}] ({method} {path}) -> 节点: {target_ws.client.host if target_ws.client else 'Unknown'} (尝试 {attempt + 1}/{max_retries})")
        except RuntimeError:
            logger.warning(f"⚠️ 转发失败，节点状态异常，尝试切换...")
            if target_ws in state.active_clients:
                state.active_clients.remove(target_ws)
            state.pending_queues.pop(req_id, None)
            continue

        try:
            # 等待第一次响应 (包含状态码和 Header 的 start 信号)
            first_msg = await asyncio.wait_for(queue.get(), timeout=600)
            
            if first_msg.get("type") == "error":
                logger.warning(f"⚠️ 节点返回内部错误: {first_msg.get('body')}，尝试切换...")
                last_response_text = f"Gateway Error: {first_msg.get('body')}"
                state.pending_queues.pop(req_id, None)
                continue

            status_code = first_msg.get("status", 200)
            
            # 遇到 401(小米并发限流) 或 429, 5xx 等异常状态码时重试
            if status_code in [401, 403, 429] or status_code >= 500:
                logger.warning(f"⚠️ 节点返回状态码 {status_code}，触发自动重试 (当前 attempt={attempt+1}/{max_retries})...")
                last_status_code = status_code
                
                # 启动后台任务排空队列剩余的数据并清理（避免内存泄漏）
                async def drain_and_close(r_id, q):
                    try:
                        while True:
                            msg = await asyncio.wait_for(q.get(), timeout=5)
                            if msg.get("type") in ["finish", "error"]:
                                break
                    except Exception:
                        pass
                    finally:
                        state.pending_queues.pop(r_id, None)
                
                asyncio.create_task(drain_and_close(req_id, queue))
                continue

            content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))

            # 构造流式生成器，边收 WS 数据边吐给外部 HTTP
            async def stream_generator(current_req_id, current_queue):
                try:
                    while True:
                        # 块与块之间的等待超时 (120秒)
                        msg = await asyncio.wait_for(current_queue.get(), timeout=120)
                        if msg.get("type") == "finish":
                            break
                        elif msg.get("type") == "chunk":
                            yield msg.get("body", "").encode("utf-8")
                except asyncio.TimeoutError:
                    logger.error(f"⚠️ 流式传输意外中断超时 [{current_req_id[:8]}]")
                finally:
                    # 传输结束，清理队列
                    state.pending_queues.pop(current_req_id, None)

            logger.info(f"👈 建立流式响应管道 [{req_id[:8]}] - 状态码: {status_code}")
            return StreamingResponse(
                stream_generator(req_id, queue),
                status_code=status_code,
                media_type=content_type,
                headers=response_headers,
            )
            
        except asyncio.TimeoutError:
            logger.error(f"⚠️ 请求等待内网节点超时 (600s) [{req_id[:8]}]，尝试切换...")
            last_status_code = 504
            last_response_text = "Gateway Error: 请求内网节点超时 (600s)"
            state.pending_queues.pop(req_id, None)
            continue

    # 如果所有重试都失败，返回最后一次的状态
    return Response(last_response_text, status_code=last_status_code)

if __name__ == "__main__":
    logger.info("🚀 启动支持多节点的公网网关...")
    # 设置 ws_max_size 为无限制或足够大，允许接收包含 Base64 的大 JSON
    uvicorn.run(app, host="0.0.0.0", port=8000, ws_max_size=10**8)
