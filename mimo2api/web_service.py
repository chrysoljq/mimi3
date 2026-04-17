import asyncio
import json
import uuid
import logging
from typing import Dict, List
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
import uvicorn
import os
import re

# 引入 Manager 长驻协程任务
from .manager import start_manager_tasks

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
    state.current_client_index = (state.current_client_index + 1) % len(state.active_clients)
    return state.active_clients[state.current_client_index]

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

@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def http_handler(request: Request, path: str):
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
            "path": f"/v1/{path}",
            "body": body.decode("utf-8", "ignore")
        })

        target_ws = get_next_client()
        if not target_ws:
            state.pending_queues.pop(req_id, None)
            break
            
        try:
            await target_ws.send_text(ws_payload)
            logger.info(f"👉 转发请求 [{req_id[:8]}] ({method} /{path}) -> 节点: {target_ws.client.host if target_ws.client else 'Unknown'} (尝试 {attempt + 1}/{max_retries})")
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

            content_type = first_msg.get("headers", {}).get("content-type", "application/json")

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
            return StreamingResponse(stream_generator(req_id, queue), status_code=status_code, media_type=content_type)
            
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