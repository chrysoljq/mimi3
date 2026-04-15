import asyncio
import json
import uuid
import logging
from typing import Dict, List
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.responses import StreamingResponse
import uvicorn

# 引入 Manager 长驻协程任务
from manager import start_manager_tasks

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

# 全局状态
class GatewayState:
    def __init__(self):
        self.active_clients: List[WebSocket] = []
        self.pending_queues: Dict[str, asyncio.Queue] = {}
        self.current_client_index: int = 0

state = GatewayState()

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
            else:
                logger.warning(f"收到未知的响应 ID: {req_id}")

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

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def http_handler(request: Request, path: str):
    if not state.active_clients:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    req_id = str(uuid.uuid4())
    queue = asyncio.Queue()
    state.pending_queues[req_id] = queue

    body = await request.body()
    method = request.method
    
    ws_payload = json.dumps({
        "req_id": req_id,
        "method": method,
        "path": f"/{path}", # 保留完整路径以备未来内网客户端需要
        "body": body.decode("utf-8", "ignore")
    })

    # 尝试寻找可用节点发送，重试次数等于在线节点数
    max_retries = len(state.active_clients)
    sent_successfully = False

    for _ in range(max_retries):
        target_ws = get_next_client()
        if not target_ws:
            break
            
        try:
            await target_ws.send_text(ws_payload)
            logger.info(f"👉 转发请求 [{req_id[:8]}] ({method} /{path}) -> 节点: {target_ws.client.host if target_ws.client else 'Unknown'}")
            sent_successfully = True
            break # 发送成功，跳出重试循环
        except RuntimeError:
             # WebSocket 可能在刚才已经关闭，从列表中移除
             logger.warning(f"⚠️ 转发失败，节点状态异常，尝试下一个...")
             if target_ws in state.active_clients:
                 state.active_clients.remove(target_ws)

    if not sent_successfully:
         state.pending_queues.pop(req_id, None)
         return Response("Gateway Error: 所有节点发送失败", status_code=502)

    try:
        # 等待第一次响应 (包含状态码和 Header 的 start 信号)
        first_msg = await asyncio.wait_for(queue.get(), timeout=600)
        
        if first_msg.get("type") == "error":
            state.pending_queues.pop(req_id, None)
            return Response(f"Gateway Error: {first_msg.get('body')}", status_code=502)

        status_code = first_msg.get("status", 200)
        content_type = first_msg.get("headers", {}).get("content-type", "application/json")

        # 构造流式生成器，边收 WS 数据边吐给外部 HTTP
        async def stream_generator():
            try:
                while True:
                    # 块与块之间的等待超时 (120秒)
                    msg = await asyncio.wait_for(queue.get(), timeout=120)
                    if msg.get("type") == "finish":
                        break
                    elif msg.get("type") == "chunk":
                        yield msg.get("body", "").encode("utf-8")
            except asyncio.TimeoutError:
                logger.error(f"⚠️ 流式传输意外中断超时 [{req_id[:8]}]")
            finally:
                # 传输结束，清理队列
                state.pending_queues.pop(req_id, None)

        logger.info(f"👈 建立流式响应管道 [{req_id[:8]}] - 状态码: {status_code}")
        return StreamingResponse(stream_generator(), status_code=status_code, media_type=content_type)
        
    except asyncio.TimeoutError:
        logger.error(f"⚠️ 请求超时 [{req_id[:8]}]")
        state.pending_queues.pop(req_id, None)
        return Response("Gateway Error: 请求内网节点超时 (600s)", status_code=504)

if __name__ == "__main__":
    logger.info("🚀 启动支持多节点的公网网关...")
    # 设置 ws_max_size 为无限制或足够大，允许接收包含 Base64 的大 JSON
    uvicorn.run(app, host="0.0.0.0", port=8000, ws_max_size=10**8)