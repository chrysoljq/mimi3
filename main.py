#!/usr/bin/env python3
"""
mimo2api 系统统一个化主入口

启动前只需修改此处的全局配置。
"""
import os
import sys
import logging
import uvicorn

# ================= 统一全局配置 =================
# 本地网关绑定的物理IP与端口
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 8000

# 内网(Claw)环境要求连回来的公网穿透访问 WS 地址。请随你的 frp/ngrok/隧道 变更而修改！
WS_TUNNEL_URL = f"ws://your-domain.com:{SERVER_PORT}/ws"
# ================================================


# 注入环境变量，让系统下面的 mimo2api/manager.py 层能读取到并替换进代码文本
os.environ["MIMO2API_WS_URL"] = WS_TUNNEL_URL

# 引入实际带 Lifespan 背景挂载服务的 FastAPI APP 对象
from mimo2api.web_service import app

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info(f"🚀 mimo2api 统一主入口 - 正在启动网关并绑定集群到 {SERVER_HOST}:{SERVER_PORT}")
    logging.info(f"🔗 云端要求 Claw 主动连接的桥接 WS URL 将统一下发为: {WS_TUNNEL_URL}")
    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT, ws_max_size=10**8)
