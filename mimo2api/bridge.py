import asyncio, websockets, httpx, json, os

KEY = os.getenv("MIMO_API_KEY")
URL = os.getenv("MIMO_API_ENDPOINT")
BASE = URL.split("/v1/")[0] if "/v1/" in URL else URL
WS_URL = "__WS_URL__"

async def handle_request(ws, req):
    req_id = req["req_id"]
    async with httpx.AsyncClient(timeout=None) as client:
        try:
            async with client.stream(
                method=req["method"], 
                url=f"{BASE}/anthropic/v1/messages" if "/anthropic/" in req["path"] else URL, 
                headers={"api-key": KEY, "Content-Type": "application/json"}, 
                content=req.get("body", "")
            ) as r:
                await ws.send(json.dumps({
                    "req_id": req_id, "type": "start", 
                    "status": r.status_code, "headers": dict(r.headers)
                }))
                async for chunk in r.aiter_text():
                    if chunk:
                        await ws.send(json.dumps({
                            "req_id": req_id, "type": "chunk", "body": chunk
                        }))
                await ws.send(json.dumps({"req_id": req_id, "type": "finish"}))
                
        except Exception as e:
            await ws.send(json.dumps({"req_id": req_id, "type": "error", "body": str(e)}))

async def main():
    while True:
        try:
            async with websockets.connect(WS_URL, max_size=10**8) as ws:
                async for msg in ws:
                    asyncio.create_task(handle_request(ws, json.loads(msg)))
        except Exception:
            await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())