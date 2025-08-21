# server.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse
import json
import asyncio
import time

app = FastAPI()

@app.get("/", response_class=PlainTextResponse)
def root():
    print("[INFO] Served GET / with 200 OK")
    return "WebSocket server is up. Connect to /ws\n"

@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    print("[INFO] Health check responded with 200 OK")
    return "ok"

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    await ws.send_text(json.dumps({"hello": "world", "ts": time.time()}))

    # keep-alive pings (some proxies idle out silent connections)
    async def pinger():
        while True:
            await asyncio.sleep(25)
            try:
                await ws.send_text(json.dumps({"type": "ping", "ts": time.time()}))
            except Exception:
                break

    ping_task = asyncio.create_task(pinger())
    try:
        while True:
            msg = await ws.receive_text()
            print(f"[INFO] Received from WS: {msg}")  # log WS traffic too
            await ws.send_text(json.dumps({"echo": msg, "ts": time.time()}))
    except WebSocketDisconnect:
        print("[INFO] WebSocket disconnected")
    finally:
        ping_task.cancel()
