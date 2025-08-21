# server.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse
import json
import asyncio
import time

app = FastAPI()

@app.get("/", response_class=PlainTextResponse)
def root():
    return "WebSocket server is up. Connect to /ws\n"

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    # optional: send a greeting
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
            # echo back with timestamp
            await ws.send_text(json.dumps({"echo": msg, "ts": time.time()}))
    except WebSocketDisconnect:
        pass
    finally:
        ping_task.cancel()
