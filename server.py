# server.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse
import asyncio
import time

app = FastAPI()

@app.get("/", response_class=PlainTextResponse)
def root():
    return "WebSocket server is up. Connect to /ws\n"

@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    return "ok"

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    # keep-alive pings (some proxies idle out silent connections)
    async def pinger():
        while True:
            await asyncio.sleep(25)
            try:
                await ws.send_text("ping")
            except Exception:
                break

    ping_task = asyncio.create_task(pinger())
    try:
        while True:
            msg = await ws.receive_text()
            if msg.strip().lower() == "now":
                print("now")
                # (do not send anything back to client)
    except WebSocketDisconnect:
        pass
    finally:
        ping_task.cancel()
