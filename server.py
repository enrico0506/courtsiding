# server.py
import json
import time
import asyncio
from typing import Set, Dict, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse, JSONResponse

app = FastAPI()

START_TIME = time.time()
CLIENTS: Set[WebSocket] = set()
CLIENT_META: Dict[WebSocket, Dict[str, Any]] = {}

def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

@app.get("/", response_class=PlainTextResponse)
def root():
    return "WebSocket server is up. Connect to /ws\n"

@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    return "ok"

@app.get("/metrics")
def metrics():
    uptime = int(time.time() - START_TIME)
    return JSONResponse({
        "uptime_seconds": uptime,
        "connected_clients": len(CLIENTS),
    })

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    peer = getattr(ws.client, "host", "?"), getattr(ws.client, "port", "?")
    CLIENTS.add(ws)
    CLIENT_META[ws] = {"connected_at": time.time(), "peer": peer}
    print(f"[{ts()}] ✅ WS connected from {peer}. Active: {len(CLIENTS)}")

    # Keep-alive pings (helps through proxies/LBs)
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
            m = msg.strip()

            # Your original behavior: only react to "now"
            if m.lower() == "now":
                print("now")
                continue  # do not reply

            # Optional helper commands
            if m.lower() == "ping":
                await ws.send_text("pong")
                continue

            if m.lower() == "who":
                await ws.send_text(json.dumps({"connected": len(CLIENTS)}))
                continue

            if m.lower().startswith("echo "):
                await ws.send_text(m[5:])
                continue

            if m.lower().startswith("broadcast "):
                payload = m[len("broadcast "):]
                dead: list[WebSocket] = []
                for client in CLIENTS:
                    if client is ws:
                        continue
                    try:
                        await client.send_text(payload)
                    except Exception:
                        dead.append(client)
                for d in dead:
                    CLIENTS.discard(d)
                    CLIENT_META.pop(d, None)
                continue

            if m.lower() == "close":
                await ws.close(code=1000, reason="Client requested close")
                break

            # otherwise: stay silent

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[{ts()}] ⚠️ WS error from {peer}: {e}")
    finally:
        ping_task.cancel()
        CLIENTS.discard(ws)
        CLIENT_META.pop(ws, None)
        print(f"[{ts()}] 🔌 WS disconnected {peer}. Active: {len(CLIENTS)}")
