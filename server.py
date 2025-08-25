# fast_server.py
import time
import asyncio
import logging
from typing import Set, Dict, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse, JSONResponse

# --- logging config: fast, stdout only, no blocking handlers ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("ws")

app = FastAPI()

START_TIME = time.time()
CLIENTS: Set[WebSocket] = set()
CLIENT_META: Dict[WebSocket, Dict[str, Any]] = {}

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

async def broadcast(message: str, exclude: WebSocket = None):
    """Send message to all connected clients except optionally one."""
    dead = []
    for ws in CLIENTS:
        if ws is exclude:
            continue
        try:
            await ws.send_text(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        CLIENTS.discard(ws)
        CLIENT_META.pop(ws, None)
        log.info(f"Pruned dead WS {ws}")

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    peer = getattr(ws.client, "host", "?"), getattr(ws.client, "port", "?")
    CLIENTS.add(ws)
    CLIENT_META[ws] = {"connected_at": time.time(), "peer": peer}
    log.info(f"WS connected from {peer}. Active: {len(CLIENTS)}")

    try:
        while True:
            msg = await ws.receive_text()
            m = msg.strip()

            if m.lower().startswith("now"):
                # Broadcast immediately to all others
                await broadcast(m, exclude=ws)
                log.debug("Broadcast 'now' from %s to %d clients", peer, len(CLIENTS)-1)
            else:
                # Echo everything else too, so clients can see
                await broadcast(m, exclude=ws)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.warning("WS error from %s: %s", peer, e)
    finally:
        CLIENTS.discard(ws)
        CLIENT_META.pop(ws, None)
        log.info(f"WS disconnected {peer}. Active: {len(CLIENTS)}")
