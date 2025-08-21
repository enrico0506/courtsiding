# server.py
import json
import time
import asyncio
import logging
from typing import Set, Dict, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse, JSONResponse

# --- logging config: goes to stdout immediately ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
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

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    peer = getattr(ws.client, "host", "?"), getattr(ws.client, "port", "?")
    CLIENTS.add(ws)
    CLIENT_META[ws] = {"connected_at": time.time(), "peer": peer}
    log.info(f"WS connected from {peer}. Active: {len(CLIENTS)}")

    # keep-alive pings
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

            if m.lower() == "now":
                # print + flush (for safety) and also log
                print("now", flush=True)
                log.info("Received 'now' from %s", peer)
                # do NOT reply to client
                continue

            # stay silent for everything else
            # (you can uncomment these to help debug)
            # log.debug("Ignoring message: %r", m)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.warning("WS error from %s: %s", peer, e)
    finally:
        ping_task.cancel()
        CLIENTS.discard(ws)
        CLIENT_META.pop(ws, None)
        log.info(f"WS disconnected {peer}. Active: {len(CLIENTS)}")
