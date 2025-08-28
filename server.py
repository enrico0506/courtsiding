# server.py
# requirements: fastapi uvicorn redis>=4.* uvloop (optional)
import os
import time
import asyncio
import logging
from typing import Set, Dict, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse, JSONResponse
import redis.asyncio as redis  # <- async Redis client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ws")

app = FastAPI()

START_TIME = time.time()
CLIENTS: Set[WebSocket] = set()
CLIENT_META: Dict[WebSocket, Dict[str, Any]] = {}

# ----- Redis fan-out (for multi-instance) -----
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CHANNEL = os.getenv("WS_CHANNEL", "ws-broadcast")

r_pub: redis.Redis | None = None
r_sub: redis.Redis | None = None
sub_task: asyncio.Task | None = None

async def broadcast_local(message: str, exclude: WebSocket | None = None):
    """Send to all connected clients in *this* process."""
    dead = []
    for ws in CLIENTS:
        if exclude is ws:
            continue
        try:
            await ws.send_text(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        CLIENTS.discard(ws)
        CLIENT_META.pop(ws, None)
        log.info("Pruned dead WS %s", ws)

async def redis_subscriber_loop():
    """Listen on Redis and fan-out to clients in this process."""
    assert r_sub is not None
    pubsub = r_sub.pubsub()
    await pubsub.subscribe(CHANNEL)
    log.info("Redis subscribed to %s", CHANNEL)
    try:
        async for msg in pubsub.listen():
            if msg.get("type") != "message":
                continue
            data = msg["data"]
            if isinstance(data, bytes):
                data = data.decode("utf-8", "replace")
            # Do NOT republish; only fan-out locally.
            await broadcast_local(data)
    finally:
        try:
            await pubsub.unsubscribe(CHANNEL)
        finally:
            await pubsub.close()

@app.on_event("startup")
async def on_startup():
    global r_pub, r_sub, sub_task
    # Connect Redis once per process
    r_pub = redis.from_url(REDIS_URL)
    r_sub = redis.from_url(REDIS_URL)
    # Spin up subscriber loop
    sub_task = asyncio.create_task(redis_subscriber_loop())
    log.info("Startup complete.")

@app.on_event("shutdown")
async def on_shutdown():
    global sub_task, r_pub, r_sub
    if sub_task:
        sub_task.cancel()
        try:
            await sub_task
        except asyncio.CancelledError:
            pass
    # Close Redis connections
    if r_pub:
        await r_pub.aclose()
    if r_sub:
        await r_sub.aclose()
    log.info("Shutdown complete.")

# ----- HTTP endpoints -----
@app.get("/", response_class=PlainTextResponse)
def root():
    return "WebSocket server is up. Connect to /ws\n"

@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    return "ok"

@app.get("/metrics")
def metrics():
    uptime = int(time.time() - START_TIME)
    return JSONResponse({"uptime_seconds": uptime, "connected_clients": len(CLIENTS)})

# ----- WebSocket endpoint -----
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    peer = (getattr(ws.client, "host", "?"), getattr(ws.client, "port", "?"))
    CLIENTS.add(ws)
    CLIENT_META[ws] = {"connected_at": time.time(), "peer": peer}
    log.info("WS connected from %s. Active: %d", peer, len(CLIENTS))

    try:
        while True:
            msg = await ws.receive_text()
            m = msg.strip()
            log.info("recv %r from %s (clients=%d)", m, peer, len(CLIENTS))

            # Optional: immediate ack to sender so single-client tests see feedback
            try:
                await ws.send_text(f"ack:{m}")
            except Exception:
                pass

            # Publish to Redis so *all instances* rebroadcast it to their local clients
            if r_pub:
                try:
                    await r_pub.publish(CHANNEL, m)
                except Exception as e:
                    log.warning("Redis publish failed: %s", e)
            else:
                # Fallback: single-instance local broadcast
                await broadcast_local(m, exclude=ws)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.warning("WS error from %s: %s", peer, e)
    finally:
        CLIENTS.discard(ws)
        CLIENT_META.pop(ws, None)
        log.info("WS disconnected %s. Active: %d", peer, len(CLIENTS))
