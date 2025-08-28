# server.py
# requirements:
#   fastapi
#   uvicorn
#   redis>=4.6
#   uvloop (optional)

import time
import asyncio
import logging
from typing import Set, Dict, Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse, JSONResponse
import redis.asyncio as redis  # async Redis client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ws")

app = FastAPI()

START_TIME = time.time()
CLIENTS: Set[WebSocket] = set()
CLIENT_META: Dict[WebSocket, Dict[str, Any]] = {}

# ---------- Redis configuration ----------
# Hardcoded Upstash URL (includes token + host)
REDIS_URL: str = (
    "rediss://default:"
    "AUK3AAIncDEzMmM3YmRlYjdmMWU0YmFjYTM3MWNiZTJhOGFkYTcyOHAxMTcwNzk"
    "@probable-viper-17079.upstash.io:6379"
)
CHANNEL = "ws-broadcast"

r_pub: Optional[redis.Redis] = None
r_sub: Optional[redis.Redis] = None
sub_task: Optional[asyncio.Task] = None

async def broadcast_local(message: str, exclude: Optional[WebSocket] = None):
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
    """Listen on Redis and fan-out to clients in this process. Reconnects on error."""
    assert r_sub is not None
    backoff = 0.5
    while True:
        try:
            pubsub = r_sub.pubsub(ignore_subscribe_messages=True)
            await pubsub.subscribe(CHANNEL)
            log.info("Redis subscribed to %s", CHANNEL)

            async for msg in pubsub.listen():
                if msg.get("type") != "message":
                    continue
                data = msg["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8", "replace")
                await broadcast_local(data)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.warning("Redis subscriber error: %s", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)
        finally:
            try:
                await pubsub.close()  # type: ignore[name-defined]
            except Exception:
                pass

@app.on_event("startup")
async def on_startup():
    global r_pub, r_sub, sub_task
    r_pub = redis.from_url(REDIS_URL)
    r_sub = redis.from_url(REDIS_URL)
    try:
        await r_pub.ping()
        log.info("Connected to Redis at %s", REDIS_URL.split("@")[-1])
    except Exception as e:
        log.warning("Redis ping failed: %s", e)
    sub_task = asyncio.create_task(redis_subscriber_loop())
    log.info("Startup complete (Redis ON).")

@app.on_event("shutdown")
async def on_shutdown():
    global sub_task, r_pub, r_sub
    if sub_task:
        sub_task.cancel()
        try:
            await sub_task
        except asyncio.CancelledError:
            pass
    if r_pub:
        await r_pub.aclose()
    if r_sub:
        await r_sub.aclose()
    log.info("Shutdown complete.")

# ---------- HTTP endpoints ----------
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

@app.get("/redis")
async def redis_info():
    try:
        pong = await r_pub.ping()
        return JSONResponse({"redis": "ok", "ping": pong, "channel": CHANNEL})
    except Exception as e:
        return JSONResponse({"redis": "error", "detail": str(e)}, status_code=500)

# ---------- WebSocket endpoint ----------
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

            try:
                await ws.send_text(f"ack:{m}")
            except Exception:
                pass

            try:
                await r_pub.publish(CHANNEL, m)
            except Exception as e:
                log.warning("Redis publish failed: %s", e)
                await broadcast_local(m, exclude=ws)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.warning("WS error from %s: %s", peer, e)
    finally:
        CLIENTS.discard(ws)
        CLIENT_META.pop(ws, None)
        log.info("WS disconnected %s. Active: %d", peer, len(CLIENTS))
