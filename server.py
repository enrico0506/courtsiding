# server.py
# requirements:
#   fastapi
#   uvicorn
#   redis>=4.6
#   uvloop (optional, but recommended: run uvicorn with --loop uvloop)

import time
import asyncio
import logging
from typing import Set, Dict, Any, Optional, Iterable, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse, JSONResponse
import redis.asyncio as redis  # async Redis client

# ---------- Tuning knobs ----------
LOG_LEVEL = logging.WARNING  # set to logging.INFO while debugging
ENABLE_ACK = False           # True = echo 'ack:<msg>' back to sender (adds a hop)
CHANNEL = "ws-broadcast"     # Redis pub/sub channel

# Hardcoded Upstash URL (token+host). You asked for no env vars.
REDIS_URL: str = (
    "rediss://default:"
    "AUK3AAIncDEzMmM3YmRlYjdmMWU0YmFjYTM3MWNiZTJhOGFkYTcyOHAxMTcwNzk"
    "@probable-viper-17079.upstash.io:6379"
)

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ws")

app = FastAPI()
START_TIME = time.time()

# Connected clients (per-process)
CLIENTS: Set[WebSocket] = set()
CLIENT_META: Dict[WebSocket, Dict[str, Any]] = {}

# Redis objects
r_pub: Optional[redis.Redis] = None
r_sub: Optional[redis.Redis] = None
sub_task: Optional[asyncio.Task] = None
pub_task: Optional[asyncio.Task] = None
PUB_QUEUE: "asyncio.Queue[str]" = asyncio.Queue(maxsize=1000)  # backpressure if needed


# ---------- Fast local broadcast ----------
async def _send_many(msg: str, conns: Iterable[WebSocket]) -> None:
    # Send concurrently to minimize tail latency
    coros: List[asyncio.Task] = []
    for ws in conns:
        coros.append(asyncio.create_task(ws.send_text(msg)))
    if coros:
        # Shield: errors on one socket shouldn't cancel others
        results = await asyncio.gather(*coros, return_exceptions=True)
        # Prune any dead sockets
        for ws, res in zip(conns, results):
            if isinstance(res, Exception):
                CLIENTS.discard(ws)
                CLIENT_META.pop(ws, None)

async def broadcast_local(message: str, exclude: Optional[WebSocket] = None) -> None:
    if not CLIENTS:
        return
    targets = [ws for ws in CLIENTS if ws is not exclude]
    if targets:
        await _send_many(message, targets)


# ---------- Redis subscriber (fan-in from other instances) ----------
async def redis_subscriber_loop():
    assert r_sub is not None
    backoff = 0.5
    while True:
        pubsub = None
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
                # Fan-out locally; DO NOT republish
                await broadcast_local(data)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.warning("Redis subscriber error: %s", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 5.0)
        finally:
            if pubsub:
                try:
                    await pubsub.close()
                except Exception:
                    pass


# ---------- Redis publisher (decoupled from WS handler) ----------
async def redis_publisher_loop():
    assert r_pub is not None
    while True:
        try:
            msg = await PUB_QUEUE.get()
            try:
                await r_pub.publish(CHANNEL, msg)
            except Exception as e:
                log.warning("Redis publish failed: %s", e)
            finally:
                PUB_QUEUE.task_done()
        except asyncio.CancelledError:
            break


# ---------- FastAPI lifecycle ----------
@app.on_event("startup")
async def on_startup():
    global r_pub, r_sub, sub_task, pub_task
    # Create Redis clients with a few sensible socket options
    r_pub = redis.from_url(
        REDIS_URL,
        socket_timeout=2.0,
        socket_connect_timeout=2.0,
        retry_on_timeout=True,
    )
    r_sub = redis.from_url(
        REDIS_URL,
        socket_timeout=0.0,  # pubsub blocks; no per-call timeout
        socket_connect_timeout=2.0,
        retry_on_timeout=True,
    )
    try:
        await r_pub.ping()
        log.info("Connected to Redis at %s", REDIS_URL.split("@")[-1])
    except Exception as e:
        log.warning("Redis ping failed: %s", e)

    # Background tasks
    sub_task = asyncio.create_task(redis_subscriber_loop())
    pub_task = asyncio.create_task(redis_publisher_loop())
    log.info("Startup complete.")


@app.on_event("shutdown")
async def on_shutdown():
    global sub_task, pub_task, r_pub, r_sub
    # Stop tasks
    for t in (sub_task, pub_task):
        if t:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
    # Close Redis
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


# ---------- WebSocket endpoint ----------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    CLIENTS.add(ws)
    CLIENT_META[ws] = {"connected_at": time.time(), "peer": (getattr(ws.client, "host", "?"), getattr(ws.client, "port", "?"))}
    if LOG_LEVEL <= logging.INFO:
        log.info("WS connected. Active: %d", len(CLIENTS))

    try:
        while True:
            # Receive (this await is the only unavoidable hop here)
            m = (await ws.receive_text()).strip()

            # Optional immediate ack to sender (off by default)
            if ENABLE_ACK:
                # Don't await broadcast here; acks are single-target
                try:
                    await ws.send_text(f"ack:{m}")
                except Exception:
                    pass

            # Fast path: local broadcast FIRST (no Redis wait for same-instance clients)
            await broadcast_local(m, exclude=ws)

            # Queue the message for Redis publish (cross-instance)
            # put_nowait avoids a context switch; if the queue is full, drop oldest to keep latency low
            try:
                PUB_QUEUE.put_nowait(m)
            except asyncio.QueueFull:
                # Drop head to maintain low latency (optional strategy)
                try:
                    _ = PUB_QUEUE.get_nowait()
                    PUB_QUEUE.task_done()
                except Exception:
                    pass
                PUB_QUEUE.put_nowait(m)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        if LOG_LEVEL <= logging.WARNING:
            log.warning("WS error: %s", e)
    finally:
        CLIENTS.discard(ws)
        CLIENT_META.pop(ws, None)
        if LOG_LEVEL <= logging.INFO:
            log.info("WS disconnected. Active: %d", len(CLIENTS))
