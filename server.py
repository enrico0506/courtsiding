# requirements: fastapi, uvicorn, aioredis (or redis>=4), uvloop

import asyncio
import json
import os
import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
rpub = redis.from_url(REDIS_URL)
rsub = redis.from_url(REDIS_URL)

CHANNEL = "ws-broadcast"

async def redis_broadcast_loop():
    pubsub = rsub.pubsub()
    await pubsub.subscribe(CHANNEL)
    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        payload = message["data"].decode("utf-8")
        # fan-out to local process clients
        await broadcast(payload, exclude=None)

# when you want to broadcast:
await rpub.publish(CHANNEL, m)
