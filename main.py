import logging
from contextlib import asynccontextmanager

import httpx
import uvicorn

from gateway.config import load_settings
from gateway.health import HealthTracker
from gateway.limiter import RateLimiter
from gateway.proxy import Gateway, create_app
from gateway.routing import RouteTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

settings = load_settings()

routes = RouteTable()
health = HealthTracker(settings.gateway)
limiter = RateLimiter()
client = httpx.AsyncClient()
gateway = Gateway(routes, health, limiter, client)


@asynccontextmanager
async def lifespan(app):
    for u in settings.initial_upstreams:
        await routes.add_upstream(u)
    for r in settings.initial_routes:
        await routes.add_route(r)
    yield
    await client.aclose()


app = create_app(gateway, lifespan=lifespan)

if __name__ == "__main__":
    uvicorn.run(app, host=settings.host, port=settings.port)
