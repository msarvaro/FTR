"""shared stuff for all services: config, db pool, fault injection, events"""
import asyncio
import os
import random
import time

from fastapi import FastAPI, Request
from psycopg_pool import ConnectionPool
import psycopg

FT = os.getenv("FT_MODE", "ft") == "ft"          # "ft" or "baseline"
SERVICE = os.getenv("SERVICE_NAME", "service")
INSTANCE = os.getenv("INSTANCE", SERVICE)

# baseline: only the primary. ft: libpq multi-host, connects to whichever node is read-write
DB_HOSTS = os.getenv("DB_HOSTS", "db-primary")
DB_PORTS = os.getenv("DB_PORTS", "5432")
DB_USER = os.getenv("DB_USER", "uni")
DB_PASS = os.getenv("DB_PASS", "uni_pass")
DB_NAME = os.getenv("DB_NAME", "university")


def dsn():
    hosts = DB_HOSTS if FT else DB_HOSTS.split(",")[0]
    ports = DB_PORTS if FT else DB_PORTS.split(",")[0]
    extra = " target_session_attrs=read-write" if FT else ""
    return (f"host={hosts} port={ports} user={DB_USER} password={DB_PASS} dbname={DB_NAME} "
            f"connect_timeout=2{extra}")


_pool = None


def pool():
    global _pool
    if _pool is None:
        _pool = ConnectionPool(dsn(), min_size=1, max_size=8, open=False, timeout=5,
                               check=ConnectionPool.check_connection if FT else None)
        _pool.open(wait=False)
    return _pool


def db_call(fn, retries=None):
    """run fn(conn). ft: retry with exponential backoff on connection errors (db failover)"""
    attempts = (retries if retries is not None else 6) if FT else 1
    delay = 0.1
    for i in range(attempts):
        try:
            with pool().connection() as conn:
                return fn(conn)
        except (psycopg.OperationalError, psycopg.errors.AdminShutdown,
                psycopg.errors.ReadOnlySqlTransaction, psycopg.InterfaceError) as e:
            if i == attempts - 1:
                raise
            time.sleep(delay + random.random() * 0.05)
            delay = min(delay * 2, 1.6)


# ---------------------------------------------------------------- fault injection
FAULTS = {"latency_ms": 0, "crash_on_next_payment": False, "crash_after_items": 0, "error_rate": 0.0}


def crash(reason):
    print(f"[{INSTANCE}] injected crash: {reason}", flush=True)
    os._exit(1)


def make_app(title):
    app = FastAPI(title=title)

    @app.middleware("http")
    async def inject(request: Request, call_next):
        if FAULTS["latency_ms"] and not request.url.path.startswith(("/admin", "/health")):
            await asyncio.sleep(FAULTS["latency_ms"] / 1000)
        return await call_next(request)

    @app.get("/health")
    def health():
        return {"status": "ok", "instance": INSTANCE, "mode": "ft" if FT else "baseline"}

    @app.post("/admin/fault")
    async def set_fault(request: Request):
        body = await request.json()
        if body.get("crash_now"):
            crash("crash_now requested")
        FAULTS.update({k: v for k, v in body.items() if k in FAULTS})
        return {"instance": INSTANCE, "faults": FAULTS}

    return app
