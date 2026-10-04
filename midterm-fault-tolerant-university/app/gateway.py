"""API gateway / load balancer

baseline: one upstream per service, no timeouts that matter, no retry, no health checks.
ft:       replicas + active health checks, per-try timeout, retry with exponential backoff,
          circuit breaker per upstream, stale-cache graceful degradation, load shedding.
"""
import asyncio
import json
import os
import random
import time

import httpx
from fastapi import FastAPI, Request, Response

FT = os.getenv("FT_MODE", "ft") == "ft"
UPSTREAMS = json.loads(os.getenv("UPSTREAMS", "{}"))
SENTINEL = os.getenv("SENTINEL_URL", "")
TRY_TIMEOUT = float(os.getenv("TRY_TIMEOUT", "1.0"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))
MAX_INFLIGHT = int(os.getenv("MAX_INFLIGHT", "60"))
DEADLINE = float(os.getenv("DEADLINE", "2.5"))   # whole request budget, below the client timeout
HC_EVERY, HC_FAILS = 1.0, 2
CB_FAILS, CB_OPEN_FOR = 3, 5.0
CACHE_TTL = 300

app = FastAPI(title="api-gateway")
EVENTS = []
CACHE = {}
inflight = {"n": 0}


def event(kind, upstream="", detail=""):
    EVENTS.append({"ts": time.time(), "type": kind, "upstream": upstream, "detail": detail})


class Upstream:
    def __init__(self, url):
        self.url = url
        self.healthy = True
        self.hc_fails = 0
        self.cb_state = "closed"
        self.cb_fails = 0
        self.cb_opened = 0.0

    def available(self):
        if not FT:
            return True
        if not self.healthy:
            return False
        if self.cb_state == "open":
            if time.time() - self.cb_opened >= CB_OPEN_FOR:
                self.cb_state = "half_open"
                event("cb_half_open", self.url)
                return True
            return False
        return True

    def ok(self):
        if self.cb_state != "closed":
            event("cb_closed", self.url)
        self.cb_state, self.cb_fails = "closed", 0

    def fail(self, why):
        self.cb_fails += 1
        if self.cb_state == "half_open" or (self.cb_state == "closed" and self.cb_fails >= CB_FAILS):
            self.cb_state, self.cb_opened = "open", time.time()
            event("cb_open", self.url, why)


POOLS = {svc: [Upstream(u) for u in (urls if FT else urls[:1])] for svc, urls in UPSTREAMS.items()}
RR = {svc: 0 for svc in POOLS}
ROUTE = {"students": "students", "enrollments": "students", "payments": "payments",
         "records": "records", "timetable": "timetable"}
client = httpx.AsyncClient(timeout=httpx.Timeout(TRY_TIMEOUT if FT else 30.0),
                           limits=httpx.Limits(max_connections=400, max_keepalive_connections=100))


async def health_loop():
    while True:
        await asyncio.sleep(HC_EVERY)
        for svc, ups in POOLS.items():
            for u in ups:
                try:
                    r = await client.get(u.url + "/health", timeout=0.5)
                    good = r.status_code == 200
                except Exception:
                    good = False
                if good:
                    u.hc_fails = 0
                    if not u.healthy:
                        u.healthy = True
                        event("health_up", u.url)
                else:
                    u.hc_fails += 1
                    if u.healthy and u.hc_fails >= HC_FAILS:
                        u.healthy = False
                        event("health_down", u.url)


@app.on_event("startup")
async def start():
    if FT:
        asyncio.create_task(health_loop())


def pick(svc, tried):
    ups = POOLS[svc]
    for _ in range(len(ups)):
        RR[svc] = (RR[svc] + 1) % len(ups)
        u = ups[RR[svc]]
        if u not in tried and u.available():
            return u
    return None


@app.get("/health")
def health():
    return {"status": "ok", "mode": "ft" if FT else "baseline"}


@app.get("/admin/state")
def state():
    return {svc: [{"url": u.url, "healthy": u.healthy, "cb": u.cb_state} for u in ups] for svc, ups in POOLS.items()}


@app.get("/admin/events")
async def events(since: float = 0):
    out = [e for e in EVENTS if e["ts"] >= since]
    if SENTINEL:
        try:
            r = await client.get(SENTINEL + "/events", params={"since": since}, timeout=1)
            out += [{**e, "upstream": "db"} for e in r.json()]
        except Exception:
            pass
    return sorted(out, key=lambda e: e["ts"])


@app.post("/admin/fault/{svc}/{idx}")
async def fault(svc: str, idx: int, request: Request):
    """forward a fault-injection command to one specific replica (replicas are not exposed to the host)"""
    url = UPSTREAMS[svc][idx]
    r = await client.post(url + "/admin/fault", content=await request.body(), timeout=2)
    return r.json()


@app.api_route("/{svc}", methods=["GET", "POST"])
async def proxy_root(svc: str, request: Request):
    return await proxy(svc, "", request)


@app.api_route("/{svc}/{path:path}", methods=["GET", "POST"])
async def proxy(svc: str, path: str, request: Request):
    if svc not in ROUTE or ROUTE[svc] not in POOLS:
        return Response(status_code=404)
    if FT and inflight["n"] >= MAX_INFLIGHT:
        # load shedding: fail fast instead of queueing until everything times out
        return Response(json.dumps({"error": "overloaded"}), status_code=503,
                        headers={"Retry-After": "1", "X-Shed": "1"}, media_type="application/json")
    inflight["n"] += 1
    try:
        return await forward(svc, path, request)
    finally:
        inflight["n"] -= 1


async def forward(svc, path, request):
    body = await request.body()
    headers = {k: v for k, v in request.headers.items() if k.lower() in ("content-type", "idempotency-key")}
    target = "/" + svc + ("/" + path if path else "")
    svc = ROUTE[svc]
    retryable = request.method == "GET" or "idempotency-key" in {k.lower() for k in headers}
    attempts = MAX_ATTEMPTS if (FT and retryable) else 1
    tried, last_err, delay, t0 = [], "no upstream", 0.05, time.time()
    for attempt in range(1, attempts + 1):
        if FT and attempt > 1 and time.time() - t0 + TRY_TIMEOUT > DEADLINE:
            last_err = "deadline"
            break
        u = pick(svc, tried) or (pick(svc, []) if FT else POOLS[svc][0])
        if u is None:
            last_err = "all replicas unavailable"
            break
        tried.append(u)
        try:
            r = await client.request(request.method, u.url + target, content=body, headers=headers,
                                     params=dict(request.query_params))
            if r.status_code >= 500:
                raise httpx.HTTPStatusError(f"upstream {r.status_code}", request=r.request, response=r)
            u.ok()
            if request.method == "GET" and r.status_code == 200:
                CACHE[target] = (time.time(), r.content)
            return Response(r.content, status_code=r.status_code, media_type="application/json",
                            headers={"X-Attempts": str(attempt), "X-Upstream": u.url})
        except Exception as e:
            last_err = type(e).__name__
            if FT:
                u.fail(last_err)
            if attempt < attempts:
                await asyncio.sleep(delay + random.random() * delay)   # exponential backoff + jitter
                delay *= 2
    # graceful degradation: serve the last good copy for reads
    if FT and request.method == "GET" and target in CACHE and time.time() - CACHE[target][0] < CACHE_TTL:
        return Response(CACHE[target][1], status_code=200, media_type="application/json",
                        headers={"X-Degraded": "stale-cache", "X-Attempts": str(len(tried))})
    return Response(json.dumps({"error": last_err}), status_code=503 if FT else 502, media_type="application/json")
