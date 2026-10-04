"""DB sentinel: watches the primary, promotes the standby after 3 missed checks (ft only)"""
import os
import threading
import time

import psycopg
from fastapi import FastAPI

FT = os.getenv("FT_MODE", "ft") == "ft"
PRIMARY = os.getenv("PRIMARY_HOST", "db-primary"), os.getenv("PRIMARY_PORT", "5432")
STANDBY = os.getenv("STANDBY_HOST", "db-replica"), os.getenv("STANDBY_PORT", "5432")
USER, PASS, DB = os.getenv("DB_USER", "uni"), os.getenv("DB_PASS", "uni_pass"), os.getenv("DB_NAME", "university")
CHECK_EVERY, MISSES = 1.0, 3

app = FastAPI(title="db-sentinel")
EVENTS = []
STATE = {"primary": "%s:%s" % PRIMARY, "failed_over": False, "misses": 0, "sync": False}


def event(kind, detail=""):
    EVENTS.append({"ts": time.time(), "type": kind, "detail": detail})
    print(f"[sentinel] {kind} {detail}", flush=True)


def conn(host_port, timeout=1):
    h, p = host_port
    return psycopg.connect(f"host={h} port={p} user={USER} password={PASS} dbname={DB} connect_timeout={timeout}",
                           autocommit=True)


def enable_sync_replication():
    """turn on synchronous commit once the standby is streaming -> acked writes exist on 2 nodes (RPO 0).
    done here and not at initdb, otherwise the primary would block before any standby exists"""
    while FT:
        try:
            with conn(PRIMARY) as c:
                rows = c.execute("SELECT application_name, state FROM pg_stat_replication").fetchall()
                if any(r[0] == "replica1" and r[1] == "streaming" for r in rows):
                    c.execute("ALTER SYSTEM SET synchronous_standby_names = 'replica1'")
                    c.execute("SELECT pg_reload_conf()")
                    STATE["sync"] = True
                    event("sync_replication_on", "replica1 streaming")
                    return
        except Exception:
            pass
        time.sleep(1)


def loop():
    enable_sync_replication()
    while True:
        time.sleep(CHECK_EVERY)
        if not FT or STATE["failed_over"]:
            continue
        try:
            with conn(PRIMARY) as c:
                c.execute("SELECT 1")
            STATE["misses"] = 0
        except Exception as e:
            STATE["misses"] += 1
            if STATE["misses"] == 1:
                event("primary_unreachable", str(e).splitlines()[0][:80])
            if STATE["misses"] >= MISSES:
                try:
                    with conn(STANDBY, timeout=2) as c:
                        c.execute("SELECT pg_promote(wait => true, wait_seconds => 10)")
                    STATE["failed_over"] = True
                    STATE["primary"] = "%s:%s" % STANDBY
                    event("failover_promoted", STATE["primary"])
                except Exception as e2:
                    event("promote_failed", str(e2).splitlines()[0][:80])


threading.Thread(target=loop, daemon=True).start()


@app.get("/health")
def health():
    return {"status": "ok", **STATE}


@app.get("/events")
def events(since: float = 0):
    return [e for e in EVENTS if e["ts"] >= since]
