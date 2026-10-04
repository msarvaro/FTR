"""failure-injection experiments: baseline vs fault-tolerant

  python experiments/run_experiments.py --backend docker      (docker compose, the real deployment)
  python experiments/run_experiments.py --backend local       (processes + local postgres, linux)

writes results/results.json and results/run_log.txt. only python standard library.
"""
import argparse
import json
import os
import random
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backends import ROOT, DockerBackend, LocalBackend, wait_until, http_ok  # noqa: E402

CLIENT_TIMEOUT = 3.0
LOG = []
T_START = time.time()


def log(msg=""):
    line = msg if not msg or msg.startswith("=") else "%7.1fs  %s" % (time.time() - T_START, msg)
    LOG.append(line)
    print(line, flush=True)


def call(gw, method, path, body=None, key=None, timeout=CLIENT_TIMEOUT):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    req = urllib.request.Request(gw + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}"), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, {}, dict(e.headers or {})
    except Exception:
        return 0, {}, {}


# ----------------------------------------------------------------------------- load generator
class Load:
    def __init__(self, gw, mix, workers, pace, client_retry=False, tag="r"):
        self.gw, self.mix, self.workers, self.pace = gw, mix, workers, pace
        self.client_retry, self.tag = client_retry, tag
        self.records, self.stop, self.seq, self.lock = [], False, 0, threading.Lock()
        self.kinds = [k for k, w in mix for _ in range(int(w * 10))]

    def one(self, kind, ref):
        sid = "S%04d" % random.randint(1, 500)
        if kind == "get_student":
            return call(self.gw, "GET", "/students/" + sid)
        if kind == "timetable":
            return call(self.gw, "GET", "/timetable/CSE-%d" % random.randint(1, 8))
        if kind == "enroll":
            return call(self.gw, "POST", "/enrollments", {"student_id": sid, "course": "FT-2026"}, key=ref)
        if kind == "pay":
            return call(self.gw, "POST", "/payments", {"student_id": sid, "amount": 1000}, key=ref)

    def worker(self):
        while not self.stop:
            with self.lock:
                self.seq += 1
                ref = "%s-%06d" % (self.tag, self.seq)
            kind = random.choice(self.kinds)
            t0 = time.time()
            status, body, h = self.one(kind, ref)
            client_retries = 0
            if status in (0, 502, 503) and h.get("X-Shed"):          # polite client: back off when shed
                time.sleep(0.3)
                status, body, h = self.one(kind, ref)
                client_retries = 1
            elif status != 200 and self.client_retry:                 # student presses "pay" again, same key
                time.sleep(1.0)
                status, body, h = self.one(kind, ref)
                client_retries = 1
            self.records.append({"t0": t0, "t1": time.time(), "kind": kind, "ref": ref, "status": status,
                                 "attempts": int(h.get("X-Attempts", h.get("x-attempts", 1)) or 1),
                                 "degraded": bool(h.get("X-Degraded") or h.get("x-degraded")),
                                 "client_retries": client_retries})
            if self.pace:
                time.sleep(self.pace)

    def __enter__(self):
        self.threads = [threading.Thread(target=self.worker, daemon=True) for _ in range(self.workers)]
        for t in self.threads:
            t.start()
        return self

    def __exit__(self, *a):
        self.stop = True
        for t in self.threads:
            t.join(timeout=CLIENT_TIMEOUT * 3)


# ----------------------------------------------------------------------------- metrics
def stats(records, inject, end):
    """counts are for requests started after the injection. outage = first failure -> last failure
    (or -> end of window when the service never came back)"""
    win = [r for r in records if r["t0"] >= inject]
    ok = [r for r in win if r["status"] == 200]
    bad = sorted([r for r in win if r["status"] != 200], key=lambda r: r["t1"])
    lat = sorted(r["t1"] - r["t0"] for r in ok) or [0]
    buckets = {}
    for r in records:
        if r["t0"] >= inject - 3:
            b = int(r["t0"] - inject) if r["t0"] >= inject else -1 - int(inject - r["t0"])
            buckets.setdefault(b, [0, 0])[0 if r["status"] == 200 else 1] += 1
    first_fail = bad[0]["t1"] - inject if bad else None
    last_fail = bad[-1]["t1"] - inject if bad else None
    not_rec = bool(bad) and bad[-1]["t1"] >= end - 2.5
    outage = 0.0 if not bad else ((end - inject) if not_rec else last_fail) - (bad[0]["t0"] - inject)
    return {
        "requests": len(win), "ok": len(ok), "failed": len(bad),
        "success_rate": round(len(ok) / len(win) * 100, 2) if win else 0,
        "degraded": sum(r["degraded"] for r in ok),
        "masked_by_gateway_retry": sum(1 for r in ok if r["attempts"] > 1),
        "recovered_by_client_retry": sum(1 for r in ok if r["client_retries"]),
        "p50_ms": round(lat[len(lat) // 2] * 1000), "p95_ms": round(lat[int(len(lat) * 0.95)] * 1000),
        "max_ms": round(lat[-1] * 1000),
        "first_failure_s": None if first_fail is None else round(first_fail, 2),
        "service_recovery_s": None if (not bad or not_rec) else round(last_fail, 2),
        "not_recovered": not_rec,
        "window_s": round(end - inject, 1),
        "outage_s": round(outage, 2),
        "timeline": {str(k): v for k, v in sorted(buckets.items())},
    }


def events_since(gw, since):
    status, body, _ = call(gw, "GET", "/admin/events?since=%f" % since, timeout=3)
    return body if isinstance(body, list) else []


def detection(gw, inject, types, match, st):
    """ft: first monitoring event (health check / circuit breaker / sentinel) after the injection.
    baseline has no monitoring -> detection = first request a user sees failing"""
    for e in events_since(gw, inject):
        if e["type"] in types and match in (e.get("upstream", "") + e.get("detail", "")):
            return round(e["ts"] - inject, 2), e["type"]
    if st["first_failure_s"] is not None:
        return st["first_failure_s"], "user-visible failure"
    return None, "-"


def acked_lost(gw, records, kind):
    acked = [r["ref"] for r in records if r["kind"] == kind and r["status"] == 200]
    found = set()
    for i in range(0, len(acked), 200):
        _, body, _ = call(gw, "POST", "/enrollments/check", {"refs": acked[i:i + 200]}, timeout=10)
        found |= set(body.get("found", []))
    _, audit, _ = call(gw, "GET", "/enrollments/audit", timeout=10)
    return {"acked_writes": len(acked), "acked_but_lost": len(set(acked) - found),
            "duplicate_rows": audit.get("duplicates")}


# ----------------------------------------------------------------------------- experiments
def exp_app_crash(be, mode):
    gw = be.gateway
    with Load(gw, [("get_student", .7), ("enroll", .3)], workers=6, pace=0.1, tag="e1") as load:
        time.sleep(4)
        inject = time.time()
        log("inject: crash student-1 (process exits with code 1)")
        call(gw, "POST", "/admin/fault/students/0", {"crash_now": True}, timeout=2)
        time.sleep(20)
    end = time.time()
    st = stats(load.records, inject, end)
    det = detection(gw, inject, ("health_down", "cb_open"), be.upstream_url("student-1"), st)
    comp = next((round(e["ts"] - inject, 2) for e in events_since(gw, inject)
                 if e["type"] == "health_up" and be.upstream_url("student-1") in e["upstream"]), None)
    if mode == "baseline":
        log("operator restarts student-1 (manual repair) to check the data")
        be.start("student-1")
        wait_until(lambda: http_ok(gw + "/students/S0001"), 30)
    cons = acked_lost(gw, load.records, "enroll")
    return {"failure": "Application crash (student-1)", "stats": st, "detection": det,
            "component_back_s": comp, "consistency": cons}


def exp_db_failure(be, mode):
    gw = be.gateway
    with Load(gw, [("enroll", .6), ("get_student", .4)], workers=6, pace=0.1, tag="e2") as load:
        time.sleep(4)
        inject = time.time()
        log("inject: kill db-primary (SIGKILL, stays down)")
        be.kill_db_primary()
        time.sleep(22)
    end = time.time()
    st = stats(load.records, inject, end)
    det = detection(gw, inject, ("primary_unreachable",), "", st)
    fo = next((round(e["ts"] - inject, 2) for e in events_since(gw, inject) if e["type"] == "failover_promoted"), None)
    if mode == "baseline":
        log("operator restarts db-primary (manual repair) to check the data")
        be.start("db-primary")
        wait_until(lambda: http_ok(gw + "/students/S0001"), 60)
    cons = acked_lost(gw, load.records, "enroll")
    return {"failure": "Database failure (primary killed)", "stats": st, "detection": det,
            "failover_s": fo, "consistency": cons}


def exp_timeout(be, mode):
    gw = be.gateway
    with Load(gw, [("get_student", 1.0)], workers=6, pace=0.1, tag="e3") as load:
        time.sleep(4)
        inject = time.time()
        log("inject: +3000 ms latency on student-1 (slow dependency)")
        call(gw, "POST", "/admin/fault/students/0", {"latency_ms": 3000}, timeout=2)
        time.sleep(16)
    end = time.time()
    call(gw, "POST", "/admin/fault/students/0", {"latency_ms": 0}, timeout=5)
    st = stats(load.records, inject, end)
    det = detection(gw, inject, ("cb_open", "health_down"), be.upstream_url("student-1"), st)
    return {"failure": "Service timeout (+3 s latency)", "stats": st, "detection": det,
            "consistency": {"note": "read-only load, nothing to corrupt"}}


def exp_node_failure(be, mode):
    gw = be.gateway
    node = ["student-1", "payment-1", "timetable"]
    with Load(gw, [("get_student", .4), ("pay", .2), ("timetable", .4)], workers=6, pace=0.1, tag="e4") as load:
        time.sleep(5)
        inject = time.time()
        log("inject: node A frozen (pause %s) for 15 s" % ", ".join(node))
        be.pause(node)
        time.sleep(15)
        be.unpause(node)
        log("node A back")
        time.sleep(8)
    end = time.time()
    st = stats(load.records, inject, end)
    det = detection(gw, inject, ("health_down", "cb_open"), "", st)
    _, cons, _ = call(gw, "GET", "/payments/consistency", timeout=10)
    return {"failure": "Node failure (node A frozen 15 s)", "stats": st, "detection": det,
            "consistency": cons}


def exp_lost_transaction(be, mode):
    gw = be.gateway
    with Load(gw, [("pay", 1.0)], workers=3, pace=0.2, client_retry=True, tag="e5") as load:
        time.sleep(3)
        inject = time.time()
        log("inject: payment-1 dies in the middle of the next payment")
        call(gw, "POST", "/admin/fault/payments/0", {"crash_on_next_payment": True}, timeout=2)
        time.sleep(14)
    end = time.time()
    st = stats(load.records, inject, end)
    det = detection(gw, inject, ("health_down", "cb_open"), be.upstream_url("payment-1"), st)
    if mode == "baseline":
        log("operator restarts payment-1 to check the data")
        be.start("payment-1")
        wait_until(lambda: call(gw, "GET", "/payments/consistency")[0] == 200, 30)
    _, cons, _ = call(gw, "GET", "/payments/consistency", timeout=10)

    # academic records: transcript batch interrupted half way
    log("inject: records service dies after 150 of 300 transcripts")
    call(gw, "POST", "/admin/fault/records/0", {"crash_after_items": 150}, timeout=2)
    t0 = time.time()
    call(gw, "POST", "/records/batches", {"batch_id": "B-%s" % mode, "size": 300}, timeout=5)

    def done():
        s, b, _ = call(gw, "GET", "/records/batches/B-%s" % mode, timeout=2)
        return s == 200 and b.get("status") == "done"
    if mode == "baseline":
        time.sleep(8)
        log("operator notices, restarts records and submits the batch again")
        be.start("records")
        call(gw, "POST", "/records/batches", {"batch_id": "B-%s" % mode, "size": 300}, timeout=5)
    wait_until(done, 60)
    _, batch, _ = call(gw, "GET", "/records/batches/B-%s" % mode, timeout=5)
    batch["completed_after_s"] = round(time.time() - t0, 1)
    return {"failure": "Interrupted payment + transcript batch", "stats": st, "detection": det,
            "consistency": cons, "batch": batch}


def exp_high_load(be, mode):
    gw = be.gateway
    with Load(gw, [("get_student", .7), ("timetable", .3)], workers=80, pace=0, tag="e6") as load:
        inject = time.time()
        log("inject: 80 concurrent clients without pause for 15 s")
        time.sleep(15)
    end = time.time()
    st = stats(load.records, inject, end)
    st["throughput_ok_per_s"] = round(st["ok"] / (end - inject), 1)
    st["rejected_fast_503"] = sum(1 for r in load.records if r["status"] in (502, 503) and r["t1"] - r["t0"] < 0.2)
    st["client_timeouts"] = sum(1 for r in load.records if r["status"] == 0)
    det = detection(gw, inject, ("cb_open", "health_down"), "", st)
    return {"failure": "High load (80 concurrent clients)", "stats": st, "detection": det,
            "consistency": {"note": "read load"}}


EXPERIMENTS = [("E1", exp_app_crash), ("E2", exp_db_failure), ("E3", exp_timeout),
               ("E4", exp_node_failure), ("E5", exp_lost_transaction), ("E6", exp_high_load)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", choices=["docker", "local"], default="docker")
    ap.add_argument("--modes", default="baseline,ft")
    ap.add_argument("--only", default="")
    args = ap.parse_args()
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    be = DockerBackend() if args.backend == "docker" else LocalBackend()
    log("=" * 78)
    log("run started %s, backend=%s" % (time.strftime("%Y-%m-%d %H:%M:%S"), args.backend))
    out = {"backend": args.backend, "started": time.strftime("%Y-%m-%d %H:%M:%S"), "runs": {}}
    try:
        for mode in args.modes.split(","):
            out["runs"][mode] = {}
            for eid, fn in EXPERIMENTS:
                if args.only and eid not in args.only.split(","):
                    continue
                log("=" * 78)
                log("%s %s - %s" % (mode.upper(), eid, fn.__name__))
                be.up(mode)
                t = time.time()
                res = fn(be, mode)
                res["duration_s"] = round(time.time() - t, 1)
                out["runs"][mode][eid] = res
                s = res["stats"]
                log("result: requests=%d ok=%d failed=%d (%.1f%% ok) degraded=%d masked_by_retry=%d detection=%s "
                    "recovery=%s outage=%.1fs p95=%dms"
                    % (s["requests"], s["ok"], s["failed"], s["success_rate"], s["degraded"],
                       s["masked_by_gateway_retry"] + s["recovered_by_client_retry"], res["detection"],
                       "not recovered" if s["not_recovered"] else s["service_recovery_s"], s["outage_s"], s["p95_ms"]))
                extra = {k: res[k] for k in ("failover_s", "component_back_s") if k in res}
                extra.update({k: s[k] for k in ("throughput_ok_per_s", "rejected_fast_503", "client_timeouts") if k in s})
                if extra:
                    log("extra: %s" % json.dumps(extra))
                log("consistency: %s" % json.dumps(res.get("consistency")))
                if "batch" in res:
                    log("batch: %s" % json.dumps(res["batch"]))
                with open(os.path.join(ROOT, "results", "results.json"), "w") as f:
                    json.dump(out, f, indent=1)
    finally:
        with open(os.path.join(ROOT, "results", "service_logs.txt"), "w") as f:
            f.write(be.logs() or "")
        be.down()
        with open(os.path.join(ROOT, "results", "run_log.txt"), "w") as f:
            f.write("\n".join(LOG) + "\n")


if __name__ == "__main__":
    main()
