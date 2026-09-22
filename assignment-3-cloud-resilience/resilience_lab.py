"""
Assignment 3 - Cloud Computing and Microservices Resilience
AITU Digital Student Services Platform (synthetic)

Student: Sarvarov Mustafa (ID 255609, group CSE-2501M)
Instructor: Serek Azamat
Version: v1.0

Run:  python3 resilience_lab.py
  1) Part B availability calculation
  2) Part D failure experiments on a small local "cluster":
     - 2 registration replicas (real http processes)
     - db primary + standby with sync replication
     - kubelet-like loop: restartPolicy Always, crash backoff,
       readiness / liveness probes with the values from k8s/registration-service.yaml
     - Service + API gateway: round-robin over READY pods, 1 retry on connection error
     - traffic generator, 20 req/s (scaled down from 500 req/s)
Everything is written to output_log.txt. Only python standard library
(PyYAML is used if installed, otherwise a small fallback parser).
"""

import json
import os
import sys
import threading
import time
import subprocess
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
YAML_PATH = os.path.join(HERE, "k8s", "registration-service.yaml")
DB_ENDPOINT_FILE = os.path.join(HERE, ".db_endpoint")
LOG = []
T0 = time.time()


def log(msg):
    line = "%7.2fs  %s" % (time.time() - T0, msg)
    LOG.append(line)
    print(line, flush=True)


def raw(msg=""):
    LOG.append(msg)
    print(msg, flush=True)


# =====================================================================
# Part B - availability
# =====================================================================
A = {
    "Load Balancer": 0.9995,
    "API Gateway": 0.9990,
    "Auth": 0.9950,
    "Course": 0.9950,
    "Registration": 0.9950,
    "Database": 0.9970,
}


def parallel(*r):
    q = 1.0
    for x in r:
        q *= (1 - x)
    return 1 - q


def availability_report():
    raw("=" * 72)
    raw("PART B - AVAILABILITY")
    raw("=" * 72)
    auth = parallel(A["Auth"], A["Auth"])
    course = parallel(A["Course"], A["Course"])
    reg = parallel(A["Registration"], A["Registration"])
    db = parallel(A["Database"], A["Database"])
    raw("Auth         1-(1-0.995)*(1-0.995) = %.6f" % auth)
    raw("Course       1-(1-0.995)*(1-0.995) = %.6f" % course)
    raw("Registration 1-(1-0.995)*(1-0.995) = %.6f" % reg)
    raw("Database     1-(1-0.997)*(1-0.997) = %.6f" % db)
    e2e = A["Load Balancer"] * A["API Gateway"] * auth * course * reg * db
    raw("End-to-end   0.9995*0.999*%.6f*%.6f*%.6f*%.6f = %.6f (%.3f%%)" % (auth, course, reg, db, e2e, e2e * 100))
    raw("target 99.9%% met: %s" % (e2e >= 0.999))
    raw("downtime/year: %.1f h (allowed at 99.9%%: 8.8 h)" % ((1 - e2e) * 8760))
    raw("downtime/month: %.0f min (allowed: 43 min)" % ((1 - e2e) * 43800))
    raw("")
    raw("what-if (one change at a time):")
    base = e2e
    options = {
        "2nd API Gateway": ("API Gateway",),
        "2nd Load Balancer": ("Load Balancer",),
        "3rd Registration replica": None,
    }
    for name, comp in options.items():
        if comp:
            c = comp[0]
            new = base / A[c] * parallel(A[c], A[c])
        else:
            new = base / reg * parallel(A["Registration"], A["Registration"], A["Registration"])
        raw("  %-26s %.6f  (+%.4f pp)  meets 99.9%%: %s" % (name, new, (new - base) * 100, new >= 0.999))
    both = base / A["API Gateway"] / A["Load Balancer"] * parallel(A["API Gateway"], A["API Gateway"]) * parallel(A["Load Balancer"], A["Load Balancer"])
    raw("  %-26s %.6f  (+%.4f pp)" % ("2nd gateway + 2nd LB", both, (both - base) * 100))
    raw("")


# =====================================================================
# probe config from the YAML
# =====================================================================
def load_config():
    text = open(YAML_PATH, encoding="utf-8").read()
    try:
        import yaml
        docs = [d for d in yaml.safe_load_all(text) if d]
        dep = next(d for d in docs if d.get("kind") == "Deployment")
        c = dep["spec"]["template"]["spec"]["containers"][0]
        return {
            "replicas": dep["spec"]["replicas"],
            "readiness": c["readinessProbe"],
            "liveness": c["livenessProbe"],
            "source": "PyYAML",
        }
    except ImportError:
        # tiny fallback: read "key: number" lines inside each probe block
        cfg = {"readiness": {}, "liveness": {}, "source": "fallback parser"}
        block = None
        for line in text.splitlines():
            s = line.strip()
            if s.startswith("replicas:"):
                cfg["replicas"] = int(s.split(":")[1])
            elif s.startswith("readinessProbe:"):
                block = "readiness"
            elif s.startswith("livenessProbe:"):
                block = "liveness"
            elif block and ":" in s and s.split(":")[1].strip().isdigit():
                cfg[block][s.split(":")[0]] = int(s.split(":")[1])
            elif block and s.startswith(("lifecycle", "- name", "resources")):
                block = None
        return cfg


# =====================================================================
# the "microservices" (run as separate processes)
# =====================================================================
def http(method, url, body=None, timeout=1.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, {}


class Quiet(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def reply(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")


def run_db(port, peer_port):
    rows = {}

    class H(Quiet):
        def do_GET(self):
            self.reply(200, {"rows": len(rows)})

        def do_POST(self):
            b = self.body()
            if self.path == "/write" and peer_port:
                # sync replication to standby before we ack
                try:
                    http("POST", "http://127.0.0.1:%d/replicate" % peer_port, b, timeout=0.5)
                except Exception:
                    pass
            rows[b["req_id"]] = b          # req_id = idempotency key
            self.reply(200, {"rows": len(rows)})

    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()


def run_replica(port):
    def db_url():
        with open(DB_ENDPOINT_FILE) as f:        # "DNS" of the db Service
            return "http://127.0.0.1:%s" % f.read().strip()

    class H(Quiet):
        def do_GET(self):
            if self.path == "/healthz":                # liveness: process only
                return self.reply(200, {"ok": True})
            if self.path == "/ready":                  # readiness: + db
                try:
                    http("GET", db_url() + "/ping", timeout=0.4)
                    return self.reply(200, {"ready": True})
                except Exception:
                    return self.reply(503, {"ready": False})
            self.reply(404, {})

        def do_POST(self):
            b = self.body()
            try:
                code, r = http("POST", db_url() + "/write", b, timeout=0.5)
                self.reply(200, {"pod": port, "rows": r.get("rows")})
            except Exception:
                self.reply(503, {"error": "db unavailable"})

    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()


def spawn(*args):
    return subprocess.Popen([sys.executable, os.path.abspath(__file__)] + [str(a) for a in args],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_port(port, path="/healthz", t=5):
    end = time.time() + t
    while time.time() < end:
        try:
            http("GET", "http://127.0.0.1:%d%s" % (port, path), timeout=0.3)
            return True
        except Exception:
            time.sleep(0.05)
    return False


# =====================================================================
# mini cluster: kubelet + Service + gateway + db failover controller
# =====================================================================
class Pod:
    def __init__(self, name, port):
        self.name, self.port = name, port
        self.proc = None
        self.ready = False
        self.started = 0
        self.restarts = 0
        self.restart_at = None
        self.r_fail = self.l_fail = 0
        self.next_r = self.next_l = 0


class Cluster:
    def __init__(self, cfg):
        self.cfg = cfg
        self.rp, self.lp = cfg["readiness"], cfg["liveness"]
        self.pods = [Pod("registration-%d" % i, 18080 + i) for i in range(cfg["replicas"])]
        self.lock = threading.Lock()
        self.rr = 0
        self.events = []
        self.stop = False
        self.traffic = []           # (t, status)
        self.req_seq = 0
        # db
        self.db_primary, self.db_standby = 15432, 15433
        self.db_procs = {}
        self.db_active = self.db_primary
        self.db_fail = 0
        self.failed_over = False

    # -- events --
    def event(self, what):
        t = time.time()
        self.events.append((t, what))
        log("EVENT " + what)

    # -- db --
    def start_db(self):
        self.db_procs[self.db_standby] = spawn("db", self.db_standby, 0)
        self.db_procs[self.db_primary] = spawn("db", self.db_primary, self.db_standby)
        wait_port(self.db_standby, "/ping")
        wait_port(self.db_primary, "/ping")
        with open(DB_ENDPOINT_FILE, "w") as f:
            f.write(str(self.db_primary))

    def db_controller(self):
        # patroni-like: 1 check per second, 3 misses -> promote standby
        while not self.stop:
            time.sleep(1)
            if self.failed_over:
                continue
            try:
                http("GET", "http://127.0.0.1:%d/ping" % self.db_active, timeout=0.4)
                self.db_fail = 0
            except Exception:
                self.db_fail += 1
                if self.db_fail == 1:
                    self.event("db-controller: primary %d not answering" % self.db_active)
                if self.db_fail >= 3:
                    self.db_active = self.db_standby
                    with open(DB_ENDPOINT_FILE, "w") as f:
                        f.write(str(self.db_standby))
                    self.failed_over = True
                    self.event("db-controller: FAILOVER, standby %d promoted" % self.db_standby)

    # -- pods --
    def start_pod(self, p):
        p.proc = spawn("replica", p.port)
        p.started = time.time()
        p.ready = False
        p.r_fail = p.l_fail = 0
        p.next_r = p.started + self.rp.get("initialDelaySeconds", 0)
        p.next_l = p.started + self.lp.get("initialDelaySeconds", 0)

    def backoff(self, p):
        # CrashLoopBackOff: first restart immediately, then 10s, 20s, 40s ... (max 300s)
        return 0 if p.restarts == 0 else min(10 * 2 ** (p.restarts - 1), 300)

    def probe(self, p, path, timeout):
        try:
            code, _ = http("GET", "http://127.0.0.1:%d%s" % (p.port, path), timeout=timeout)
            return code == 200
        except Exception:
            return False

    def kubelet(self):
        while not self.stop:
            now = time.time()
            for p in self.pods:
                # container exited -> not ready at once, restart (restartPolicy: Always)
                if p.proc and p.proc.poll() is not None and p.restart_at is None:
                    p.ready = False
                    delay = self.backoff(p)
                    p.restart_at = now + delay
                    self.event("kubelet: %s container exited, restart in %ds (restarts=%d)" % (p.name, delay, p.restarts))
                if p.restart_at is not None and now >= p.restart_at:
                    p.restart_at = None
                    p.restarts += 1
                    self.start_pod(p)
                    self.event("kubelet: %s restarted" % p.name)
                if p.restart_at is not None or p.proc.poll() is not None:
                    continue
                if now >= p.next_r:
                    p.next_r = now + self.rp["periodSeconds"]
                    ok = self.probe(p, "/ready", self.rp["timeoutSeconds"])
                    if ok:
                        p.r_fail = 0
                        if not p.ready:
                            p.ready = True
                            self.event("readiness: %s READY -> added to Service endpoints" % p.name)
                    else:
                        p.r_fail += 1
                        if p.ready and p.r_fail >= self.rp["failureThreshold"]:
                            p.ready = False
                            self.event("readiness: %s NOT READY -> removed from endpoints" % p.name)
                if now >= p.next_l:
                    p.next_l = now + self.lp["periodSeconds"]
                    if self.probe(p, "/healthz", self.lp["timeoutSeconds"]):
                        p.l_fail = 0
                    else:
                        p.l_fail += 1
                        if p.l_fail >= self.lp["failureThreshold"]:
                            self.event("liveness: %s failed %d times -> kill" % (p.name, p.l_fail))
                            p.proc.kill()
            time.sleep(0.1)

    # -- Service + API gateway --
    def endpoints(self):
        return [p for p in self.pods if p.ready]

    def gateway(self, body):
        eps = self.endpoints()
        if not eps:
            return 503               # no ready pods -> fast 503, no timeout
        for attempt in range(2):     # 1 retry on another endpoint on connection error
            with self.lock:
                p = eps[self.rr % len(eps)]
                self.rr += 1
            try:
                code, _ = http("POST", "http://127.0.0.1:%d/register" % p.port, body, timeout=1.0)
                return code
            except Exception:
                continue
        return 502

    def load(self, rps=20):
        while not self.stop:
            self.req_seq += 1
            t = time.time()
            code = self.gateway({"req_id": "R%05d" % self.req_seq, "student": "S%04d" % (self.req_seq % 10000)})
            self.traffic.append((t, code))
            time.sleep(max(0, 1.0 / rps - (time.time() - t)))

    # -- helpers for experiments --
    def wait(self, cond, timeout=60):
        end = time.time() + timeout
        while time.time() < end:
            if cond():
                return time.time()
            time.sleep(0.05)
        return None

    def first_event_after(self, t, text):
        for et, w in self.events:
            if et >= t and text in w:
                return et
        return None

    def impact(self, t_from, t_to):
        win = [c for t, c in self.traffic if t_from <= t <= t_to]
        bad = [c for c in win if c != 200]
        return len(win), len(bad)

    def rows(self, port):
        try:
            return http("GET", "http://127.0.0.1:%d/ping" % port, timeout=0.5)[1]["rows"]
        except Exception:
            return None


# =====================================================================
# Part D - experiments
# =====================================================================
def experiments():
    cfg = load_config()
    raw("=" * 72)
    raw("PART D - FAILURE EXPERIMENTS")
    raw("=" * 72)
    raw("config from %s (%s): replicas=%s" % (os.path.relpath(YAML_PATH, HERE), cfg["source"], cfg["replicas"]))
    raw("readiness=%s" % {k: v for k, v in cfg["readiness"].items() if k != "httpGet"})
    raw("liveness =%s" % {k: v for k, v in cfg["liveness"].items() if k != "httpGet"})
    raw("traffic: 20 req/s through gateway -> Service -> registration pods -> db")
    raw("")

    c = Cluster(cfg)
    c.start_db()
    for p in c.pods:
        c.start_pod(p)
    for fn in (c.kubelet, c.db_controller):
        threading.Thread(target=fn, daemon=True).start()
    c.wait(lambda: len(c.endpoints()) == cfg["replicas"], 20)
    threading.Thread(target=c.load, daemon=True).start()
    time.sleep(3)
    results = []

    # ---- 1: one replica ----
    raw("")
    log("--- EXPERIMENT 1: kill one registration replica ---")
    p = c.pods[0]
    t_kill = time.time()
    p.proc.kill()
    log("INJECT kill %s" % p.name)
    t_det = c.wait(lambda: not p.ready, 30)
    t_rec = c.wait(lambda: p.ready, 60)
    time.sleep(1)
    n, bad = c.impact(t_kill, t_rec)
    results.append(("1", "One Registration replica", t_det - t_kill, t_rec - t_kill, n, bad,
                    "other pod served everything"))
    log("RESULT detection=%.1fs recovery=%.1fs requests=%d failed=%d" % (t_det - t_kill, t_rec - t_kill, n, bad))
    time.sleep(3)

    # ---- 2: all replicas ----
    raw("")
    log("--- EXPERIMENT 2: kill ALL registration replicas ---")
    t_kill = time.time()
    for p in c.pods:
        p.proc.kill()
    log("INJECT kill %s" % ", ".join(p.name for p in c.pods))
    t_det = c.wait(lambda: not c.endpoints(), 30)
    t_first = c.wait(lambda: len(c.endpoints()) >= 1, 90)
    t_all = c.wait(lambda: len(c.endpoints()) == cfg["replicas"], 90)
    time.sleep(1)
    n, bad = c.impact(t_kill, t_first)
    results.append(("2", "All Registration replicas", t_det - t_kill, t_first - t_kill, n, bad,
                    "full capacity after %.1fs (backoff)" % (t_all - t_kill)))
    log("RESULT detection=%.1fs service back=%.1fs full capacity=%.1fs requests=%d failed=%d"
        % (t_det - t_kill, t_first - t_kill, t_all - t_kill, n, bad))
    time.sleep(3)

    # ---- 3: database ----
    raw("")
    log("--- EXPERIMENT 3: database primary down ---")
    rows_before = c.rows(c.db_primary)
    t_kill = time.time()
    c.db_procs[c.db_primary].kill()
    log("INJECT kill db primary (%d rows at that moment)" % rows_before)
    t_det = c.first_event_after(t_kill, "not answering") or c.wait(
        lambda: c.first_event_after(t_kill, "not answering") is not None, 30)
    t_fo = c.wait(lambda: c.failed_over, 30)
    # recovered = first successful request after failover
    t_rec = c.wait(lambda: any(t > t_fo and code == 200 for t, code in c.traffic[-40:]), 30)
    time.sleep(2)
    n, bad = c.impact(t_kill, t_rec)
    rows_after = c.rows(c.db_standby)
    lost = rows_before - min(rows_before, rows_after or 0)
    results.append(("3", "Database unavailable", t_det - t_kill, t_rec - t_kill, n, bad,
                    "rows before=%d, on standby now=%d, lost=%d" % (rows_before, rows_after, lost)))
    log("RESULT detection=%.1fs recovery=%.1fs requests=%d failed=%d rows_before=%d standby_rows=%d lost=%d"
        % (t_det - t_kill, t_rec - t_kill, n, bad, rows_before, rows_after, lost))

    c.stop = True
    time.sleep(0.5)
    for p in c.pods:
        if p.proc and p.proc.poll() is None:
            p.proc.kill()
    for pr in c.db_procs.values():
        if pr.poll() is None:
            pr.kill()
    try:
        os.remove(DB_ENDPOINT_FILE)
    except OSError:
        pass

    raw("")
    raw("=" * 72)
    raw("SUMMARY")
    raw("=" * 72)
    raw("%-3s %-27s %-10s %-10s %-9s %-7s %s" % ("#", "failure", "detect", "recover", "requests", "failed", "note"))
    for e, name, det, rec, n, bad, note in results:
        raw("%-3s %-27s %-10s %-10s %-9d %-7s %s" % (e, name, "%.1fs" % det, "%.1fs" % rec, n,
                                                     "%d (%.0f%%)" % (bad, bad / n * 100 if n else 0), note))
    total = len(c.traffic)
    ok = sum(1 for _, code in c.traffic if code == 200)
    raw("")
    raw("whole run: %d requests, %d ok, %.2f%% success" % (total, ok, ok / total * 100))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "db":
        run_db(int(sys.argv[2]), int(sys.argv[3]))
    elif len(sys.argv) > 1 and sys.argv[1] == "replica":
        run_replica(int(sys.argv[2]))
    else:
        raw("resilience_lab.py v1.0  run at %s" % time.strftime("%Y-%m-%d %H:%M:%S"))
        raw("")
        availability_report()
        experiments()
        with open(os.path.join(HERE, "output_log.txt"), "w", encoding="utf-8") as f:
            f.write("\n".join(LOG) + "\n")
        print("\nlog written to output_log.txt")
