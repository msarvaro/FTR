"""two ways to run the system for experiments

DockerBackend  - docker compose (the real deployment)
LocalBackend   - same services as plain processes + two local postgres clusters (linux, postgres 16
                 binaries needed). emulates restart policy, pause (SIGSTOP) and kill.
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def http_ok(url, timeout=1.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def wait_until(fn, timeout=90, every=0.3):
    end = time.time() + timeout
    while time.time() < end:
        if fn():
            return True
        time.sleep(every)
    return False


# ======================================================================= docker
class DockerBackend:
    gateway = "http://localhost:8080"

    def __init__(self):
        self.mode = None

    def _env(self):
        env = dict(os.environ)
        if self.mode == "baseline":
            env.update(FT_MODE="baseline", RESTART="no", SENTINEL_URL="")
        else:
            env.update(FT_MODE="ft", RESTART="unless-stopped")
        return env

    def _dc(self, *args, check=True):
        cmd = ["docker", "compose"] + (["--profile", "ft"] if self.mode == "ft" else []) + list(args)
        return subprocess.run(cmd, cwd=ROOT, env=self._env(), check=check, capture_output=True, text=True)

    def up(self, mode):
        self.mode = mode
        self._dc("--profile", "ft", "down", "-v", "--remove-orphans", check=False)
        self._dc("up", "-d", "--build", "--wait")
        if mode == "ft":   # wait for sync replication to be switched on by the sentinel
            wait_until(lambda: b"sync_replication_on" in self._get("/admin/events"), 60)
        assert wait_until(lambda: http_ok(self.gateway + "/health"), 60)

    def _get(self, path):
        try:
            return urllib.request.urlopen(self.gateway + path, timeout=2).read()
        except Exception:
            return b""

    def down(self):
        self._dc("--profile", "ft", "down", "-v", "--remove-orphans", check=False)

    def kill(self, name):
        self._dc("kill", name, check=False)

    def start(self, name):
        self._dc("start", name, check=False)

    def pause(self, names):
        self._dc("pause", *names, check=False)

    def unpause(self, names):
        self._dc("unpause", *names, check=False)

    def kill_db_primary(self):
        self.kill("db-primary")

    def upstream_url(self, name):
        return "http://%s:8000" % name

    def logs(self):
        return self._dc("logs", "--no-color", "--timestamps", check=False).stdout


# ======================================================================= local
SERVICES = {   # name: (module, port, extra env)
    "student-1": ("student", 18001, {"SERVICE_NAME": "student", "INSTANCE": "student-1"}),
    "student-2": ("student", 18002, {"SERVICE_NAME": "student", "INSTANCE": "student-2"}),
    "payment-1": ("payment", 18011, {"SERVICE_NAME": "payment", "INSTANCE": "payment-1"}),
    "payment-2": ("payment", 18012, {"SERVICE_NAME": "payment", "INSTANCE": "payment-2"}),
    "records": ("records", 18021, {"SERVICE_NAME": "records", "INSTANCE": "records-1"}),
    "timetable": ("timetable", 18031, {"SERVICE_NAME": "timetable", "INSTANCE": "timetable-1"}),
    "db-sentinel": ("sentinel", 18041, {}),
    "gateway": ("gateway", 8080, {}),
}
FT_ONLY = {"student-2", "payment-2", "db-sentinel"}
PG_PORTS = {"db-primary": 15432, "db-replica": 15433}


class LocalBackend:
    gateway = "http://127.0.0.1:8080"

    def __init__(self, workdir="/tmp/ftlab"):
        self.work = workdir
        self.procs, self.stopped, self.mode = {}, set(), None
        self.pg_bin = os.getenv("PG_BIN") or sorted(
            d + "/bin" for d in (os.path.join("/usr/lib/postgresql", v) for v in os.listdir("/usr/lib/postgresql")))[-1]
        self.as_pg = ["runuser", "-u", "postgres", "--"] if os.geteuid() == 0 else []
        self.log = open(os.path.join(ROOT, "results", "local_services.log"), "a")
        self._sup = None

    # ---------------- postgres
    def _pg(self, tool, *args, check=True):
        return subprocess.run(self.as_pg + [os.path.join(self.pg_bin, tool)] + list(args),
                              check=check, capture_output=True, text=True)

    def _pg_conf(self, data, port):
        with open(os.path.join(data, "postgresql.auto.conf"), "a") as f:
            f.write(f"\nport = {port}\nlisten_addresses = '127.0.0.1'\nunix_socket_directories = '{self.work}'\n"
                    "wal_level = replica\nmax_wal_senders = 10\nmax_connections = 200\nhot_standby = on\n"
                    "fsync = on\n")

    def _start_pg(self, name):
        data = os.path.join(self.work, name)
        self._pg("pg_ctl", "-D", data, "-l", data + ".log", "-w", "start")

    def _psql(self, port, sql, db="university"):
        return self._pg("psql", "-h", "127.0.0.1", "-p", str(port), "-U", "uni", "-d", db, "-v", "ON_ERROR_STOP=1",
                        "-c", sql)

    def _init_db(self):
        if os.path.exists(self.work):
            self._stop_all_pg()
            shutil.rmtree(self.work)
        os.makedirs(self.work)
        if self.as_pg:
            shutil.chown(self.work, "postgres", "postgres")
        prim = os.path.join(self.work, "db-primary")
        self._pg("initdb", "-D", prim, "-U", "uni", "--auth=trust", "--no-sync")
        self._pg_conf(prim, PG_PORTS["db-primary"])
        with open(os.path.join(prim, "pg_hba.conf"), "a") as f:
            f.write("\nhost replication all 127.0.0.1/32 trust\n")
        self._start_pg("db-primary")
        self._pg("createdb", "-h", "127.0.0.1", "-p", str(PG_PORTS["db-primary"]), "-U", "uni", "university")
        self._pg("psql", "-h", "127.0.0.1", "-p", str(PG_PORTS["db-primary"]), "-U", "uni", "-d", "university",
                 "-v", "ON_ERROR_STOP=1", "-f", os.path.join(ROOT, "app", "schema.sql"))
        if self.mode == "ft":
            rep = os.path.join(self.work, "db-replica")
            self._pg("pg_basebackup", "-d", f"host=127.0.0.1 port={PG_PORTS['db-primary']} user=uni "
                     "application_name=replica1", "-D", rep, "-X", "stream", "-R", "-c", "fast")
            self._pg_conf(rep, PG_PORTS["db-replica"])
            self._start_pg("db-replica")

    def _stop_all_pg(self):
        for name in PG_PORTS:
            data = os.path.join(self.work, name)
            if os.path.exists(os.path.join(data, "postmaster.pid")):
                self._pg("pg_ctl", "-D", data, "-m", "immediate", "stop", check=False)

    # ---------------- services
    def _env(self, name):
        mod, port, extra = SERVICES[name]
        env = dict(os.environ, FT_MODE=self.mode, DB_HOSTS="127.0.0.1,127.0.0.1",
                   DB_PORTS=f"{PG_PORTS['db-primary']},{PG_PORTS['db-replica']}", DB_USER="uni", DB_PASS="x",
                   DB_NAME="university", PRIMARY_HOST="127.0.0.1", PRIMARY_PORT=str(PG_PORTS["db-primary"]),
                   STANDBY_HOST="127.0.0.1", STANDBY_PORT=str(PG_PORTS["db-replica"]), PYTHONUNBUFFERED="1", **extra)
        if name == "gateway":
            u = lambda n: "http://127.0.0.1:%d" % SERVICES[n][1]
            env["UPSTREAMS"] = json.dumps({"students": [u("student-1"), u("student-2")],
                                           "payments": [u("payment-1"), u("payment-2")],
                                           "records": [u("records")], "timetable": [u("timetable")]})
            env["SENTINEL_URL"] = u("db-sentinel") if self.mode == "ft" else ""
        return env

    def _spawn(self, name):
        mod, port, _ = SERVICES[name]
        self.procs[name] = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", f"{mod}:app", "--host", "127.0.0.1", "--port", str(port),
             "--log-level", "warning"], cwd=os.path.join(ROOT, "app"), env=self._env(name),
            stdout=self.log, stderr=subprocess.STDOUT)

    def _supervisor(self):
        # restart: unless-stopped (ft only). small delay like a container restart
        while self.mode == "ft" and self._sup_on:
            for name, p in list(self.procs.items()):
                if p.poll() is not None and name not in self.stopped:
                    self.log.write(f"{time.time():.3f} supervisor: {name} exited ({p.returncode}), restarting\n")
                    self.log.flush()
                    time.sleep(0.5)
                    self._spawn(name)
            time.sleep(0.2)

    def up(self, mode):
        self.down()
        self.mode, self.stopped = mode, set()
        self._init_db()
        names = [n for n in SERVICES if mode == "ft" or n not in FT_ONLY]
        for n in names:
            if n != "gateway":
                self._spawn(n)
        for n in names:
            if n != "gateway":
                assert wait_until(lambda: http_ok("http://127.0.0.1:%d/health" % SERVICES[n][1]), 30), n
        self._spawn("gateway")
        assert wait_until(lambda: http_ok(self.gateway + "/health"), 30)
        if mode == "ft":
            assert wait_until(lambda: b'"sync":true' in urllib.request.urlopen(
                "http://127.0.0.1:18041/health", timeout=2).read(), 30)
            self._sup_on = True
            self._sup = threading.Thread(target=self._supervisor, daemon=True)
            self._sup.start()

    def down(self):
        self._sup_on = False
        for p in self.procs.values():
            if p.poll() is None:
                try:
                    os.kill(p.pid, signal.SIGCONT)
                except OSError:
                    pass
                p.kill()
                p.wait()
        self.procs = {}
        if os.path.exists(self.work):
            self._stop_all_pg()

    def kill(self, name):
        if name in PG_PORTS:
            return self.kill_db_primary()
        self.stopped.add(name)
        self.procs[name].kill()

    def start(self, name):
        if name in PG_PORTS:
            return self._start_pg(name)
        self.stopped.discard(name)
        if self.procs[name].poll() is not None:
            self._spawn(name)
            wait_until(lambda: http_ok("http://127.0.0.1:%d/health" % SERVICES[name][1]), 20)

    def pause(self, names):
        for n in names:
            os.kill(self.procs[n].pid, signal.SIGSTOP)

    def unpause(self, names):
        for n in names:
            os.kill(self.procs[n].pid, signal.SIGCONT)

    def kill_db_primary(self):
        pidfile = os.path.join(self.work, "db-primary", "postmaster.pid")
        pid = int(open(pidfile).readline())
        subprocess.run(["pkill", "-9", "-P", str(pid)], capture_output=True)   # all backends at once
        os.kill(pid, signal.SIGKILL)                                            # and the postmaster

    def upstream_url(self, name):
        return "http://127.0.0.1:%d" % SERVICES[name][1]

    def logs(self):
        self.log.flush()
        return open(os.path.join(ROOT, "results", "local_services.log")).read()
