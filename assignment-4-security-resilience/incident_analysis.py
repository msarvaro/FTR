"""
Assignment 4 - Security, Resilience and Intrusion Tolerance
Synthetic Kazakhstan university research server incident

Student: Sarvarov Mustafa (ID 255609, group CSE-2501M)
Instructor: Serek Azamat
Version: v1.0

Run:  python incident_analysis.py
  Part A  rule-based classification of every event (stage, evidence, risk)
  Part B  risk matrix, Risk = Likelihood x Impact
  Part C  replay of the incident against my detection rules -> when would it be stopped
  Part D  recovery timeline vs RPO <= 5 min and RTO <= 30 min
  Part E  anomaly scores, count/rate, priority
Writes everything to output_log.txt. Plain python, no libraries.
"""

import time

EVENTS = [
    ("09:01", "15 failed logins", "10.20.4.15", "Medium"),
    ("09:03", "21 failed logins", "10.20.4.15", "High"),
    ("09:04", "Successful login", "10.20.4.15", "High"),
    ("09:07", "320 files modified", "10.20.4.15", "Critical"),
    ("09:08", "CPU usage increased to 95%", "Research-Server-01", "High"),
    ("09:10", "1,200 files encrypted", "Research-Server-01", "Critical"),
    ("09:12", "Backup deletion attempt", "Research-Server-01", "Critical"),
    ("09:13", "Outbound connection to unknown IP", "Research-Server-01", "High"),
    ("09:15", "Database access spike", "Research-Server-01", "High"),
    ("09:18", "Antivirus disabled", "Research-Server-01", "Critical"),
]

SCORES = [  # event, normal, observed
    ("Login failures", 0.10, 0.81),
    ("File modifications", 0.12, 0.96),
    ("CPU usage", 0.20, 0.88),
    ("Outbound traffic", 0.15, 0.79),
    ("Database requests", 0.18, 0.91),
]
THRESHOLD = 0.75

OUT = []


def out(s=""):
    OUT.append(s)
    print(s)


def section(t):
    out("")
    out("=" * 76)
    out(t)
    out("=" * 76)


def minutes(hhmm):
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def number(text):
    first = text.split()[0].replace(",", "")
    return int(first) if first.isdigit() else 0


# ------------------------------------------------------------------ Part A
def classify(i, ev):
    t, name, src, sev = ev
    low = name.lower()
    prev = EVENTS[i - 1] if i else None
    fails_before = sum(number(e[1]) for e in EVENTS[:i] if "failed login" in e[1].lower())
    if "failed login" in low:
        if prev is None or "failed login" not in prev[1].lower():
            return "reconnaissance", "first burst %s from one IP, attacker probes accounts" % name.split()[0]
        n0 = number(prev[1])
        rate = number(name) / (minutes(t) - minutes(prev[0]))
        return "credential attack", "%d -> %d fails, %.1f/min from same IP = brute force" % (n0, number(name), rate)
    if "successful login" in low:
        return "privilege compromise", "success from %s right after %d fails" % (src, fails_before)
    if "files modified" in low:
        return "data destruction", "%d files changed remotely 3 min after the login" % number(name)
    if "cpu" in low:
        return "lateral movement", "source moves from %s to the server itself, 95%% cpu = payload runs there" % EVENTS[i - 1][2]
    if "encrypted" in low:
        return "data destruction", "%d files encrypted by %s, ~%d files/min since 09:07" % (
            number(name), t, number(name) / (minutes(t) - minutes("09:07")))
    if "backup" in low:
        return "recovery attack", "tries to delete backups 2 min after encryption so we cant restore"
    if "outbound" in low:
        return "exfiltration", "server talks to unknown ip right after encryption = double extortion / c2"
    if "database" in low:
        return "exfiltration", "db spike after outbound channel opened = dumping research data"
    if "antivirus" in low:
        return "persistence", "av off at the end so the implant survives and next run isnt caught"
    return "unknown", ""


def part_a():
    section("PART A - INCIDENT ANALYSIS")
    out("%-6s %-34s %-21s %-8s %s" % ("time", "event", "stage", "risk", "evidence"))
    stages = []
    for i, ev in enumerate(EVENTS):
        stage, why = classify(i, ev)
        stages.append(stage)
        out("%-6s %-34s %-21s %-8s %s" % (ev[0], ev[1], stage, ev[3], why))
    out("")
    out("stages used: %d of 8 -> %s" % (len(set(stages)), ", ".join(sorted(set(stages)))))
    out("first sign 09:01 -> encryption 09:10: %d min window to react" % (minutes("09:10") - minutes("09:01")))
    return stages


# ------------------------------------------------------------------ Part B
RISKS = [  # threat, likelihood, impact, why
    ("Credential compromise", 5, 4, "already happened, no lockout / no mfa"),
    ("Ransomware", 4, 5, "320 modified + 1200 encrypted, research stops"),
    ("Backup destruction", 3, 5, "only an attempt, but success = no recovery at all"),
    ("Data exfiltration", 4, 4, "outbound + db spike seen, personal/research data leak"),
    ("Privilege escalation", 4, 4, "backup delete and av off need admin rights"),
]


def band(r):
    if r <= 4:
        return "Low"
    if r <= 9:
        return "Medium"
    if r <= 16:
        return "High"
    return "Critical"


def part_b():
    section("PART B - RISK MATRIX (Risk = L x I, 1-4 Low, 5-9 Medium, 10-16 High, 17-25 Critical)")
    rows = sorted(RISKS, key=lambda r: -(r[1] * r[2]))
    for th, l, i, why in rows:
        out("%-22s L=%d I=%d risk=%2d %-8s %s" % (th, l, i, l * i, band(l * i), why))


# ------------------------------------------------------------------ Part C (replay)
RULES = [
    ("R1 brute force", lambda e, s: "failed login" in e[1].lower() and s["fails"] >= 20,
     "SIEM: >=20 failed logins from one ip in 5 min"),
    ("R2 login after fails", lambda e, s: "successful login" in e[1].lower() and s["fails"] >= 10,
     "SIEM: success after >=10 fails"),
    ("R3 mass file change", lambda e, s: "files" in e[1].lower() and number(e[1]) >= 100,
     "EDR/FIM: >=100 files changed per minute"),
    ("R4 backup delete", lambda e, s: "backup" in e[1].lower(), "backup api: any delete call"),
    ("R5 egress unknown", lambda e, s: "outbound" in e[1].lower(), "IDS/firewall: egress not in allowlist"),
    ("R6 av tamper", lambda e, s: "antivirus" in e[1].lower(), "EDR: security service stopped"),
]
ISOLATE_MIN = 2   # EDR network isolation + account disable


def files_hit_before(t_min):
    return sum(number(e[1]) for e in EVENTS if ("files" in e[1].lower()) and minutes(e[0]) < t_min)


def part_c():
    section("PART C - REPLAY OF THE INCIDENT AGAINST MY DETECTION RULES")
    state = {"fails": 0}
    fired = []
    for ev in EVENTS:
        if "failed login" in ev[1].lower():
            state["fails"] += number(ev[1])
        for name, cond, desc in RULES:
            if name not in [f[0] for f in fired] and cond(ev, state):
                fired.append((name, ev[0], desc))
                out("%s  %-20s fires on '%s'  (%s)" % (ev[0], name, ev[1], desc))
    total_files = files_hit_before(24 * 60)
    out("")
    out("before (what happened, only host av which got disabled): %d files hit, backup delete tried, data out" % total_files)
    first = fired[0]
    contain = minutes(first[1]) + ISOLATE_MIN
    out("after, detection only (first alert %s %s + %d min isolation -> contained %02d:%02d): %d files hit"
        % (first[1], first[0], ISOLATE_MIN, contain // 60, contain % 60, files_hit_before(contain)))
    r3 = next(f for f in fired if f[0].startswith("R3"))
    c3 = minutes(r3[1]) + ISOLATE_MIN
    out("after, only file rule R3 (%s + %d min -> %02d:%02d): %d files hit"
        % (r3[1], ISOLATE_MIN, c3 // 60, c3 % 60, files_hit_before(c3)))
    out("after, with prevention (MFA + lockout after 5 fails): 09:04 login never succeeds -> 0 files, attack ends at stage 2")


# ------------------------------------------------------------------ Part D
PLAN = [
    ("Detect incident", "SIEM correlation R1/R3 + EDR alert to on-call", 2),
    ("Isolate affected system", "EDR network isolation, firewall rule, disable account", 3),
    ("Preserve evidence", "disk + memory snapshot, logs to WORM storage, sha256", 5),
    ("Activate backup", "last clean immutable snapshot (5 min interval) into recovery env", 5),
    ("Restore services", "rebuild from golden image in recovery env, attach restored data, failover DNS", 8),
    ("Verify integrity", "hash check vs manifest, EDR scan, test logins", 5),
    ("Return to production", "switch traffic, new creds + MFA, extra monitoring 72h", 2),
]
SNAPSHOT_EVERY = 5


def part_d():
    section("PART D - RECOVERY PLAN (RPO <= 5 min, RTO <= 30 min)")
    t = 0
    for i, (step, comp, d) in enumerate(PLAN, 1):
        t += d
        out("%d %-24s %-80s +%-2d min  T+%d" % (i, step, comp, d, t))
    out("RTO = %d min -> %s" % (t, "OK" if t <= 30 else "FAIL"))
    first_bad = minutes("09:07")
    last_clean = (first_bad - 1) // SNAPSHOT_EVERY * SNAPSHOT_EVERY
    out("snapshots every %d min: last clean one %02d:%02d, first damage 09:07 -> data loss %d min, worst case %d min -> RPO %s"
        % (SNAPSHOT_EVERY, last_clean // 60, last_clean % 60, first_bad - last_clean, SNAPSHOT_EVERY,
           "OK" if SNAPSHOT_EVERY <= 5 else "FAIL"))


# ------------------------------------------------------------------ Part E
def part_e():
    section("PART E - AI-ASSISTED DETECTION (anomaly if score >= %.2f)" % THRESHOLD)
    anomalies = []
    for ev, normal, obs in SCORES:
        flag = obs >= THRESHOLD
        dev = obs - normal
        if flag:
            anomalies.append((ev, obs, dev))
        out("%-20s normal=%.2f observed=%.2f deviation=+%.2f x%.1f %s"
            % (ev, normal, obs, dev, obs / normal, "ANOMALY" if flag else "ok"))
    out("")
    out("detected %d of %d -> rate %.0f%%" % (len(anomalies), len(SCORES), len(anomalies) / len(SCORES) * 100))
    top = max(anomalies, key=lambda a: a[1])
    out("highest priority: %s (score %.2f, deviation +%.2f)" % (top[0], top[1], top[2]))
    out("ranking by deviation: " + ", ".join("%s %.2f" % (a[0], a[2]) for a in sorted(anomalies, key=lambda a: -a[2])))
    margin = min(a[1] - THRESHOLD for a in anomalies)
    out("smallest margin over threshold: %.2f (if threshold was %.2f outbound traffic would be missed)" % (margin, 0.80))


if __name__ == "__main__":
    out("incident_analysis.py v1.0  run at %s" % time.strftime("%Y-%m-%d %H:%M:%S"))
    part_a()
    part_b()
    part_c()
    part_d()
    part_e()
    with open("output_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(OUT) + "\n")
    print("\nlog written to output_log.txt")
