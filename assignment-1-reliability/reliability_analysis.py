"""
Assignment 1 - Reliability Engineering and Failure Analysis
Astana IT University | Fault Tolerance | 2026-2027

Student: Sarvarov Mustafa (ID 255609, group CSE-2501M)
Instructor: Serek Azamat

Plain Python, no external libraries. Run:  python3 reliability_analysis.py
All data below is the synthetic dataset given in the assignment.
"""

PERIOD_H = 720  # observation period, hours

# Event: (component, failure time h, repair completed h)
FAILURES = [
    ("Application Server A", 38, 40),
    ("Primary Database", 91, 95),
    ("Campus Network", 143, 144),
    ("Application Server B", 201, 203),
    ("Primary Database", 287, 291),
    ("Load Balancer", 356, 357),
    ("Application Server A", 411, 413),
    ("Campus Network", 478, 480),
    ("Primary Database", 529, 534),
    ("Application Server B", 601, 603),
    ("Application Server A", 654, 655),
    ("Load Balancer", 689, 690),
]

R = {
    "Load Balancer": 0.995,
    "Application Server A": 0.970,
    "Application Server B": 0.970,
    "Primary Database": 0.980,
    "Standby Database": 0.990,
    "Campus Network": 0.995,
}


def part_a():
    """Total downtime, uptime, availability, MTTF, MTTR, MTBF."""
    durations = [end - start for _, start, end in FAILURES]
    n = len(durations)
    downtime = sum(durations)
    uptime = PERIOD_H - downtime
    availability = uptime / PERIOD_H
    mttf = uptime / n          # operating time only, repair time excluded
    mttr = downtime / n
    mtbf = mttf + mttr         # = 720 / 12 = 60 h

    print("=== Part A - Reliability metrics ===")
    print("failures            :", n)
    print("total downtime      :", downtime, "h")
    print("total uptime        :", uptime, "h")
    print("availability        : %.4f (%.2f%%)" % (availability, availability * 100))
    print("MTTF                : %.2f h" % mttf)
    print("MTTR                : %.2f h" % mttr)
    print("MTBF                : %.2f h" % mtbf)

    print("\ndowntime per component:")
    per = {}
    cnt = {}
    for name, start, end in FAILURES:
        per[name] = per.get(name, 0) + (end - start)
        cnt[name] = cnt.get(name, 0) + 1
    for name in sorted(per, key=lambda k: -per[k]):
        print("  %-22s %2d failures, %2d h" % (name, cnt[name], per[name]))
    return availability


def parallel(*rs):
    """Redundant block: works if at least one element works."""
    q = 1.0
    for r in rs:
        q *= (1 - r)
    return 1 - q


def series(*rs):
    """Series block: works only if every element works."""
    r = 1.0
    for x in rs:
        r *= x
    return r


def part_b():
    """Reliability Block Diagram."""
    app = parallel(R["Application Server A"], R["Application Server B"])
    db = parallel(R["Primary Database"], R["Standby Database"])
    lb = R["Load Balancer"]
    net = R["Campus Network"]
    overall = series(lb, app, db, net)

    print("\n=== Part B - RBD ===")
    print("Application (A or B)        : 1 - (1-0.970)*(1-0.970) = %.6f" % app)
    print("Database (Primary or Standby): 1 - (1-0.980)*(1-0.990) = %.6f" % db)
    print("Load Balancer (single)      : %.6f" % lb)
    print("Campus Network (single)     : %.6f" % net)
    print("Overall (series)            : %.4f * %.6f * %.6f * %.4f = %.6f"
          % (lb, app, db, net, overall))
    return overall


# Part C - FMEA: (component, S, O, D)
FMEA = [
    ("Load Balancer", 10, 3, 2),
    ("Application A", 4, 5, 3),
    ("Application B", 4, 4, 3),
    ("Primary Database", 9, 5, 4),
    ("Standby Database", 8, 2, 8),
    ("Campus Network", 9, 3, 3),
]


def part_c():
    print("\n=== Part C - FMEA (RPN = S * O * D) ===")
    rows = [(c, s, o, d, s * o * d) for c, s, o, d in FMEA]
    for c, s, o, d, rpn in sorted(rows, key=lambda r: -r[4]):
        print("  %-20s S=%2d O=%2d D=%2d -> RPN=%3d" % (c, s, o, d, rpn))
    print("  top 3:", ", ".join(r[0] for r in sorted(rows, key=lambda r: -r[4])[:3]))


def part_d(overall):
    """Fault tree - top event probability."""
    app_down = (1 - R["Application Server A"]) * (1 - R["Application Server B"])
    db_down = (1 - R["Primary Database"]) * (1 - R["Standby Database"])
    top = 1 - overall

    print("\n=== Part D - FTA ===")
    print("Application subsystem unavailable (AND): %.6f" % app_down)
    print("Database subsystem unavailable (AND)   : %.6f" % db_down)
    print("TOP EVENT - portal unavailable (OR)    : 1 - %.6f = %.6f (%.2f%%)"
          % (overall, top, top * 100))


if __name__ == "__main__":
    part_a()
    overall = part_b()
    part_c()
    part_d(overall)
