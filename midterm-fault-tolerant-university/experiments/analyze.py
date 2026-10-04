"""reliability analysis from results/results.json -> results/summary.txt + summary.json

theory:  component availabilities (assumed, typical cloud numbers) -> series / parallel model
measured: every experiment window = observation time, every user-visible outage = one failure
"""
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
A = {"gateway": 0.999, "service_instance": 0.995, "db_node": 0.997}


def par(a, n=2):
    return 1 - (1 - a) ** n


def theory():
    s, g, d = A["service_instance"], A["gateway"], A["db_node"]
    base = g * s ** 4 * d                                   # gateway, 4 services x1, 1 db, all in series
    ft = g * par(s) * par(s) * s * 1.0 * par(d)             # students x2, payments x2, records x1,
    #                                                         timetable masked by stale cache, db pair
    return base, ft


def measured(runs):
    out = {}
    for mode, exps in runs.items():
        T = sum(e["stats"]["window_s"] for e in exps.values())
        n = sum(1 for e in exps.values() if e["stats"]["failed"] > 0)
        D = sum(e["stats"]["outage_s"] for e in exps.values())
        req = sum(e["stats"]["requests"] for e in exps.values())
        ok = sum(e["stats"]["ok"] for e in exps.values())
        failed = req - ok
        recovered = sum(e["stats"]["masked_by_gateway_retry"] + e["stats"]["recovered_by_client_retry"]
                        + e["stats"]["degraded"] for e in exps.values())
        mttr = D / n if n else 0.0
        mttf = (T - D) / n if n else float("inf")
        rec = [e["stats"]["service_recovery_s"] for e in exps.values() if e["stats"]["service_recovery_s"] is not None]
        det = [e["detection"][0] for e in exps.values() if e["detection"][0] is not None]
        out[mode] = {
            "observed_time_s": round(T, 1), "failures": n, "downtime_s": round(D, 1),
            "MTTR_s": round(mttr, 1), "MTTF_s": round(mttf, 1) if n else None,
            "MTBF_s": round(mttf + mttr, 1) if n else None,
            "availability_time": round((T - D) / T * 100, 2),
            "availability_requests": round(ok / req * 100, 2),
            "failure_rate_per_hour": round(n / T * 3600, 1),
            "requests": req, "failed_requests": failed, "recovered_requests": recovered,
            "mean_detection_s": round(sum(det) / len(det), 2) if det else None,
            "experiments_not_recovered": sum(1 for e in exps.values() if e["stats"]["not_recovered"]),
        }
    return out


def main():
    data = json.load(open(os.path.join(ROOT, "results", "results.json")))
    tb, tf = theory()
    m = measured(data["runs"])
    lines = ["RELIABILITY SUMMARY (backend=%s, run %s)" % (data["backend"], data["started"]), "",
             "theory (assumed: gateway %.3f, service instance %.3f, db node %.3f)" % (A["gateway"], A["service_instance"], A["db_node"]),
             "  baseline  g*s^4*d                 = %.5f  (%.3f%%, %.1f h down/year)" % (tb, tb * 100, (1 - tb) * 8760),
             "  ft        g*(1-(1-s)^2)^2*s*(1-(1-d)^2) = %.5f  (%.3f%%, %.1f h down/year)" % (tf, tf * 100, (1 - tf) * 8760),
             ""]
    keys = ["observed_time_s", "failures", "downtime_s", "MTTF_s", "MTTR_s", "MTBF_s", "availability_time",
            "availability_requests", "failure_rate_per_hour", "requests", "failed_requests", "recovered_requests",
            "mean_detection_s", "experiments_not_recovered"]
    lines.append("%-26s %14s %14s" % ("measured", "baseline", "ft"))
    for k in keys:
        lines.append("%-26s %14s %14s" % (k, m.get("baseline", {}).get(k), m.get("ft", {}).get(k)))
    lines += ["", "per experiment (failed requests / outage s / detection s):"]
    for eid in data["runs"].get("baseline", {}):
        b = data["runs"]["baseline"][eid]; f = data["runs"].get("ft", {}).get(eid)
        fmt = lambda e: "%5d / %5.1f / %s" % (e["stats"]["failed"], e["stats"]["outage_s"], e["detection"][0])
        lines.append("  %s %-40s base %s   ft %s" % (eid, b["failure"], fmt(b), fmt(f) if f else "-"))
    txt = "\n".join(lines)
    print(txt)
    open(os.path.join(ROOT, "results", "summary.txt"), "w").write(txt + "\n")
    json.dump({"theory": {"baseline": tb, "ft": tf, "assumptions": A}, "measured": m},
              open(os.path.join(ROOT, "results", "summary.json"), "w"), indent=1)


if __name__ == "__main__":
    main()
