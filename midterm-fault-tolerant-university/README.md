# Fault-Tolerant University Information System (midterm)

Sarvarov Mustafa · 255609 · CSE-2501M · Fault Tolerance and Dependable Computing

Services (Python + FastAPI): API gateway/LB, student (x2), payment (x2), records, timetable,
PostgreSQL 16 primary + hot standby (sync replication), db-sentinel (automatic failover).
`FT_MODE=baseline` turns every fault-tolerance mechanism off -> same code, two versions.

## run
```
# fault-tolerant
docker compose --profile ft up -d --build
# baseline
FT_MODE=baseline RESTART=no SENTINEL_URL= docker compose up -d --build
# gateway: http://localhost:8080  (e.g. /students/S0001, /timetable/CSE-1, /admin/state, /admin/events)
```

## experiments (failure injection + measurement)
```
python experiments/run_experiments.py --backend docker            # both versions, 6 experiments each
python experiments/run_experiments.py --backend local             # linux, processes + local postgres 16
python experiments/run_experiments.py --backend docker --modes ft --only E1,E2,E4   # short demo
python experiments/analyze.py                                      # MTTF / MTTR / MTBF / availability
```
Results: `results/results.json`, `results/run_log.txt`, `results/summary.txt`.

| id | failure | injection |
|----|---------|-----------|
| E1 | application crash | student-1 process exits (code 1) |
| E2 | database failure | db-primary killed (SIGKILL) |
| E3 | network/service timeout | +3000 ms latency on student-1 |
| E4 | node failure | node A (student-1, payment-1, timetable) frozen 15 s |
| E5 | lost transaction | payment-1 dies mid-payment, records dies after 150/300 transcripts |
| E6 | high load | 80 concurrent clients, 15 s |
