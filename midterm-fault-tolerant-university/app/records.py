"""Academic records service: transcript batch generation with checkpointing"""
import random
import threading
import time

from fastapi import HTTPException, Request
import psycopg

from common import FAULTS, FT, INSTANCE, crash, db_call, make_app

app = make_app("records-service")
CHUNK = 10          # checkpoint every 10 transcripts
ITEM_TIME = 0.02    # "generating" one transcript takes 20 ms
running = set()
done_items = {"n": 0}


def gpa(sid):
    random.seed(sid)
    return round(random.uniform(2.0, 4.0), 2)


def run_batch_ft(batch_id, size):
    """resume from the last checkpoint, every chunk = transcripts + checkpoint in ONE transaction"""
    start = db_call(lambda c: c.execute("SELECT last_index FROM batch_checkpoints WHERE batch_id=%s",
                                        (batch_id,)).fetchone()[0])
    print(f"[{INSTANCE}] batch {batch_id} resume from {start}/{size}", flush=True)
    i = start
    while i < size:
        chunk = list(range(i + 1, min(i + CHUNK, size) + 1))
        rows = []
        for n in chunk:
            time.sleep(ITEM_TIME)
            done_items["n"] += 1
            if FAULTS["crash_after_items"] and done_items["n"] >= FAULTS["crash_after_items"]:
                crash(f"batch {batch_id} interrupted at item {n}")
            sid = "S%04d" % n
            rows.append((batch_id, sid, gpa(sid)))

        def save(conn):
            with conn.transaction():
                conn.cursor().executemany("INSERT INTO transcripts (batch_id, student_id, gpa) VALUES (%s,%s,%s)", rows)
                conn.execute("UPDATE batch_checkpoints SET last_index=%s, updated_at=now(), status=%s WHERE batch_id=%s",
                             (chunk[-1], "done" if chunk[-1] >= size else "running", batch_id))
        db_call(save)
        i = chunk[-1]
    running.discard(batch_id)


def run_batch_baseline(batch_id, size):
    """naive: start from 0 every time, commit each transcript separately, no checkpoint"""
    for n in range(1, size + 1):
        time.sleep(ITEM_TIME)
        done_items["n"] += 1
        if FAULTS["crash_after_items"] and done_items["n"] >= FAULTS["crash_after_items"]:
            crash(f"batch {batch_id} interrupted at item {n}")
        sid = "S%04d" % n

        def save(conn):
            conn.execute("INSERT INTO transcripts (batch_id, student_id, gpa) VALUES (%s,%s,%s)", (batch_id, sid, gpa(sid)))
            conn.commit()
        db_call(save)
    db_call(lambda c: (c.execute("UPDATE batch_checkpoints SET status='done', last_index=%s WHERE batch_id=%s",
                                 (size, batch_id)), c.commit()))
    running.discard(batch_id)


def start_batch(batch_id, size):
    if batch_id in running:
        return
    running.add(batch_id)
    target = run_batch_ft if FT else run_batch_baseline
    threading.Thread(target=target, args=(batch_id, size), daemon=True).start()


@app.on_event("startup")
def resume_unfinished():
    if not FT:
        return

    def q(conn):
        return conn.execute("SELECT batch_id, size FROM batch_checkpoints WHERE status='running'").fetchall()
    try:
        for batch_id, size in db_call(q, retries=10):
            start_batch(batch_id, size)
    except psycopg.OperationalError:
        pass


@app.post("/records/batches")
async def submit(request: Request):
    body = await request.json()
    batch_id, size = body["batch_id"], int(body["size"])

    def q(conn):
        conn.execute("INSERT INTO batch_checkpoints (batch_id, size) VALUES (%s,%s) ON CONFLICT (batch_id) DO NOTHING",
                     (batch_id, size))
        conn.commit()
        return conn.execute("SELECT status FROM batch_checkpoints WHERE batch_id=%s", (batch_id,)).fetchone()[0]
    try:
        status = db_call(q)
    except psycopg.OperationalError:
        raise HTTPException(503, "database unavailable")
    if status != "done" or not FT:
        start_batch(batch_id, size)
    return {"batch_id": batch_id, "status": "accepted", "served_by": INSTANCE}


@app.get("/records/batches/{batch_id}")
def batch_status(batch_id: str):
    def q(conn):
        cp = conn.execute("SELECT size, last_index, status FROM batch_checkpoints WHERE batch_id=%s", (batch_id,)).fetchone()
        rows, distinct = conn.execute("SELECT count(*), count(DISTINCT student_id) FROM transcripts WHERE batch_id=%s",
                                      (batch_id,)).fetchone()
        return cp, rows, distinct
    cp, rows, distinct = db_call(q)
    if not cp:
        raise HTTPException(404, "no such batch")
    return {"batch_id": batch_id, "size": cp[0], "checkpoint": cp[1], "status": cp[2],
            "rows": rows, "unique_students": distinct, "duplicates": rows - distinct,
            "missing": cp[0] - distinct, "running_here": batch_id in running}


@app.get("/records/transcript/{sid}")
def transcript(sid: str):
    return {"student_id": sid, "gpa": gpa(sid), "served_by": INSTANCE}
