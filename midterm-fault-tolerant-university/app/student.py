"""Student service: student info + course registration"""
from fastapi import HTTPException, Request
import psycopg

from common import FT, INSTANCE, db_call, make_app

app = make_app("student-service")


@app.get("/students/{sid}")
def get_student(sid: str):
    def q(conn):
        return conn.execute("SELECT id, name, paid FROM students WHERE id=%s", (sid,)).fetchone()
    try:
        row = db_call(q)
    except psycopg.OperationalError:
        raise HTTPException(503, "database unavailable")
    if not row:
        raise HTTPException(404, "no such student")
    return {"id": row[0], "name": row[1], "paid": row[2], "served_by": INSTANCE}


@app.post("/enrollments")
async def enroll(request: Request):
    body = await request.json()
    key = request.headers.get("Idempotency-Key")

    def q(conn):
        if FT and key:
            # duplicate-request detection: same request id -> same result, no second row
            row = conn.execute(
                "INSERT INTO enrollments (student_id, course, client_ref, request_id) VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (request_id) WHERE request_id IS NOT NULL DO NOTHING RETURNING id",
                (body["student_id"], body["course"], key, key)).fetchone()
            if row is None:
                old = conn.execute("SELECT id FROM enrollments WHERE request_id=%s", (key,)).fetchone()
                conn.commit()
                return old[0], True
            conn.commit()
            return row[0], False
        row = conn.execute(
            "INSERT INTO enrollments (student_id, course, client_ref) VALUES (%s,%s,%s) RETURNING id",
            (body["student_id"], body["course"], key)).fetchone()
        conn.commit()
        return row[0], False
    try:
        eid, dup = db_call(q)
    except psycopg.OperationalError:
        raise HTTPException(503, "database unavailable")
    return {"enrollment_id": eid, "duplicate": dup, "served_by": INSTANCE}


@app.get("/enrollments/audit")
def audit():
    def q(conn):
        total, refs, distinct = conn.execute(
            "SELECT count(*), count(client_ref), count(DISTINCT client_ref) FROM enrollments").fetchone()
        return {"rows": total, "duplicates": refs - distinct}
    return db_call(q)


@app.post("/enrollments/check")
async def check(request: Request):
    """which of the given client refs exist in the db (used to find lost acknowledged writes)"""
    refs = (await request.json())["refs"]

    def q(conn):
        rows = conn.execute("SELECT DISTINCT client_ref FROM enrollments WHERE client_ref = ANY(%s)", (refs,)).fetchall()
        return {"found": [r[0] for r in rows]}
    return db_call(q)
