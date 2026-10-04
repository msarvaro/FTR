"""Payment service: tuition payments, idempotent + atomic in ft mode"""
from fastapi import HTTPException, Request
import psycopg

from common import FAULTS, FT, INSTANCE, crash, db_call, make_app

app = make_app("payment-service")


@app.post("/payments")
async def pay(request: Request):
    body = await request.json()
    key = request.headers.get("Idempotency-Key")
    sid, amount = body["student_id"], int(body["amount"])

    def ft_tx(conn):
        # one transaction: payment row + balance. crash in the middle -> postgres rolls back everything
        with conn.transaction():
            row = conn.execute(
                "INSERT INTO payments (student_id, amount, client_ref, idem_key) VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (idem_key) WHERE idem_key IS NOT NULL DO NOTHING RETURNING id",
                (sid, amount, key, key)).fetchone()
            if row is None:
                old = conn.execute("SELECT id FROM payments WHERE idem_key=%s", (key,)).fetchone()
                return old[0], True
            if FAULTS["crash_on_next_payment"]:
                crash("payment interrupted after insert, before balance update")
            conn.execute("UPDATE students SET paid = paid + %s WHERE id=%s", (amount, sid))
            return row[0], False

    def baseline_tx(conn):
        # naive: two separate commits, no idempotency key
        conn.execute("UPDATE students SET paid = paid + %s WHERE id=%s", (amount, sid))
        conn.commit()
        if FAULTS["crash_on_next_payment"]:
            crash("payment interrupted after balance update, before payment row")
        row = conn.execute("INSERT INTO payments (student_id, amount, client_ref) VALUES (%s,%s,%s) RETURNING id",
                           (sid, amount, key)).fetchone()
        conn.commit()
        return row[0], False

    try:
        pid, dup = db_call(ft_tx if FT else baseline_tx)
    except psycopg.OperationalError:
        raise HTTPException(503, "database unavailable")
    return {"payment_id": pid, "duplicate": dup, "served_by": INSTANCE}


@app.get("/payments/consistency")
def consistency():
    def q(conn):
        paid = conn.execute("SELECT coalesce(sum(paid),0) FROM students").fetchone()[0]
        rows, total, refs, distinct = conn.execute(
            "SELECT count(*), coalesce(sum(amount),0), count(client_ref), count(DISTINCT client_ref) FROM payments"
        ).fetchone()
        return {"balance_sum": paid, "payments_sum": total, "payment_rows": rows,
                "duplicates": refs - distinct, "consistent": paid == total}
    return db_call(q)
