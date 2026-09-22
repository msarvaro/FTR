"""
Assignment 2 - Software Fault-Tolerant Design
AITU Student Scholarship and Tuition Payment Processing

Student: Sarvarov Mustafa (ID 255609, group CSE-2501M)
Instructor: Serek Azamat
Version: v1.0

Run:  python3 payment_processor.py
Writes the full run log (baseline, exceptions, retry, checkpoints, analysis) to output_log.txt
Plain Python, no external libraries.
"""

import copy
import datetime

# ---------------------------------------------------------------- input data
TRANSACTIONS = [
    ("T001", 12000, None), ("T002", 25000, "Network"), ("T003", 8000, None),
    ("T004", 45000, "Timeout"), ("T005", 13000, None), ("T006", 70000, "Database"),
    ("T007", 9000, None), ("T008", 31000, "Network"), ("T009", 15000, None),
    ("T010", 50000, "Timeout"), ("T011", 6000, None), ("T012", 80000, "Database"),
    ("T013", 11000, None), ("T014", 22000, None), ("T015", 40000, "Network"),
]

# fault rules: how many attempts in a row fail for each failure type
# Network  -> fail, success
# Timeout  -> fail, fail, success
# Database -> fail, fail, fail -> rollback
FAILS_IN_A_ROW = {"Network": 1, "Timeout": 2, "Database": 3}
MAX_RETRIES = 2            # initial attempt + 2 retries = 3 attempts max
CHECKPOINT_EVERY = 5       # checkpoint after every 5 successful transactions

LOG_LINES = []


def log(**fields):
    """structured log line: time + key=value pairs"""
    ts = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    line = ts + " " + " ".join("%s=%s" % (k, v) for k, v in fields.items())
    LOG_LINES.append(line)
    print(line)


def section(title):
    LOG_LINES.append("")
    LOG_LINES.append("=" * 70)
    LOG_LINES.append(title)
    LOG_LINES.append("=" * 70)
    print("\n" + title)


# ---------------------------------------------------------------- exceptions
class TransactionError(Exception):
    failure = "Unknown"
    retryable = False


class NetworkError(TransactionError):
    failure = "Network"
    retryable = True


class PaymentTimeoutError(TransactionError):
    failure = "Timeout"
    retryable = True


class DatabaseError(TransactionError):
    failure = "Database"
    retryable = True     # retried by the rule, but after 3 fails -> rollback


class ValidationError(TransactionError):
    failure = "Validation"
    retryable = False    # bad input, retry never helps


# ---------------------------------------------------------------- fault simulation
class FaultInjector:
    """makes the given transaction fail exactly as the fault rules say"""

    def __init__(self):
        self.attempts = {}

    def check(self, tx_id, failure, step):
        if failure is None:
            return
        n = self.attempts.get(tx_id, 0)
        where = {"Network": "process", "Timeout": "process", "Database": "record"}[failure]
        if step != where:
            return
        self.attempts[tx_id] = n + 1
        if n < FAILS_IN_A_ROW[failure]:
            if failure == "Network":
                raise NetworkError("bank gateway unreachable")
            if failure == "Timeout":
                raise PaymentTimeoutError("bank gateway did not answer in time")
            if failure == "Database":
                raise DatabaseError("ledger write failed")


class BankGateway:
    """external side: remembers idempotency keys so the same payment is never charged twice"""

    def __init__(self):
        self.charged = {}

    def charge(self, tx_id, amount):
        if tx_id in self.charged:
            return "already_charged"
        self.charged[tx_id] = amount
        return "charged"

    def void(self, tx_id):
        self.charged.pop(tx_id, None)


# ---------------------------------------------------------------- pipeline
def validate(tx_id, amount):
    if amount <= 0:
        raise ValidationError("amount must be positive")


def process(tx_id, amount, failure, faults, gateway):
    # timeout = request reached the bank but the answer got lost,
    # so the charge happens BEFORE the error. retry must not charge again
    if failure == "Timeout":
        gateway.charge(tx_id, amount)
    faults.check(tx_id, failure, "process")
    return gateway.charge(tx_id, amount)


def record(tx_id, amount, failure, faults, state):
    state["ledger"].append((tx_id, amount))          # partial write happens first
    faults.check(tx_id, failure, "record")           # then db can fail
    state["done"].add(tx_id)
    state["count"] += 1
    state["total"] += amount


def new_state():
    return {"ledger": [], "done": set(), "count": 0, "total": 0}


# ---------------------------------------------------------------- Part A
def run_baseline():
    section("PART A - BASELINE (no fault tolerance, stops on first failure)")
    faults, gateway, state = FaultInjector(), BankGateway(), new_state()
    attempted = 0
    try:
        for tx_id, amount, failure in TRANSACTIONS:
            attempted += 1
            validate(tx_id, amount)
            process(tx_id, amount, failure, faults, gateway)
            record(tx_id, amount, failure, faults, state)
            log(tx=tx_id, amount=amount, result="success")
    except TransactionError as e:
        log(tx=tx_id, failure=e.failure, result="CRASH", msg='"%s"' % e)
        log(event="processor_stopped", at=tx_id)

    total = sum(a for _, a, _ in TRANSACTIONS)
    res = {
        "attempted": attempted,
        "success": state["count"],
        "lost": len(TRANSACTIONS) - state["count"],
        "processed": state["total"],
        "lost_amount": total - state["total"],
        "retries": 0, "rollbacks": 0,
    }
    for k, v in res.items():
        LOG_LINES.append("  %-12s %s" % (k, v))
    return res


# ---------------------------------------------------------------- Part B
def run_exceptions_only():
    section("PART B - EXCEPTION HANDLING (try/except, classify, log, continue)")
    faults, gateway, state = FaultInjector(), BankGateway(), new_state()
    failed = []
    for tx_id, amount, failure in TRANSACTIONS:
        try:
            validate(tx_id, amount)
            process(tx_id, amount, failure, faults, gateway)
            record(tx_id, amount, failure, faults, state)
            log(tx=tx_id, amount=amount, result="success")
        except TransactionError as e:
            if isinstance(e, DatabaseError):
                state["ledger"].pop()        # drop the half written row
                gateway.void(tx_id)
            failed.append(tx_id)
            log(tx=tx_id, failure=e.failure, exception=type(e).__name__,
                action="logged_and_skipped", result="failed")
    LOG_LINES.append("  success=%d failed=%d processed=%d" % (state["count"], len(failed), state["total"]))
    return state["count"], failed, state["total"]


# ---------------------------------------------------------------- Part C + D
def make_checkpoint(state, checkpoints):
    cp = {
        "name": "CP%d" % (len(checkpoints) + 1),
        "count": state["count"],
        "total": state["total"],
        "ids": sorted(state["done"]),
        "state": copy.deepcopy(state),
    }
    checkpoints.append(cp)
    log(event="checkpoint", cp=cp["name"], count=cp["count"], total=cp["total"],
        ids="%s..%s" % (cp["ids"][0], cp["ids"][-1]))


def rollback(state, checkpoints, tx_id, gateway, replay):
    cp = checkpoints[-1] if checkpoints else {"name": "START", "state": new_state()}
    restored = copy.deepcopy(cp["state"])
    # anything committed after the checkpoint (not the failed one) gets replayed
    lost_after_cp = [t for t in state["ledger"] if t[0] not in cp["state"]["done"] and t[0] != tx_id]
    gateway.void(tx_id)      # compensation: failed payment must not stay charged
    state.clear()
    state.update(restored)
    log(event="rollback", tx=tx_id, to=cp["name"], count=state["count"], total=state["total"],
        replay=len(lost_after_cp))
    for t_id, amount in lost_after_cp:
        replay(t_id, amount)


def run_fault_tolerant():
    section("PART C + D - RETRY + CHECKPOINT + ROLLBACK")
    faults, gateway, state = FaultInjector(), BankGateway(), new_state()
    checkpoints, attempt_log = [], []
    retries = rollbacks = 0
    step = 0
    failed = []

    def replay(t_id, amount):
        record(t_id, amount, None, faults, state)
        log(event="replay", tx=t_id)

    for tx_id, amount, failure in TRANSACTIONS:
        attempt = 0
        while True:
            attempt += 1
            step += 1
            try:
                validate(tx_id, amount)
                gw = process(tx_id, amount, failure, faults, gateway)
                record(tx_id, amount, failure, faults, state)
                action = "commit" if gw == "charged" else "commit (idempotent, no 2nd charge)"
                attempt_log.append((step, tx_id, attempt, failure or "None", action, "Success"))
                log(step=step, tx=tx_id, attempt=attempt, gateway=gw, result="success")
                if state["count"] % CHECKPOINT_EVERY == 0:
                    make_checkpoint(state, checkpoints)
                break
            except TransactionError as e:
                if isinstance(e, DatabaseError):
                    state["ledger"].pop()            # undo the half written row before retry
                if e.retryable and attempt <= MAX_RETRIES:
                    retries += 1
                    attempt_log.append((step, tx_id, attempt, e.failure, "retry", "Fail"))
                    log(step=step, tx=tx_id, attempt=attempt, failure=e.failure, action="retry")
                    continue
                if isinstance(e, DatabaseError):
                    rollbacks += 1
                    attempt_log.append((step, tx_id, attempt, e.failure, "rollback", "Rollback"))
                    log(step=step, tx=tx_id, attempt=attempt, failure=e.failure, action="retries_exhausted")
                    rollback(state, checkpoints, tx_id, gateway, replay)
                else:
                    attempt_log.append((step, tx_id, attempt, e.failure, "give up", "Failed"))
                    log(step=step, tx=tx_id, attempt=attempt, failure=e.failure, action="give_up")
                failed.append(tx_id)
                break

    # rule gives only 2 checkpoints (13 successes < 15), so save the final state too
    if state["count"] % CHECKPOINT_EVERY != 0:
        log(event="end_of_batch")
        make_checkpoint(state, checkpoints)

    section("ATTEMPT LOG (generated)")
    LOG_LINES.append("%-5s %-5s %-8s %-9s %-36s %s" % ("step", "tx", "attempt", "failure", "action", "result"))
    for row in attempt_log:
        LOG_LINES.append("%-5s %-5s %-8s %-9s %-36s %s" % row)

    section("CHECKPOINTS")
    for cp in checkpoints:
        LOG_LINES.append("  %s count=%d total=%d ids=%s" % (cp["name"], cp["count"], cp["total"], ",".join(cp["ids"])))

    total = sum(a for _, a, _ in TRANSACTIONS)
    charged = sum(gateway.charged.values())
    LOG_LINES.append("")
    LOG_LINES.append("  bank charged total=%d  ledger total=%d  match=%s" % (charged, state["total"], charged == state["total"]))

    return {
        "attempted": len(TRANSACTIONS),
        "success": state["count"],
        "lost": len(failed),
        "processed": state["total"],
        "lost_amount": total - state["total"],
        "retries": retries,
        "rollbacks": rollbacks,
        "attempts_total": step,
    }


# ---------------------------------------------------------------- Part E
def analysis(base, ft, b_success):
    section("PART E - BEFORE / AFTER")
    n = len(TRANSACTIONS)
    rows = [
        ("successful", base["success"], ft["success"]),
        ("failed", base["lost"], ft["lost"]),
        ("lost amount", base["lost_amount"], ft["lost_amount"]),
        ("retries", base["retries"], ft["retries"]),
        ("rollbacks", base["rollbacks"], ft["rollbacks"]),
    ]
    for name, b, f in rows:
        LOG_LINES.append("  %-12s baseline=%-8s ft=%-8s diff=%s" % (name, b, f, f - b))
    cr_b = base["success"] / n * 100
    cr_f = ft["success"] / n * 100
    LOG_LINES.append("  completion   baseline=%.2f%% ft=%.2f%% diff=+%.2f pp" % (cr_b, cr_f, cr_f - cr_b))
    faulty = [t for t in TRANSACTIONS if t[2]]
    recovered = ft["success"] - sum(1 for t in TRANSACTIONS if not t[2])
    LOG_LINES.append("  recovery: %d of %d faulty transactions recovered = %.1f%%"
                     % (recovered, len(faulty), recovered / len(faulty) * 100))
    LOG_LINES.append("  loss reduction (count):  (%d-%d)/%d = %.1f%%"
                     % (base["lost"], ft["lost"], base["lost"], (base["lost"] - ft["lost"]) / base["lost"] * 100))
    LOG_LINES.append("  loss reduction (amount): (%d-%d)/%d = %.1f%%"
                     % (base["lost_amount"], ft["lost_amount"], base["lost_amount"],
                        (base["lost_amount"] - ft["lost_amount"]) / base["lost_amount"] * 100))
    LOG_LINES.append("  contribution: exceptions only -> %d success, retry adds -> %d"
                     % (b_success - base["success"], ft["success"] - b_success))
    LOG_LINES.append("  total attempts with retry: %d" % ft["attempts_total"])


if __name__ == "__main__":
    LOG_LINES.append("payment_processor.py v1.0  run at %s" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    base = run_baseline()
    b_success, b_failed, b_total = run_exceptions_only()
    ft = run_fault_tolerant()
    analysis(base, ft, b_success)
    with open("output_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES) + "\n")
    print("\nlog written to output_log.txt")
