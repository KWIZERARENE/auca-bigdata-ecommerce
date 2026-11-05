"""Row-key design for HBase (pure functions, unit-testable without a server).

user_sessions   row key = <user_id>#<reverse_ts>      reverse_ts = MAXTS - start_ms, zero padded to 13 digits
                HBase sorts rows by key BYTES ascending, so the NEWEST session of a user sorts FIRST.
                -> "last N sessions of user X" = prefix scan 'X#' with limit N (reads N rows, then stops).
product_metrics row key = <product_id>#<yyyymmdd>     -> date range for a product = start/stop row scan.
All times are UTC so keys are identical on every machine.
"""
from datetime import datetime, timezone

MAXTS = 10 ** 13            # > any epoch-millisecond value until the year 2286, keeps 13-digit width

def to_ms(iso):
    d = datetime.fromisoformat(iso)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return int(d.timestamp() * 1000)

def session_key(user_id, start_iso):
    return f"{user_id}#{MAXTS - to_ms(start_iso):013d}".encode()

def user_prefix(user_id):
    return f"{user_id}#".encode()

def session_range(user_id, start_iso, end_iso):
    """Row range (start_row inclusive, stop_row exclusive) of one user's sessions with start in [start, end].
    A later time gives a SMALLER reverse timestamp, so the END of the time range is the START row."""
    lo = f"{user_id}#{MAXTS - to_ms(end_iso):013d}".encode()
    hi = f"{user_id}#{MAXTS - to_ms(start_iso) + 1:013d}".encode()
    return lo, hi

def metric_key(product_id, yyyymmdd):
    return f"{product_id}#{yyyymmdd}".encode()

def metric_range(product_id, d1, d2):
    """Product metrics for days d1..d2 inclusive (yyyymmdd strings)."""
    return f"{product_id}#{d1}".encode(), f"{product_id}#{d2}~".encode()     # '~' sorts after any digit

def day_of(iso):
    return datetime.fromisoformat(iso).strftime("%Y%m%d")
