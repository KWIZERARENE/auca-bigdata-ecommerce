"""HBase queries (demonstration for the report).

Usage:
  python hbase/queries.py                       # demo: picks a user and a product automatically
  python hbase/queries.py --user user_000042 --n 5
  python hbase/queries.py --product prod_00123 --from 20261001 --to 20261031
"""
import argparse, sys
from keys import user_prefix, session_range, metric_range

ap = argparse.ArgumentParser()
ap.add_argument("--host", default="localhost")
ap.add_argument("--port", type=int, default=9090)
ap.add_argument("--user")
ap.add_argument("--n", type=int, default=5)
ap.add_argument("--between", nargs=2, metavar=("START_ISO", "END_ISO"))
ap.add_argument("--product")
ap.add_argument("--from", dest="d1", default="20260801")
ap.add_argument("--to", dest="d2", default="20261231")

def last_sessions(table, user_id, n=5):
    """Latest n sessions of a user: prefix scan; newest first thanks to the reverse timestamp."""
    return list(table.scan(row_prefix=user_prefix(user_id), limit=n, columns=[b"info"]))

def sessions_between(table, user_id, start_iso, end_iso):
    lo, hi = session_range(user_id, start_iso, end_iso)
    return list(table.scan(row_start=lo, row_stop=hi, columns=[b"info"]))

def session_detail(table, row_key):
    """All cells (info, pv, cart) of one session = its full activity."""
    return table.row(row_key)

def product_metrics(table, product_id, d1, d2):
    lo, hi = metric_range(product_id, d1, d2)
    return list(table.scan(row_start=lo, row_stop=hi))

def show_session(key, cells):
    g = lambda c: cells.get(c, b"").decode()
    print(f"  {key.decode():<28} start={g(b'info:start')} device={g(b'info:device'):<8} "
          f"referrer={g(b'info:referrer'):<14} page_views={g(b'info:n_pv'):>3} status={g(b'info:status')}")

if __name__ == "__main__":
    a = ap.parse_args()
    import happybase
    try:
        conn = happybase.Connection(a.host, port=a.port, timeout=60000)
        conn.open()
    except Exception as e:
        sys.exit(f"Cannot reach HBase Thrift at {a.host}:{a.port}: {str(e)[:200]}")
    us, pm = conn.table("user_sessions"), conn.table("product_metrics")

    user = a.user
    if not user:                                    # demo: first user that has data
        first = next(us.scan(limit=1, columns=[b"info:session_id"]), None)
        if not first:
            sys.exit("user_sessions is empty. Run hbase/load.py first.")
        user = first[0].decode().split("#")[0]
    print(f"Q1  last {a.n} sessions of {user} (newest first):")
    rows = last_sessions(us, user, a.n)
    for k, c in rows:
        show_session(k, c)
    if rows:
        k = rows[0][0]
        full = session_detail(us, k)
        print(f"    full activity of newest session {k.decode()}: {len(full)} cells "
              f"({sum(1 for c in full if c.startswith(b'pv:'))} page views, "
              f"{sum(1 for c in full if c.startswith(b'cart:'))} cart lines)")
        if a.between is None:
            start = rows[-1][1][b"info:start"].decode()
            end = rows[0][1][b"info:start"].decode()
            a.between = [start, end]
    if a.between:
        print(f"Q2  {user}'s sessions between {a.between[0]} and {a.between[1]}:")
        for k, c in sessions_between(us, user, *a.between):
            show_session(k, c)

    product = a.product
    if not product:
        first = next(pm.scan(limit=1), None)
        product = first[0].decode().split("#")[0] if first else None
    if product:
        print(f"Q3  daily metrics of {product} from {a.d1} to {a.d2}:")
        for k, c in product_metrics(pm, product, a.d1, a.d2)[:15]:
            print(f"  {k.decode():<22} views={c[b'm:views'].decode():>3} cart_adds={c[b'm:cart_adds'].decode():>3} "
                  f"purchases={c[b'm:purchases'].decode():>3}")
