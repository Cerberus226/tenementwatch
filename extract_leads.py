#!/usr/bin/env python3
"""Extract segmented lead lists from tenements.db into CSV files.

Three segments, each with a different buyer + pitch:
  1. active_explorers.csv  — new applications (sell to drillers/labs/consultants)
  2. expiring_30d.csv      — LIVE tenements expiring within 30 days (sell to compliance/legal)
  3. top_holders.csv       — holders by tenement count (sell to major-mining service vendors)
"""
import sqlite3, csv, os
from datetime import datetime, timedelta, timezone

DB = r"C:\Users\User\tenementwatch\tenements.db"
OUT = r"C:\Users\User\tenementwatch\leads"
os.makedirs(OUT, exist_ok=True)

con = sqlite3.connect(DB)
cur = con.cursor()
today = datetime.now(timezone.utc).date()
soon = (today + timedelta(days=30)).isoformat()

def write(name, sql, params=(), header=None):
    path = os.path.join(OUT, name)
    cur.execute(sql, params)
    rows = cur.fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if header:
            w.writerow(header)
        w.writerows(rows)
    return path, len(rows)

# 1. Active explorers (new applications)
p1, n1 = write(
    "active_explorers.csv",
    "SELECT state, tenement_id, type, holder, commodity, application_date FROM tenements "
    "WHERE status='Application' ORDER BY application_date DESC",
    header=["state","tenement_id","type","holder","commodity","application_date"],
)

# 2. Expiring within 30 days (LIVE/PENDING/Granted only)
p2, n2 = write(
    "expiring_30d.csv",
    "SELECT state, tenement_id, type, holder, expire_date, status FROM tenements "
    "WHERE status IN ('LIVE','PENDING','Granted') AND expire_date IS NOT NULL AND expire_date != '' "
    "AND expire_date >= ? AND expire_date <= ? ORDER BY expire_date ASC",
    (today.isoformat(), soon),
    header=["state","tenement_id","type","holder","expire_date","status"],
)

# 3. Top holders by tenement count
p3, n3 = write(
    "top_holders.csv",
    "SELECT holder, COUNT(*) AS tenement_count FROM tenements "
    "WHERE holder IS NOT NULL AND holder != '' GROUP BY holder ORDER BY tenement_count DESC LIMIT 200",
    header=["holder","tenement_count"],
)

print(f"1. active_explorers.csv — {n1} leads")
print(f"2. expiring_30d.csv    — {n2} leads")
print(f"3. top_holders.csv     — {n3} holders")
print(f"\nWritten to {OUT}/")
con.close()
