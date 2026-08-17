#!/usr/bin/env python3
"""
TenementWatch API v0.1.0
Multi-state Australian mining tenement data.
Run: python server.py [--port 8000] [--refresh]
"""
import asyncio
import hashlib
import http.server
import json
import os
import sqlite3
import sys
import signal
import secrets
import threading
import urllib.parse
from datetime import datetime, timezone
from typing import Optional
import httpx

DB_PATH = os.path.join(os.path.dirname(__file__), "tenements.db")
WA_URL = "https://public-services.slip.wa.gov.au/public/rest/services/SLIP_Public_Services/Industry_and_Mining/MapServer/3/query"
QLD_URL = "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Economy/MineralTenement/MapServer/0/query"

# API tiers: calls per day
TIERS = {"free": 100, "starter": 1000, "pro": 10000}

# ── Database ────────────────────────────────────────────────────

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    c = db()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS tenements (
            uid TEXT PRIMARY KEY,
            state TEXT NOT NULL,
            tenement_id TEXT NOT NULL,
            name TEXT,
            type TEXT,
            status TEXT,
            holder TEXT,
            commodity TEXT,
            area_ha REAL,
            grant_date TEXT,
            expire_date TEXT,
            application_date TEXT,
            hash TEXT,
            first_seen TEXT NOT NULL,
            last_updated TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenement_uid TEXT NOT NULL,
            tenement_id TEXT NOT NULL,
            state TEXT NOT NULL,
            old_status TEXT,
            new_status TEXT,
            change_type TEXT,
            detail TEXT,
            changed_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_state ON tenements(state);
        CREATE INDEX IF NOT EXISTS idx_status ON tenements(status);
        CREATE INDEX IF NOT EXISTS idx_holder ON tenements(holder);
        CREATE INDEX IF NOT EXISTS idx_commodity ON tenements(commodity);
        CREATE INDEX IF NOT EXISTS idx_changes_state ON changes(state);
        CREATE TABLE IF NOT EXISTS api_keys (
            key TEXT PRIMARY KEY,
            name TEXT,
            tier TEXT DEFAULT 'free',
            calls_today INTEGER DEFAULT 0,
            day_date TEXT,
            created_at TEXT NOT NULL,
            last_used_at TEXT
        );
    """)
    c.commit()
    c.close()

def seed_bootstrap_keys():
    """Ensure a bootstrap API key from the BOOTSTRAP_API_KEY env var exists."""
    key = os.environ.get("BOOTSTRAP_API_KEY", "").strip()
    if not key:
        return
    c = db()
    if c.execute("SELECT key FROM api_keys WHERE key=?", (key,)).fetchone():
        c.close()
        return
    c.execute("INSERT INTO api_keys(key, name, tier, created_at) VALUES(?,?,?,?)",
              (key, "bootstrap", "pro", datetime.now(timezone.utc).isoformat()))
    c.commit()
    c.close()
    print("[boot] seeded bootstrap API key (pro tier)")

# ── Scraper ─────────────────────────────────────────────────────

async def fetch_state(state: str) -> list[dict]:
    url = WA_URL if state == "wa" else QLD_URL
    params = {"where": "1=1", "outFields": "*", "returnGeometry": "false", "f": "json", "resultRecordCount": 10000}

    async with httpx.AsyncClient(timeout=90) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

    features = data.get("features", [])
    now = datetime.now(timezone.utc).isoformat()

    if state == "wa":
        return _normalize_wa(features, now)
    return _normalize_qld(features, now)

def _normalize_wa(features, now):
    results = []
    for f in features:
        a = f.get("attributes", {})
        tid = a.get("tenid", "").strip()
        if not tid:
            continue
        raw = json.dumps(a, sort_keys=True)
        uid = f"wa:{tid}"
        results.append({
            "uid": uid, "state": "wa", "tenement_id": tid,
            "name": a.get("fmt_tenid", tid),
            "type": a.get("type", "").strip(),
            "status": a.get("tenstatus", "").strip(),
            "holder": (a.get("holder1", "") or "").strip(),
            "commodity": "",
            "area_ha": a.get("legal_area"),
            "grant_date": _date(a.get("grantdate")),
            "expire_date": _date(a.get("enddate")),
            "application_date": _date(a.get("startdate")),
            "hash": hashlib.md5(raw.encode()).hexdigest(),
            "first_seen": now, "last_updated": now,
        })
    return results

def _normalize_qld(features, now):
    results = []
    for f in features:
        a = f.get("attributes", {})
        tid = (a.get("tenid") or a.get("fileid") or "").strip()
        if not tid:
            continue
        raw = json.dumps(a, sort_keys=True)
        uid = f"qld:{tid}"
        results.append({
            "uid": uid, "state": "qld", "tenement_id": tid,
            "name": a.get("tenname", tid),
            "type": a.get("tentype", "").strip(),
            "status": a.get("tenstatus", "").strip(),
            "holder": (a.get("tenowner", "") or "").strip(),
            "commodity": (a.get("tenmineral", "") or "").strip(),
            "area_ha": _safe_float(a.get("shapearea")),
            "grant_date": _date(a.get("grantdate")),
            "expire_date": _date(a.get("expiredate")),
            "application_date": _date(a.get("appdate")),
            "hash": hashlib.md5(raw.encode()).hexdigest(),
            "first_seen": now, "last_updated": now,
        })
    return results

def _date(val):
    if not val or str(val).strip() in ("", "0", "None"):
        return None
    s = str(val).strip()
    if s.isdigit() and len(s) >= 10:
        try:
            ts = int(s)/1000 if len(s)>10 else int(s)
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        except: pass
    if len(s) >= 10:
        return s[:10]
    return s

def _safe_float(val):
    try: return float(val) if val not in (None,"","None") else None
    except: return None

async def refresh_state(state: str):
    c = db()
    tenements = await fetch_state(state)
    new_count, changed_count = 0, 0
    now = datetime.now(timezone.utc).isoformat()

    for t in tenements:
        existing = c.execute("SELECT status, hash FROM tenements WHERE uid=?", (t["uid"],)).fetchone()
        if existing:
            if existing["hash"] != t["hash"]:
                old_status = existing["status"]
                if old_status and old_status != t["status"]:
                    c.execute(
                        "INSERT INTO changes(tenement_uid, tenement_id, state, old_status, new_status, change_type, detail, changed_at) VALUES(?,?,?,?,?,?,?,?)",
                        (t["uid"], t["tenement_id"], t["state"], old_status, t["status"], "status_change",
                         json.dumps({"old": old_status, "new": t["status"]}), now))
                    changed_count += 1
                c.execute("""UPDATE tenements SET name=?,type=?,status=?,holder=?,commodity=?,
                    area_ha=?,grant_date=?,expire_date=?,application_date=?,hash=?,last_updated=? WHERE uid=?""",
                    (t["name"],t["type"],t["status"],t["holder"],t["commodity"],
                     t["area_ha"],t["grant_date"],t["expire_date"],t["application_date"],
                     t["hash"],now,t["uid"]))
            else:
                c.execute("UPDATE tenements SET last_updated=? WHERE uid=?", (now, t["uid"]))
        else:
            c.execute("""INSERT INTO tenements(uid,state,tenement_id,name,type,status,holder,commodity,
                area_ha,grant_date,expire_date,application_date,hash,first_seen,last_updated)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (t["uid"],t["state"],t["tenement_id"],t["name"],t["type"],t["status"],t["holder"],
                 t["commodity"],t["area_ha"],t["grant_date"],t["expire_date"],t["application_date"],
                 t["hash"],t["first_seen"],now))
            new_count += 1

    c.commit()
    c.close()
    return {"state": state, "total": len(tenements), "new": new_count, "changes": changed_count}

# ── HTTP API ────────────────────────────────────────────────────

def row_to_dict(r):
    return dict(r) if r else None

LANDING = """<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>TenementWatch API</title>
<style>:root{--bg:#0a0a0f;--card:#12121a;--gold:#bdb970;--text:#c8c8d0;--muted:#6b6b78;--border:#1e1e2a}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;line-height:1.6}
.container{max-width:900px;margin:0 auto;padding:2rem}
header{padding:3rem 0 2rem;text-align:center}
h1{font-size:2.5rem;color:#fff}h1 span{color:var(--gold)}
.subtitle{color:var(--muted);font-size:1.1rem;margin-bottom:1.5rem}
.cta{display:inline-block;background:var(--gold);color:#0a0a0f;padding:.75rem 2rem;border-radius:6px;font-weight:600;text-decoration:none}
.card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:1.5rem;margin:1.5rem 0}
.card h3{color:#fff;margin-bottom:.75rem}
pre{background:#08080f;border:1px solid var(--border);border-radius:6px;padding:1rem;overflow-x:auto;font-size:.85rem}
.endpoint{color:var(--gold)}.method{color:#7ecb8a;font-weight:600}
.pricing-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:1rem;margin:1.5rem 0}
.plan{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:1.5rem;text-align:center}
.plan.featured{border-color:var(--gold)}
.plan h3{color:#fff}.plan .price{font-size:2rem;color:var(--gold);font-weight:700}
.plan .period{color:var(--muted)}.plan ul{list-style:none;text-align:left;margin:1rem 0}
.plan li{padding:.25rem 0}.plan li::before{content:'✓ ';color:var(--gold)}
footer{text-align:center;color:var(--muted);padding:2rem 0;font-size:.85rem}</style></head>
<body><div class="container">
<header><h1>Tenement<span>Watch</span> API</h1>
<p class="subtitle">Live Australian mining tenement data — WA + QLD.<br>Simple REST API. No maps. Just JSON.</p>
<a href="#endpoints" class="cta">API Docs</a></header>

<div class="card"><h3>Why TenementWatch?</h3>
<p>Government tenement data is public but scattered across state portals. TenementWatch normalizes it into a single REST API with change detection. Track new applications, grants, surrenders, and expiries — <strong>before it hits the news</strong>.</p></div>

<h3 style="color:#fff;margin-top:2rem">Pricing</h3>
<div class="pricing-grid">
<div class="plan"><h3>Free</h3><div class="price">$0</div><div class="period">/month</div>
<ul><li>100 calls/day</li><li>WA + QLD</li><li>7-day lookback</li></ul></div>
<div class="plan featured"><h3>Starter</h3><div class="price">$99</div><div class="period">/month</div>
<ul><li>1,000 calls/day</li><li>3 webhooks</li><li>30-day lookback</li></ul></div>
<div class="plan"><h3>Pro</h3><div class="price">$299</div><div class="period">/month</div>
<ul><li>10,000 calls/day</li><li>20 webhooks</li><li>Full history</li></ul></div></div>

<div class="card" id="endpoints"><h3>API Endpoints</h3>
<pre><span class="method">GET</span> <span class="endpoint">/</span>                — API info + counts
<span class="method">GET</span> <span class="endpoint">/tenements</span>       — Search: ?state=wa&status=LIVE&holder=rio&commodity=gold&granted_since=2026-01-01&limit=50
<span class="method">GET</span> <span class="endpoint">/tenements/{uid}</span> — Single tenement + status history
<span class="method">GET</span> <span class="endpoint">/changes</span>         — Recent changes: ?state=qld&since=2026-08-01&limit=50
<span class="method">POST</span> <span class="endpoint">/refresh/{state}</span> — Trigger data refresh (wa, qld)
<span class="method">GET</span> <span class="endpoint">/health</span>          — Health check</pre></div>

<footer>TenementWatch API · Public data from WA DMIRS & QLD Resources · CC-BY 4.0 · Built in Newcastle, Australia</footer>
</div></body></html>"""

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args): pass  # quiet

    def _json(self, data, code=200):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())

    def _html(self, html, code=200):
        self.send_response(code)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(html.encode())

    def _auth(self):
        """Return (key_row, tier) if authorized and under rate limit, else None."""
        key = self.headers.get("X-API-Key") or \
              self.headers.get("Authorization", "").replace("Bearer ", "").strip()
        if not key:
            q = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(self.path).query))
            key = q.get("api_key", "")
        if not key:
            self._json({"error": "Missing API key — get one at /landing"}, 401)
            return None
        c = db()
        row = c.execute("SELECT key, name, tier, calls_today, day_date FROM api_keys WHERE key=?",
                        (key,)).fetchone()
        if not row:
            c.close()
            self._json({"error": "Invalid API key"}, 401)
            return None
        limit = TIERS.get(row["tier"], 100)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        now = datetime.now(timezone.utc).isoformat()
        if row["day_date"] != today:
            calls = 1
        else:
            calls = row["calls_today"] + 1
        if calls > limit:
            c.close()
            self._json({"error": f"Rate limit exceeded ({limit}/day on {row['tier']} tier)"}, 429)
            return None
        c.execute("UPDATE api_keys SET calls_today=?, day_date=?, last_used_at=? WHERE key=?",
                  (calls, today, now, key))
        c.commit()
        c.close()
        return row["tier"]

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip("/")
        params = dict(urllib.parse.parse_qsl(parsed.query))

        if path == "" or path == "/landing":
            return self._html(LANDING)

        if path == "/health":
            return self._json({"status": "ok", "ts": datetime.now(timezone.utc).isoformat()})

        if self._auth() is None:
            return

        if path == "/tenements":
            return self._handle_tenements(params)

        if path.startswith("/tenements/"):
            uid = path.split("/tenements/", 1)[1]
            return self._handle_tenement(uid)

        if path == "/changes":
            return self._handle_changes(params)

        self._json({"error": "Not found"}, 404)

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path.rstrip("/")
        if path == "/keys":
            return self._handle_mint_key()
        if path.startswith("/refresh/"):
            if self._auth() is None:
                return
            state = path.split("/refresh/", 1)[1]
            if state not in ("wa", "qld"):
                return self._json({"error": "Unknown state"}, 400)
            result = asyncio.run(refresh_state(state))
            return self._json(result)
        self._json({"error": "Not found"}, 404)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _handle_mint_key(self):
        admin = os.environ.get("ADMIN_KEY", "").strip()
        if not admin or self.headers.get("X-Admin-Key", "").strip() != admin:
            return self._json({"error": "Unauthorized"}, 401)
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode() or "{}")
        except Exception:
            return self._json({"error": "Invalid JSON body"}, 400)
        name = str(body.get("name", "")).strip()
        tier = str(body.get("tier", "free")).strip().lower()
        if not name:
            return self._json({"error": "name is required"}, 400)
        if tier not in TIERS:
            return self._json({"error": "tier must be free|starter|pro"}, 400)
        key = "tw_" + secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc).isoformat()
        c = db()
        c.execute("INSERT INTO api_keys(key, name, tier, created_at) VALUES(?,?,?,?)",
                  (key, name, tier, now))
        c.commit()
        c.close()
        return self._json({"name": name, "tier": tier, "key": key})

    def _handle_tenements(self, params):
        c = db()
        where = []
        vals = []

        for key, col in [("state","state"),("status","status"),("commodity","commodity")]:
            if key in params:
                where.append(f"{col}=?" if key != "commodity" else f"{col} LIKE ?")
                val = params[key].upper()
                if key == "state":
                    val = params[key].lower()  # state stored as lowercase
                elif key == "commodity":
                    val = f"%{params[key]}%"
                vals.append(val)

        if "holder" in params:
            where.append("holder LIKE ?")
            vals.append(f"%{params['holder']}%")
        if "granted_since" in params:
            where.append("grant_date >= ?")
            vals.append(params["granted_since"])

        w = " AND ".join(where) if where else "1=1"
        limit = min(int(params.get("limit", 50)), 500)
        offset = int(params.get("offset", 0))

        rows = c.execute(f"SELECT * FROM tenements WHERE {w} ORDER BY last_updated DESC LIMIT ? OFFSET ?",
                         vals + [limit, offset]).fetchall()
        count = c.execute(f"SELECT COUNT(*) FROM tenements WHERE {w}", vals).fetchone()[0]
        c.close()
        self._json({"count": count, "limit": limit, "offset": offset, "results": [dict(r) for r in rows]})

    def _handle_tenement(self, uid):
        c = db()
        row = c.execute("SELECT * FROM tenements WHERE uid=?", (uid,)).fetchone()
        if not row:
            c.close()
            return self._json({"error": "Not found"}, 404)
        changes = c.execute("SELECT * FROM changes WHERE tenement_uid=? ORDER BY changed_at DESC", (uid,)).fetchall()
        c.close()
        self._json({"tenement": dict(row), "status_history": [dict(ch) for ch in changes]})

    def _handle_changes(self, params):
        c = db()
        where = []
        vals = []
        if "state" in params:
            where.append("state=?")
            vals.append(params["state"].lower())
        if "since" in params:
            where.append("changed_at >= ?")
            vals.append(params["since"])
        w = " AND ".join(where) if where else "1=1"
        limit = min(int(params.get("limit", 50)), 500)
        rows = c.execute(f"SELECT * FROM changes WHERE {w} ORDER BY changed_at DESC LIMIT ?", vals + [limit]).fetchall()
        c.close()
        self._json({"count": len(rows), "results": [dict(r) for r in rows]})

# ── Main ────────────────────────────────────────────────────────

async def fetch_both():
    print("[refresh] Fetching WA + QLD...")
    wa = await refresh_state("wa")
    ql = await refresh_state("qld")
    print(f"  WA: {wa}")
    print(f"  QLD: {ql}")
    return wa, ql

def _seed_background():
    try:
        c = db()
        n = c.execute("SELECT COUNT(*) FROM tenements").fetchone()[0]
        c.close()
        if n == 0:
            print("[boot] DB empty — seeding WA + QLD in background...")
            asyncio.run(fetch_both())
        else:
            print(f"[boot] DB has {n} tenements — skipping seed")
    except Exception as e:
        print(f"[boot] seed error: {e}")

def main():
    port = int(os.environ.get("PORT", "8000"))
    if "--port" in sys.argv:
        port = int(sys.argv[sys.argv.index("--port") + 1])

    init_db()

    seed_bootstrap_keys()

    if "--refresh" in sys.argv:
        asyncio.run(fetch_both())
    elif "--seed-if-empty" in sys.argv:
        threading.Thread(target=_seed_background, daemon=True).start()

    server = http.server.HTTPServer(("0.0.0.0", port), Handler)
    print(f"\n  TenementWatch API v0.1.0")
    print(f"  http://0.0.0.0:{port} — API")
    print(f"  http://0.0.0.0:{port}/landing — Landing Page")
    print(f"  POST /refresh/wa | /refresh/qld — Refresh data\n")

    def shutdown(sig, frame):
        print("\n[shutdown]")
        server.shutdown()
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
