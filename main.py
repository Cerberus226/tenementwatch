"""
TenementWatch API — Multi-state Australian mining tenement data API.
Data sources: WA DMIRS (ArcGIS MapServer), QLD Resources (ArcGIS REST).
"""
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from contextlib import asynccontextmanager
import sqlite3
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
import asyncio
import httpx

DB_PATH = os.path.join(os.path.dirname(__file__), "tenements.db")

# ── Data sources ──────────────────────────────────────────────────

WA_MAPSERVER = "https://public-services.slip.wa.gov.au/public/rest/services/SLIP_Public_Services/Industry_and_Mining/MapServer/3"
QLD_MAPSERVER = "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/Economy/MineralTenement/MapServer/0"

STATES = {
    "wa": {"name": "Western Australia", "url": WA_MAPSERVER, "id_field": "tenid"},
    "qld": {"name": "Queensland", "url": QLD_MAPSERVER, "id_field": "tenid"},
}

# ── DB setup ──────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tenements (
            id TEXT PRIMARY KEY,
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
            raw_json TEXT,
            first_seen TEXT NOT NULL,
            last_updated TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS status_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenement_id TEXT NOT NULL,
            state TEXT NOT NULL,
            old_status TEXT,
            new_status TEXT NOT NULL,
            changed_at TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tenements_state ON tenements(state)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tenements_status ON tenements(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tenements_holder ON tenements(holder)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tenements_commodity ON tenements(commodity)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_changes_tenement ON status_changes(tenement_id)")
    conn.commit()
    conn.close()

# ── Scraper ───────────────────────────────────────────────────────

async def fetch_tenements(state_code: str) -> list[dict]:
    """Fetch all tenements for a state and normalize them."""
    state = STATES[state_code]
    url = f"{state['url']}/query"
    params = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": 10000,  # WA has ~30k, need pagination in prod
    }

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

    features = data.get("features", [])
    if not features:
        return []

    results = []
    now = datetime.now(timezone.utc).isoformat()

    if state_code == "wa":
        return _normalize_wa(features, now)
    elif state_code == "qld":
        return _normalize_qld(features, now)

    return results

def _normalize_wa(features: list, now: str) -> list[dict]:
    results = []
    for f in features:
        attrs = f.get("attributes", {})
        tenid = attrs.get("tenid", "").strip()
        if not tenid:
            continue

        results.append({
            "id": f"wa:{tenid}",
            "state": "wa",
            "tenement_id": tenid,
            "name": attrs.get("fmt_tenid", tenid),
            "type": attrs.get("type", ""),
            "status": attrs.get("tenstatus", ""),
            "holder": attrs.get("holder1", ""),
            "commodity": None,  # WA doesn't include commodity in MapServer
            "area_ha": attrs.get("legal_area"),
            "grant_date": _parse_date(attrs.get("grantdate")),
            "expire_date": _parse_date(attrs.get("enddate")),
            "application_date": _parse_date(attrs.get("startdate")),
            "raw_json": json.dumps(attrs),
            "first_seen": now,
            "last_updated": now,
        })
    return results

def _normalize_qld(features: list, now: str) -> list[dict]:
    results = []
    for f in features:
        attrs = f.get("attributes", {})
        tenid = attrs.get("tenid", "").strip()
        if not tenid:
            continue

        results.append({
            "id": f"qld:{tenid}",
            "state": "qld",
            "tenement_id": tenid,
            "name": attrs.get("tenname", tenid),
            "type": attrs.get("tentype", ""),
            "status": attrs.get("tenstatus", ""),
            "holder": attrs.get("tenowner", ""),
            "commodity": attrs.get("tenmineral", ""),
            "area_ha": _safe_float(attrs.get("shapearea")),
            "grant_date": _parse_date(attrs.get("grantdate")),
            "expire_date": _parse_date(attrs.get("expiredate")),
            "application_date": _parse_date(attrs.get("appdate")),
            "raw_json": json.dumps(attrs),
            "first_seen": now,
            "last_updated": now,
        })
    return results

def _parse_date(val) -> Optional[str]:
    if not val or str(val).strip() in ("", "0", "None"):
        return None
    val = str(val).strip()
    # Handle Unix timestamps in milliseconds
    if val.isdigit() and len(val) >= 10:
        try:
            ts = int(val) / 1000 if len(val) > 10 else int(val)
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        except (ValueError, OSError):
            pass
    # Handle common date formats
    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y%m%d", "%Y-%m-%dT%H:%M:%S"]:
        try:
            return datetime.strptime(val[:10], "%Y-%m-%d" if "-" in val else "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            continue
    return val[:10] if len(val) >= 10 else val

def _safe_float(val) -> Optional[float]:
    try:
        return float(val) if val not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None

async def refresh_state(state_code: str):
    """Fetch and upsert tenements, recording status changes."""
    conn = get_db()
    tenements = await fetch_tenements(state_code)
    changes = 0

    for t in tenements:
        # Check existing
        existing = conn.execute(
            "SELECT status FROM tenements WHERE id = ?", (t["id"],)
        ).fetchone()

        if existing:
            old_status = existing["status"]
            if old_status and old_status != t["status"]:
                conn.execute(
                    "INSERT INTO status_changes (tenement_id, state, old_status, new_status, changed_at) VALUES (?, ?, ?, ?, ?)",
                    (t["tenement_id"], t["state"], old_status, t["status"], t["last_updated"]),
                )
                changes += 1
            conn.execute("""
                UPDATE tenements SET name=?, type=?, status=?, holder=?, commodity=?,
                    area_ha=?, grant_date=?, expire_date=?, application_date=?,
                    raw_json=?, last_updated=?
                WHERE id=?
            """, (t["name"], t["type"], t["status"], t["holder"], t["commodity"],
                  t["area_ha"], t["grant_date"], t["expire_date"], t["application_date"],
                  t["raw_json"], t["last_updated"], t["id"]))
        else:
            conn.execute("""
                INSERT INTO tenements (id, state, tenement_id, name, type, status, holder,
                    commodity, area_ha, grant_date, expire_date, application_date,
                    raw_json, first_seen, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (t["id"], t["state"], t["tenement_id"], t["name"], t["type"],
                  t["status"], t["holder"], t["commodity"], t["area_ha"],
                  t["grant_date"], t["expire_date"], t["application_date"],
                  t["raw_json"], t["first_seen"], t["last_updated"]))

    conn.commit()
    conn.close()
    return {"state": state_code, "tenements": len(tenements), "status_changes": changes}

# ── App ──────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(
    title="TenementWatch API",
    description="Multi-state Australian mining tenement data API. WA + QLD live.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def root():
    conn = get_db()
    wa_count = conn.execute("SELECT COUNT(*) FROM tenements WHERE state='wa'").fetchone()[0]
    qld_count = conn.execute("SELECT COUNT(*) FROM tenements WHERE state='qld'").fetchone()[0]
    conn.close()
    return {
        "name": "TenementWatch API",
        "version": "0.1.0",
        "states": {
            "wa": {"name": "Western Australia", "tenements": wa_count},
            "qld": {"name": "Queensland", "tenements": qld_count},
        },
        "endpoints": ["/tenements", "/tenements/{id}", "/changes", "/refresh/{state}"],
    }

@app.get("/tenements")
async def list_tenements(
    state: Optional[str] = Query(None, description="Filter by state: wa, qld"),
    status: Optional[str] = Query(None, description="Filter by status: LIVE, PENDING, etc"),
    holder: Optional[str] = Query(None, description="Filter by holder/company name (partial match)"),
    commodity: Optional[str] = Query(None, description="Filter by commodity: gold, copper, lithium, etc"),
    granted_since: Optional[str] = Query(None, description="Granted since date (YYYY-MM-DD)"),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Search tenements across states."""
    conn = get_db()
    where = []
    params = []

    if state:
        where.append("state = ?")
        params.append(state.lower())
    if status:
        where.append("status = ?")
        params.append(status.upper())
    if holder:
        where.append("holder LIKE ?")
        params.append(f"%{holder}%")
    if commodity:
        where.append("commodity LIKE ?")
        params.append(f"%{commodity}%")
    if granted_since:
        where.append("grant_date >= ?")
        params.append(granted_since)

    where_clause = " AND ".join(where) if where else "1=1"

    rows = conn.execute(
        f"SELECT * FROM tenements WHERE {where_clause} ORDER BY last_updated DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    count = conn.execute(
        f"SELECT COUNT(*) FROM tenements WHERE {where_clause}", params
    ).fetchone()[0]

    conn.close()
    return {
        "count": count,
        "limit": limit,
        "offset": offset,
        "results": [dict(r) for r in rows],
    }

@app.get("/tenements/{tenement_id}")
async def get_tenement(tenement_id: str):
    """Get a single tenement by ID."""
    conn = get_db()
    row = conn.execute("SELECT * FROM tenements WHERE id = ?", (tenement_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Tenement not found")
    changes = conn.execute(
        "SELECT * FROM status_changes WHERE tenement_id = ? ORDER BY changed_at DESC",
        (dict(row)["tenement_id"],),
    ).fetchall()
    conn.close()
    return {"tenement": dict(row), "status_history": [dict(c) for c in changes]}

@app.get("/changes")
async def list_changes(
    state: Optional[str] = Query(None),
    since: Optional[str] = Query(None, description="Changes since date (YYYY-MM-DD)"),
    limit: int = Query(50, ge=1, le=500),
):
    """Get recent status changes."""
    conn = get_db()
    where = []
    params = []
    if state:
        where.append("state = ?")
        params.append(state.lower())
    if since:
        where.append("changed_at >= ?")
        params.append(since)
    where_clause = " AND ".join(where) if where else "1=1"

    rows = conn.execute(
        f"SELECT * FROM status_changes WHERE {where_clause} ORDER BY changed_at DESC LIMIT ?",
        params + [limit],
    ).fetchall()
    conn.close()
    return {"count": len(rows), "results": [dict(r) for r in rows]}

@app.post("/refresh/{state}")
async def trigger_refresh(state: str):
    """Manually trigger a data refresh for a state."""
    if state not in STATES:
        raise HTTPException(status_code=400, detail=f"Unknown state: {state}. Use: {list(STATES.keys())}")
    result = await refresh_state(state)
    return result

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.get("/landing", response_class=HTMLResponse)
async def landing():
    return HTMLResponse(LANDING_HTML)

# ── Landing Page ─────────────────────────────────────────────────

LANDING_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TenementWatch API — Australian Mining Tenement Data</title>
<style>
    :root { --bg: #0a0a0f; --card: #12121a; --gold: #bdb970; --text: #c8c8d0; --muted: #6b6b78; --border: #1e1e2a; }
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; line-height: 1.6; }
    .container { max-width: 900px; margin: 0 auto; padding: 2rem; }
    header { padding: 4rem 0 2rem; text-align: center; }
    h1 { font-size: 2.5rem; color: #fff; margin-bottom: 0.5rem; }
    h1 span { color: var(--gold); }
    .subtitle { color: var(--muted); font-size: 1.1rem; margin-bottom: 2rem; }
    .cta { display: inline-block; background: var(--gold); color: #0a0a0f; padding: 0.75rem 2rem; border-radius: 6px; font-weight: 600; text-decoration: none; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 1.5rem; margin: 1.5rem 0; }
    .card h3 { color: #fff; margin-bottom: 0.75rem; }
    pre { background: #08080f; border: 1px solid var(--border); border-radius: 6px; padding: 1rem; overflow-x: auto; font-size: 0.85rem; }
    code { font-family: 'JetBrains Mono', 'Fira Code', monospace; }
    .endpoint { color: var(--gold); }
    .method { color: #7ecb8a; font-weight: 600; }
    .pricing-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 1rem; margin: 1.5rem 0; }
    .plan { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 1.5rem; text-align: center; }
    .plan.featured { border-color: var(--gold); }
    .plan h3 { color: #fff; }
    .plan .price { font-size: 2rem; color: var(--gold); font-weight: 700; }
    .plan .period { color: var(--muted); }
    .plan ul { list-style: none; text-align: left; margin: 1rem 0; }
    .plan li { padding: 0.25rem 0; }
    .plan li::before { content: '✓ '; color: var(--gold); }
    footer { text-align: center; color: var(--muted); padding: 2rem 0; font-size: 0.85rem; }
</style>
</head>
<body>
<div class="container">
<header>
    <h1>Tenement<span>Watch</span> API</h1>
    <p class="subtitle">Live Australian mining tenement data — WA + QLD.<br>Simple REST API. No maps. Just JSON.</p>
    <a href="#try" class="cta">Try It Free</a>
</header>

<div class="card">
    <h3>Why TenementWatch?</h3>
    <p>Government tenement data is public but scattered across state portals in different formats. TenementWatch normalizes it into a single REST API. Track new applications, grants, surrenders, and expiries across WA and QLD — <strong>before it hits the news</strong>.</p>
</div>

<h3 style="color:#fff;margin-top:2rem;">Pricing</h3>
<div class="pricing-grid">
    <div class="plan">
        <h3>Free</h3>
        <div class="price">$0</div>
        <div class="period">/month</div>
        <ul>
            <li>100 calls/day</li>
            <li>7-day lookback</li>
            <li>WA + QLD</li>
            <li>Community support</li>
        </ul>
    </div>
    <div class="plan featured">
        <h3>Starter</h3>
        <div class="price">$99</div>
        <div class="period">/month</div>
        <ul>
            <li>1,000 calls/day</li>
            <li>30-day lookback</li>
            <li>WA + QLD</li>
            <li>3 webhooks</li>
            <li>Email support</li>
        </ul>
    </div>
    <div class="plan">
        <h3>Pro</h3>
        <div class="price">$299</div>
        <div class="period">/month</div>
        <ul>
            <li>10,000 calls/day</li>
            <li>Full history</li>
            <li>All states (coming soon)</li>
            <li>20 webhooks</li>
            <li>Priority support</li>
        </ul>
    </div>
</div>

<div class="card" id="try">
    <h3>Quick Start</h3>
    <p style="margin-bottom:0.75rem;"><span class="method">GET</span> <span class="endpoint">/tenements?state=wa&status=LIVE&commodity=gold&granted_since=2026-01-01</span></p>
    <pre><code>curl "http://localhost:8000/tenements?state=qld&holder=rio&limit=5" | python -m json.tool</code></pre>
    <p style="margin-top:0.75rem;">API key authentication coming soon. For now, the API is open during beta.</p>
</div>

<div class="card">
    <h3>API Endpoints</h3>
    <pre><code><span class="method">GET</span>  <span class="endpoint">/tenements</span>          — Search tenements (state, status, holder, commodity, granted_since)
<span class="method">GET</span>  <span class="endpoint">/tenements/{id}</span>     — Single tenement + status history
<span class="method">GET</span>  <span class="endpoint">/changes</span>             — Recent status changes
<span class="method">POST</span> <span class="endpoint">/refresh/{state}</span>    — Trigger data refresh (wa, qld)
<span class="method">GET</span>  <span class="endpoint">/health</span>              — Health check</code></pre>
</div>

<footer>
    TenementWatch API · Public data from WA DMIRS & QLD Resources · CC-BY 4.0<br>
    Built in Newcastle, Australia
</footer>
</div>
</body>
</html>"""
