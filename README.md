# TenementWatch API

Live Australian mining tenement data (WA + QLD) as a simple REST API.
Normalized from government sources with change detection.

## Run locally

```bash
pip install -r requirements.txt
python server.py --refresh          # fetch WA + QLD into SQLite
python server.py --port 8000        # serve
```

Endpoints: `GET /tenements`, `GET /tenements/{uid}`, `GET /changes`, `POST /refresh/{state}`, `GET /health`, `GET /landing`.

API keys: `python mint_key.py "name" [free|starter|pro]`.
Auth via `X-API-Key` header. Tiers: free 100/day, starter 1000/day, pro 10000/day.

## Deploy (Render)

- Runtime: Python 3
- Build: `pip install -r requirements.txt`
- Start: `python server.py --seed-if-empty`
- Env: `PORT` is set automatically by Render.

Required env vars (Render dashboard → Environment → Add Environment Variable):
- `BOOTSTRAP_API_KEY` — a fixed API key (pro tier) that survives redeploys.
  Without at least one key the API is unusable, since SQLite is wiped on deploy.
- `ADMIN_KEY` — master key for the `POST /keys` mint endpoint.

Mint keys remotely (no Shell needed):
```bash
curl -X POST https://<host>/keys \
  -H "X-Admin-Key: $ADMIN_KEY" -H "Content-Type: application/json" \
  -d '{"name":"acme","tier":"starter"}'
```

Data is stored in `tenements.db` (SQLite). On Render's free tier the disk is
ephemeral, so `--seed-if-empty` re-fetches on cold boot. The `changes` table
(change history) is also reset on every redeploy.
