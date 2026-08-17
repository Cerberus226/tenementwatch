#!/usr/bin/env python3
"""Mint an API key for TenementWatch.

Usage: python mint_key.py <name> [tier]
  tier: free (default) | starter | pro
"""
import secrets, sqlite3, sys, os
from datetime import datetime, timezone

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tenements.db")

def main():
    if len(sys.argv) < 2:
        print('usage: mint_key.py <name> [free|starter|pro]')
        sys.exit(2)
    name = sys.argv[1]
    tier = sys.argv[2] if len(sys.argv) > 2 else "free"
    if tier not in ("free", "starter", "pro"):
        print("tier must be free|starter|pro")
        sys.exit(2)

    key = "tw_" + secrets.token_urlsafe(32)
    con = sqlite3.connect(DB)
    con.execute("INSERT INTO api_keys(key, name, tier, created_at) VALUES(?,?,?,?)",
                (key, name, tier, datetime.now(timezone.utc).isoformat()))
    con.commit()
    con.close()
    print(f"name: {name}")
    print(f"tier: {tier}")
    print(f"key:  {key}")
    print()
    print(f"curl -H 'X-API-Key: {key}' http://127.0.0.1:8000/tenements?state=wa&limit=5")

if __name__ == "__main__":
    main()
