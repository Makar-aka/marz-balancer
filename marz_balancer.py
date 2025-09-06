import os
import time
import asyncio
from typing import Dict, Any, Optional, List

import aiohttp
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

load_dotenv()

APP = FastAPI()

MARZBAN_URL = os.getenv("MARZBAN_URL", "").rstrip("/")
MARZBAN_ADMIN_USER = os.getenv("MARZBAN_ADMIN_USER", "")
MARZBAN_ADMIN_PASS = os.getenv("MARZBAN_ADMIN_PASS", "")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5"))
APP_PORT = int(os.getenv("APP_PORT", "8023"))

# runtime state
stats: Dict[str, Any] = {"nodes": [], "last_update": None, "error": None}
_token_cache: Dict[str, Any] = {"token": None, "fetched_at": 0, "ttl": 300}


async def _fetch_token(session: aiohttp.ClientSession) -> Optional[str]:
    """
    Получает и кэширует admin token от Marzban.
    POST /api/admin/token with form data: username, password
    Ответ: {"access_token": "..."}
    """
    if not MARZBAN_URL or not MARZBAN_ADMIN_USER or not MARZBAN_ADMIN_PASS:
        return None
    now = time.time()
    if _token_cache["token"] and now - _token_cache["fetched_at"] < _token_cache["ttl"]:
        return _token_cache["token"]
    url = f"{MARZBAN_URL}/api/admin/token"
    data = {"username": MARZBAN_ADMIN_USER, "password": MARZBAN_ADMIN_PASS}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        async with session.post(url, data=data, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"token request failed: {resp.status} {text}")
            j = await resp.json()
            token = j.get("access_token") or j.get("token")
            if token:
                _token_cache["token"] = token
                _token_cache["fetched_at"] = now
                return token
    except Exception:
        return None
    return None


async def _fetch_nodes(session: aiohttp.ClientSession, token: Optional[str]) -> Optional[List[Dict[str, Any]]]:
    if not MARZBAN_URL:
        return None
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    url = f"{MARZBAN_URL}/api/nodes"
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


async def poll_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                token = await _fetch_token(session)
                nodes = await _fetch_nodes(session, token)
                if nodes is None:
                    stats["error"] = "failed to fetch nodes"
                    stats["nodes"] = []
                else:
                    # nodes expected as list of NodeResponse objects from OpenAPI
                    stats["nodes"] = [
                        {
                            "id": n.get("id"),
                            "name": n.get("name"),
                            "address": n.get("address"),
                            "api_port": n.get("api_port"),
                            "status": n.get("status"),
                            "message": n.get("message"),
                        }
                        for n in nodes
                    ]
                    stats["error"] = None
                stats["last_update"] = time.time()
            except Exception as ex:
                stats["error"] = str(ex)
            await asyncio.sleep(POLL_INTERVAL)


@APP.on_event("startup")
async def startup():
    if not MARZBAN_URL:
        stats["error"] = "MARZBAN_URL not configured"
    asyncio.create_task(poll_loop())


@APP.get("/api/stats")
async def api_stats():
    return JSONResponse(content=stats)


@APP.get("/", response_class=HTMLResponse)
async def index():
    # simple UI: lists nodes and statuses
    nodes = stats.get("nodes", [])
    last = stats.get("last_update")
    err = stats.get("error")
    last_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last)) if last else "—"
    items = ""
    for n in nodes:
        items += f"<div style='border:1px solid #ddd;padding:8px;margin:6px;'><b>{n.get('name') or n.get('address')}</b><br/>"
        items += f"ID: {n.get('id') or '—'}<br/>"
        items += f"Address: {n.get('address') or '—'}<br/>"
        items += f"API port: {n.get('api_port') or '—'}<br/>"
        items += f"Status: {n.get('status') or '—'}<br/>"
        if n.get("message"):
            items += f"Message: {n.get('message')}<br/>"
        items += "</div>"
    if not items:
        items = "<div>Ноды не обнаружены.</div>"
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Marzban nodes</title></head><body>
<h1>Marzban — ноды</h1>
<div>Последнее обновление: {last_str}</div>
<div style="color:#b00">{err or ''}</div>
<div>{items}</div>
<script>
setTimeout(()=>location.reload(), {int(POLL_INTERVAL*1000)});
</script>
</body></html>"""
    return HTMLResponse(content=html)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("marz_balancer:APP", host="0.0.0.0", port=APP_PORT, reload=True)