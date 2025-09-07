import os
import time
import asyncio
import subprocess
import re
from typing import Dict, Any, Optional, List
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import aiohttp
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

load_dotenv()

MARZBAN_URL = os.getenv("MARZBAN_URL", "").rstrip("/")
MARZBAN_ADMIN_USER = os.getenv("MARZBAN_ADMIN_USER", "")
MARZBAN_ADMIN_PASS = os.getenv("MARZBAN_ADMIN_PASS", "")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5"))
APP_PORT = int(os.getenv("APP_PORT", "8023"))
IP_AGENT_PORT = os.getenv("IP_AGENT_PORT", "").strip()
IP_AGENT_SCHEME = os.getenv("IP_AGENT_SCHEME", "http").strip()

MONITOR_PORT = int(os.getenv("MONITOR_PORT", "8443"))

NODE_CANDIDATE_PATHS = [
    "/connections",
    "/clients",
    "/status",
]

# runtime state
stats: Dict[str, Any] = {
    "nodes": [],
    "last_update": None,
    "error": None,
    "system": None,
    "nodes_usage": None,
    "users_usage": None,
    "port_8443": {"unique_clients": 0, "clients": []},
}
_token_cache: Dict[str, Any] = {"token": None, "fetched_at": 0, "ttl": 300}

async def _fetch_token(session: aiohttp.ClientSession) -> Optional[str]:
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
                return None
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

async def _fetch_system(session: aiohttp.ClientSession, token: Optional[str]) -> Optional[Dict[str, Any]]:
    if not MARZBAN_URL:
        return None
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    url = f"{MARZBAN_URL}/api/system"
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None

async def _fetch_nodes_usage(session: aiohttp.ClientSession, token: Optional[str], start: Optional[str] = None, end: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not MARZBAN_URL:
        return None
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    url = f"{MARZBAN_URL}/api/nodes/usage"
    try:
        async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None

async def _fetch_users_usage(session: aiohttp.ClientSession, token: Optional[str], start: Optional[str] = None, end: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    if not MARZBAN_URL:
        return None
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    url = f"{MARZBAN_URL}/api/users/usage"
    try:
        async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None

def _build_node_base(node: Dict[str, Any]) -> str:
    addr = node.get("address") or node.get("name") or ""
    api_port = node.get("api_port")
    if not addr:
        return ""
    if addr.startswith("http://") or addr.startswith("https://"):
        if ":" not in addr.split("://", 1)[1]:
            if api_port:
                return f"{addr.rstrip('/')}:{api_port}"
            if IP_AGENT_PORT:
                return f"{addr.rstrip('/')}:{IP_AGENT_PORT}"
        return addr.rstrip("/")
    if api_port:
        return f"http://{addr}:{api_port}"
    if IP_AGENT_PORT:
        return f"http://{addr}:{IP_AGENT_PORT}"
    return f"http://{addr}"

async def _try_node_path(session: aiohttp.ClientSession, base: str, path: str, timeout_s: int = 5) -> Optional[Any]:
    url = f"{base.rstrip('/')}{path}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
            if resp.status != 200:
                return {"error": f"{resp.status} {resp.reason}"}
            try:
                return await resp.json()
            except Exception:
                text = await resp.text()
                return {"raw": text}
    except Exception as ex:
        return {"error": str(ex)}

def _normalize_node_response(data: Any) -> Dict[str, Any]:
    clients: List[Any] = []
    count = 0
    if isinstance(data, list):
        clients = data
        count = len(clients)
        return {"count": count, "clients": clients}
    if isinstance(data, dict):
        if "ips" in data and isinstance(data["ips"], list):
            clients = data["ips"]
            count = int(data.get("count", len(clients)))
            return {"count": count, "clients": clients, "port": data.get("port"), "meta": {k: data.get(k) for k in ("count_ipv4_enabled", "count_ipv6_enabled", "trusted_ips_configured") if k in data}}
        for key in ("clients", "connections", "peers", "addresses"):
            if key in data and isinstance(data[key], list):
                clients = data[key]
                count = len(clients)
                return {"count": count, "clients": clients}
        if "count" in data and isinstance(data["count"], int):
            return {"count": data["count"], "clients": []}
        for k, v in data.items():
            if isinstance(v, list):
                clients = v
                count = len(v)
                return {"count": count, "clients": clients}
    return {"count": count, "clients": clients}

def _build_ip_agent_base(node: Dict[str, Any]) -> Optional[str]:
    addr = node.get("address") or node.get("name") or ""
    if not addr:
        return None
    if addr.startswith("http://") or addr.startswith("https://"):
        host_part = addr.rstrip("/")
        if ":" not in host_part.split("://", 1)[1] and IP_AGENT_PORT:
            return f"{host_part}:{IP_AGENT_PORT}"
        return host_part
    port = IP_AGENT_PORT or node.get("api_port")
    scheme = IP_AGENT_SCHEME or "http"
    if port:
        return f"{scheme}://{addr}:{port}"
    return f"{scheme}://{addr}"

async def fetch_node_clients(session: aiohttp.ClientSession, node: Dict[str, Any]) -> Dict[str, Any]:
    result = {"count": 0, "clients": [], "detected_path": None, "error": None}
    base_ip_agent = _build_ip_agent_base(node)
    if base_ip_agent:
        res = await _try_node_path(session, base_ip_agent, "/connections", timeout_s=5)
        if res is not None and not (isinstance(res, dict) and res.get("error")):
            norm = _normalize_node_response(res)
            if norm.get("count", 0) > 0 or len(norm.get("clients", [])) > 0 or isinstance(res, (list, dict)):
                if isinstance(norm, dict) and "meta" in norm:
                    result["meta"] = norm["meta"]
                if isinstance(norm, dict) and "port" in norm:
                    result["port"] = norm["port"]
                result.update({"count": norm.get("count", 0), "clients": norm.get("clients", []), "detected_path": f"{base_ip_agent}/connections"})
                return result
    base = _build_node_base(node)
    if not base:
        result["error"] = "no base address"
        return result

    paths = []
    cfg_path = node.get("clients_path")
    if cfg_path:
        paths.append(cfg_path)
    for p in NODE_CANDIDATE_PATHS:
        if p not in paths:
            paths.append(p)

    for p in paths:
        res = await _try_node_path(session, base, p, timeout_s=5)
        if res is None:
            continue
        if isinstance(res, dict) and res.get("error"):
            continue
        norm = _normalize_node_response(res)
        if norm.get("count", 0) > 0 or len(norm.get("clients", [])) > 0 or isinstance(res, (list, dict)):
            if isinstance(norm, dict) and "meta" in norm:
                result["meta"] = norm["meta"]
            if isinstance(norm, dict) and "port" in norm:
                result["port"] = norm["port"]
            result.update({"count": norm.get("count", 0), "clients": norm.get("clients", []), "detected_path": p})
            return result

    result["error"] = "no usable endpoint"
    return result

def _parse_ss_output_for_remote_ips(output: str) -> List[str]:
    ips = set()
    for line in output.splitlines():
        line = line.strip()
        if not line or line.lower().startswith("netid") or line.lower().startswith("state"):
            continue
        parts = re.split(r"\s+", line)
        if len(parts) < 1:
            continue
        peer = parts[-1]
        m = re.match(r"^\[?([^\]]+?)\]?:(\d+)$", peer)
        if m:
            ip = m.group(1)
            ips.add(ip)

async def poll_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                token = await _fetch_token(session)
                tasks_master = [
                    _fetch_nodes(session, token),
                    _fetch_system(session, token),
                    _fetch_nodes_usage(session, token),
                    _fetch_users_usage(session, token),
                ]
                nodes, system_stat, nodes_usage, users_usage = await asyncio.gather(*tasks_master)
                stats["system"] = system_stat
                stats["nodes_usage"] = nodes_usage
                stats["users_usage"] = users_usage

                if nodes is None:
                    stats["error"] = "failed to fetch nodes"
                    stats["nodes"] = []
                    stats["last_update"] = time.time()
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                node_entries: List[Dict[str, Any]] = []
                for n in nodes:
                    entry = {
                        "id": n.get("id"),
                        "name": n.get("name"),
                        "address": n.get("address"),
                        "api_port": n.get("api_port"),
                        "status": n.get("status"),
                        "message": n.get("message"),
                        "clients_count": None,
                        "clients": [],
                        "detected_path": None,
                        "clients_error": None,
                        "uplink": None,
                        "downlink": None,
                    }
                    node_entries.append(entry)

                if nodes_usage and isinstance(nodes_usage, dict):
                    usages = nodes_usage.get("usages") or []
                    for entry in node_entries:
                        for u in usages:
                            if (entry["id"] is not None and u.get("node_id") == entry["id"]) or (u.get("node_name") and u.get("node_name") == entry.get("name")):
                                entry["uplink"] = u.get("uplink")
                                entry["downlink"] = u.get("downlink")
                                break

                tasks = [fetch_node_clients(session, n) for n in nodes]
                clients_results = await asyncio.gather(*tasks, return_exceptions=True)

                for i, res in enumerate(clients_results):
                    if isinstance(res, Exception):
                        node_entries[i]["clients_error"] = str(res)
                        node_entries[i]["clients_count"] = None
                        continue
                    node_entries[i]["clients_count"] = res.get("count", 0)
                    node_entries[i]["clients"] = res.get("clients", [])
                    node_entries[i]["detected_path"] = res.get("detected_path")
                    node_entries[i]["clients_error"] = res.get("error")
                    if res.get("port") is not None:
                        node_entries[i]["clients_port"] = res.get("port")
                    if res.get("meta") is not None:
                        node_entries[i]["clients_meta"] = res.get("meta")

                stats["nodes"] = node_entries

                try:
                    unique_ips = await asyncio.to_thread(get_unique_remote_ips, MONITOR_PORT)
                    stats["port_8443"] = {"unique_clients": len(unique_ips), "clients": unique_ips[:200]}
                except Exception:
                    stats["port_8443"] = {"unique_clients": 0, "clients": []}

                stats["error"] = None
                stats["last_update"] = time.time()
            except Exception as ex:
                stats["error"] = str(ex)
                stats["nodes"] = []
                stats["last_update"] = time.time()
            await asyncio.sleep(POLL_INTERVAL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    if not MARZBAN_URL:
        stats["error"] = "MARZBAN_URL not configured"
    task = asyncio.create_task(poll_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

def human_bytes(num: Optional[int]) -> str:
    if num is None:
        return "—"
    for unit in ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ', 'ПБ']:
        if abs(num) < 1024.0:
            if unit == 'Б':
                return f"{num} {unit}"
            return f"{num:.2f} {unit}"
        num /= 1024.0
    return f"{num:.2f} ЭБ"

def get_usage_range(period: str) -> tuple[Optional[str], Optional[str]]:
    now = datetime.utcnow()
    if period == "1d":
        start = (now - timedelta(days=1)).isoformat(timespec="seconds") + "Z"
    elif period == "1w":
        start = (now - timedelta(weeks=1)).isoformat(timespec="seconds") + "Z"
    elif period == "1m":
        start = (now - timedelta(days=30)).isoformat(timespec="seconds") + "Z"
    else:
        return (None, None)
    return (start, now.isoformat(timespec="seconds") + "Z")

APP = FastAPI(lifespan=lifespan)

@APP.get("/api/stats")
async def api_stats():
    return JSONResponse(content=stats)

@APP.get("/", response_class=HTMLResponse)
async def index(request: Request):
    nodes = stats.get("nodes", [])
    last = stats.get("last_update")
    err = stats.get("error")
    system = stats.get("system")
    port_info = stats.get("port_8443", {})
    last_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last)) if last else "—"

    # суммарное количество активных клиентов по всем нодам
    total_clients = sum(int(n.get('clients_count') or 0) for n in nodes)

    header = f"""
    <div class="mb-3 d-flex flex-wrap align-items-center">
        <span class="badge bg-secondary">Последнее обновление: {last_str}</span>
        <span class="badge bg-info text-dark ms-2">Подключений к порту {MONITOR_PORT}: {port_info.get('unique_clients', '—')}</span>
        <span class="badge bg-dark ms-2">Активных клиентов: {total_clients}</span>
    </div>
    """
    if system:
        header += f"""
        <div class="mb-3">
            <span class="badge bg-success">Online users (master): {system.get('online_users', '—')}</span>
            <span class="badge bg-primary ms-2">Incoming bandwidth: {human_bytes(system.get('incoming_bandwidth'))}</span>
            <span class="badge bg-primary ms-2">Outgoing bandwidth: {human_bytes(system.get('outgoing_bandwidth'))}</span>
        </div>
        """

    items = ""
    for n in nodes:
        items += f"""
        <div class="col">
            <div class="card shadow-sm mb-4">
                <div class="card-header bg-light">
                    <b>{n.get('name') or n.get('address')}</b>
                </div>
                <div class="card-body">
                    <ul class="list-group list-group-flush">
                        <li class="list-group-item"><b>Address:</b> {n.get('address') or '—'}</li>
                        <li class="list-group-item"><b>API port:</b> {n.get('api_port') or '—'}</li>
                        <li class="list-group-item"><b>Status:</b> {n.get('status') or '—'}</li>
                        <li class="list-group-item"><b>Clients:</b> {n.get('clients_count') if n.get('clients_count') is not None else '—'}</li>
                        <li class="list-group-item"><b>Uplink:</b> {human_bytes(n.get('uplink'))} <b>Downlink:</b> {human_bytes(n.get('downlink'))}</li>
                    </ul>
                    {"<div class='alert alert-danger mt-2'>Clients error: " + n.get('clients_error') + "</div>" if n.get('clients_error') else ""}
                </div>
            </div>
        </div>
        """
    if not items:
        items = "<div class='alert alert-warning'>Ноды не обнаружены.</div>"

    html = f"""<!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <title>Marzban nodes</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <!-- Bootstrap 5 CDN -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
<div class="container py-4">
    <h1 class="mb-4">Marzban — Ноды</h1>
    {header}
    <div style="color:#b00">{err or ''}</div>
    <div class="row row-cols-1 row-cols-md-2 row-cols-lg-3 g-4">
        {items}
    </div>
</div>

<!-- Копирайт в нижнем правом углу -->
<a href="https://github.com/Makar-aka/marz-balancer"
   target="_blank" rel="noopener noreferrer"
   class="text-decoration-none"
   style="position:fixed; right:12px; bottom:12px; z-index:9999; background:#212529; color:#fff; padding:6px 10px; border-radius:8px; font-size:12px; opacity:.85;">
   &copy; MakarSPB
</a>

<script>
setTimeout(()=>location.reload(), {int(POLL_INTERVAL*1000)});
</script>
</body>
</html>"""
    return HTMLResponse(content=html)
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("marz_balancer:APP", host="0.0.0.0", port=APP_PORT, reload=True)