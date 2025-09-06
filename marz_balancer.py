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

# candidate node endpoints to try for live clients
NODE_CANDIDATE_PATHS = [
    "/api/clients",
    "/clients",
    "/api/v1/clients",
    "/v1/clients",
    "/api/connections",
    "/connections",
    "/api/peers",
    "/peers",
    "/status",
    "/stats",
]

# runtime state
stats: Dict[str, Any] = {
    "nodes": [],
    "last_update": None,
    "error": None,
    "system": None,
    "nodes_usage": None,
    "users_usage": None,
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
        if api_port and ":" not in addr.split("://", 1)[1]:
            return f"{addr.rstrip('/')}:{api_port}"
        return addr.rstrip("/")
    if api_port:
        return f"http://{addr}:{api_port}"
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
    elif isinstance(data, dict):
        for key in ("clients", "connections", "peers"):
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


async def fetch_node_clients(session: aiohttp.ClientSession, node: Dict[str, Any]) -> Dict[str, Any]:
    base = _build_node_base(node)
    result = {"count": 0, "clients": [], "detected_path": None, "error": None}
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
        if norm["count"] > 0 or len(norm["clients"]) > 0 or isinstance(res, (list, dict)):
            result.update({"count": norm["count"], "clients": norm["clients"], "detected_path": p})
            return result

    result["error"] = "no usable endpoint"
    return result


async def poll_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                token = await _fetch_token(session)

                # parallel master calls: nodes, system, nodes_usage, users_usage
                tasks_master = [
                    _fetch_nodes(session, token),
                    _fetch_system(session, token),
                    _fetch_nodes_usage(session, token),
                    _fetch_users_usage(session, token),
                ]
                nodes, system_stat, nodes_usage, users_usage = await asyncio.gather(*tasks_master)

                # store master-level data
                stats["system"] = system_stat
                stats["nodes_usage"] = nodes_usage
                stats["users_usage"] = users_usage

                if nodes is None:
                    stats["error"] = "failed to fetch nodes"
                    stats["nodes"] = []
                    stats["last_update"] = time.time()
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                # build base nodes info
                node_entries: List[Dict[str, Any]] = []
                for n in nodes:
                    entry = {
                        "id": n.get("id"),
                        "name": n.get("name"),
                        "address": n.get("address"),
                        "api_port": n.get("api_port"),
                        "status": n.get("status"),
                        "message": n.get("message"),
                        # placeholders for clients data
                        "clients_count": None,
                        "clients": [],
                        "detected_path": None,
                        "clients_error": None,
                        # usage fields (from nodes_usage)
                        "uplink": None,
                        "downlink": None,
                    }
                    node_entries.append(entry)

                # attach nodes_usage (uplink/downlink) if available
                if nodes_usage and isinstance(nodes_usage, dict):
                    usages = nodes_usage.get("usages") or []
                    # usages: list of {node_id,node_name,uplink,downlink}
                    for entry in node_entries:
                        # try match by id then by name
                        for u in usages:
                            if (entry["id"] is not None and u.get("node_id") == entry["id"]) or (u.get("node_name") and u.get("node_name") == entry.get("name")):
                                entry["uplink"] = u.get("uplink")
                                entry["downlink"] = u.get("downlink")
                                break

                # concurrently query node APIs for live clients
                tasks = [fetch_node_clients(session, n) for n in nodes]
                clients_results = await asyncio.gather(*tasks, return_exceptions=True)

                # attach results to node_entries (match by index)
                for i, res in enumerate(clients_results):
                    if isinstance(res, Exception):
                        node_entries[i]["clients_error"] = str(res)
                        node_entries[i]["clients_count"] = None
                        continue
                    node_entries[i]["clients_count"] = res.get("count", 0)
                    node_entries[i]["clients"] = res.get("clients", [])
                    node_entries[i]["detected_path"] = res.get("detected_path")
                    node_entries[i]["clients_error"] = res.get("error")

                stats["nodes"] = node_entries
                stats["error"] = None
                stats["last_update"] = time.time()
            except Exception as ex:
                stats["error"] = str(ex)
                stats["nodes"] = []
                stats["last_update"] = time.time()
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
    nodes = stats.get("nodes", [])
    last = stats.get("last_update")
    err = stats.get("error")
    system = stats.get("system")
    last_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last)) if last else "—"

    header = f"<div>Последнее обновление: {last_str}</div>"
    if system:
        header += f"<div>Online users (master): {system.get('online_users', '—')}</div>"
        header += f"<div>Incoming bandwidth: {system.get('incoming_bandwidth', '—')}</div>"
        header += f"<div>Outgoing bandwidth: {system.get('outgoing_bandwidth', '—')}</div>"

    items = ""
    for n in nodes:
        items += "<div style='border:1px solid #ddd;padding:8px;margin:6px;'>"
        items += f"<b>{n.get('name') or n.get('address')}</b><br/>"
        items += f"ID: {n.get('id') or '—'}<br/>"
        items += f"Address: {n.get('address') or '—'}<br/>"
        items += f"API port: {n.get('api_port') or '—'}<br/>"
        items += f"Status: {n.get('status') or '—'}<br/>"
        if n.get("message"):
            items += f"Message: {n.get('message')}<br/>"
        items += f"Clients: {n.get('clients_count') if n.get('clients_count') is not None else '—'}<br/>"