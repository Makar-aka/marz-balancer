# marz_balancer.py
import os
import time
import asyncio
import subprocess
import re
from typing import Dict, Any, Optional, List
from contextlib import asynccontextmanager

import aiohttp
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

load_dotenv()

MARZBAN_URL = os.getenv("MARZBAN_URL", "").rstrip("/")
MARZBAN_ADMIN_USER = os.getenv("MARZBAN_ADMIN_USER", "")
MARZBAN_ADMIN_PASS = os.getenv("MARZBAN_ADMIN_PASS", "")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5"))
APP_PORT = int(os.getenv("APP_PORT", "8023"))
IP_AGENT_PORT = os.getenv("IP_AGENT_PORT", "").strip()
# схема для ip_agent (по умолчанию http)
IP_AGENT_SCHEME = os.getenv("IP_AGENT_SCHEME", "http").strip()

# port to monitor for unique client IPs
MONITOR_PORT = int(os.getenv("MONITOR_PORT", "8443"))

# candidate node endpoints to try for live clients
# Оставляем только необходимые пути — приоритет: /connections (ip_agent), затем запасные /clients и /status
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
        # если в адресе нет порта — добавляем api_port или глобальный IP_AGENT_PORT
        if ":" not in addr.split("://", 1)[1]:
            if api_port:
                return f"{addr.rstrip('/')}:{api_port}"
            if IP_AGENT_PORT:
                return f"{addr.rstrip('/')}:{IP_AGENT_PORT}"
        return addr.rstrip("/")
    # addr без схемы: используем api_port, иначе fallback на IP_AGENT_PORT
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
    """
    Нормализует разные форматы ответа ноды в единый {'count': int, 'clients': list}.
    Поддерживает стандартные ключи clients/connections/peers и новый формат с 'ips'.
    """
    clients: List[Any] = []
    count = 0
    if isinstance(data, list):
        clients = data
        count = len(clients)
        return {"count": count, "clients": clients}
    if isinstance(data, dict):
        # first, handle explicit "ips" microservice format
        if "ips" in data and isinstance(data["ips"], list):
            clients = data["ips"]
            count = int(data.get("count", len(clients)))
            return {"count": count, "clients": clients, "port": data.get("port"), "meta": {k: data.get(k) for k in ("count_ipv4_enabled", "count_ipv6_enabled", "trusted_ips_configured") if k in data}}
        # common keys for list of clients
        for key in ("clients", "connections", "peers", "addresses"):
            if key in data and isinstance(data[key], list):
                clients = data[key]
                count = len(clients)
                return {"count": count, "clients": clients}
        # if node returns only count number
        if "count" in data and isinstance(data["count"], int):
            return {"count": data["count"], "clients": []}
        # fallback: find any list value
        for k, v in data.items():
            if isinstance(v, list):
                clients = v
                count = len(v)
                return {"count": count, "clients": clients}
    return {"count": count, "clients": clients}


def _build_ip_agent_base(node: Dict[str, Any]) -> Optional[str]:
    """
    Построить базовый URL до ip_agent для конкретной ноды.
    Приоритет:
     - если node.address содержит схему, использовать её и IP_AGENT_PORT (если указан)
     - иначе использовать IP_AGENT_SCHEME и IP_AGENT_PORT
    Возвращает строку типа "http://host:port" или None при отсутствии адреса.
    """
    addr = node.get("address") or node.get("name") or ""
    if not addr:
        return None
    # если адрес содержит схему, используем её
    if addr.startswith("http://") or addr.startswith("https://"):
        # если в адресе нет порта — добавить IP_AGENT_PORT если есть
        host_part = addr.rstrip("/")
        if ":" not in host_part.split("://", 1)[1] and IP_AGENT_PORT:
            return f"{host_part}:{IP_AGENT_PORT}"
        return host_part
    # без схемы
    port = IP_AGENT_PORT or node.get("api_port")
    scheme = IP_AGENT_SCHEME or "http"
    if port:
        return f"{scheme}://{addr}:{port}"
    return f"{scheme}://{addr}"


async def fetch_node_clients(session: aiohttp.ClientSession, node: Dict[str, Any]) -> Dict[str, Any]:
    """
    Сначала явно пробует ip_agent /connections по предопределённому порту (IP_AGENT_PORT),
    затем — fallback на стандартную логику перебора NODE_CANDIDATE_PATHS.
    """
    result = {"count": 0, "clients": [], "detected_path": None, "error": None}
    base_ip_agent = _build_ip_agent_base(node)
    # попытка напрямую опросить ip_agent /connections первым
    if base_ip_agent:
        res = await _try_node_path(session, base_ip_agent, "/connections", timeout_s=5)
        if res is not None and not (isinstance(res, dict) and res.get("error")):
            norm = _normalize_node_response(res)
            if norm.get("count", 0) > 0 or len(norm.get("clients", [])) > 0 or isinstance(res, (list, dict)):
                # attach meta/port if present
                if isinstance(norm, dict) and "meta" in norm:
                    result["meta"] = norm["meta"]
                if isinstance(norm, dict) and "port" in norm:
                    result["port"] = norm["port"]
                result.update({"count": norm.get("count", 0), "clients": norm.get("clients", []), "detected_path": f"{base_ip_agent}/connections"})
                return result
    # fallback: старая логика с перебором путей
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


# ------------------ new: unique connections counter (fallback) ------------------
def _parse_ss_output_for_remote_ips(output: str) -> List[str]:
    ips = set()
    # each useful line contains Local Address:Port  Peer Address:Port
    for line in output.splitlines():
        line = line.strip()
        if not line or line.lower().startswith("netid") or line.lower().startswith("state"):
            continue
        # peer is usually last column
        parts = re.split(r"\s+", line)
        if len(parts) < 1:
            continue
        peer = parts[-1]
        # strip possible brackets for IPv6 and port
        # match ending :port
        m = re.match(r"^\[?([^\]]+?)\]?:(\d+)$", peer)
        if m:
            ip = m.group(1)
            ips.add(ip)
           