import os
import time
from typing import Any, Dict, List, Optional

import aiohttp

MARZBAN_URL = os.getenv("MARZBAN_URL")  # пример: https://mrz.vpnza.ru
MARZBAN_USER = os.getenv("MARZBAN_ADMIN_USER")
MARZBAN_PASS = os.getenv("MARZBAN_ADMIN_PASS")
# TTL токена в секундах (кэширование)
TOKEN_TTL = int(os.getenv("MARZBAN_TOKEN_TTL", "300"))

_token_cache: Dict[str, Any] = {"token": None, "fetched_at": 0}


async def _fetch_token(session: aiohttp.ClientSession) -> Optional[str]:
    if not MARZBAN_URL or not MARZBAN_USER or not MARZBAN_PASS:
        return None
    # кэширование
    now = time.time()
    if _token_cache["token"] and now - _token_cache["fetched_at"] < TOKEN_TTL:
        return _token_cache["token"]
    url = f"{MARZBAN_URL.rstrip('/')}/api/admin/token"
    data = {"username": MARZBAN_USER, "password": MARZBAN_PASS}
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


def _bearer_header(token: Optional[str]) -> Dict[str, str]:
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


async def fetch_system_stats(session: aiohttp.ClientSession) -> Optional[Dict[str, Any]]:
    """
    GET /api/system -> SystemStats
    """
    if not MARZBAN_URL:
        return None
    token = await _fetch_token(session)
    url = f"{MARZBAN_URL.rstrip('/')}/api/system"
    try:
        async with session.get(url, headers=_bearer_header(token), timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


async def fetch_nodes(session: aiohttp.ClientSession) -> Optional[List[Dict[str, Any]]]:
    """
    GET /api/nodes -> список нод (NodeResponse)
    """
    if not MARZBAN_URL:
        return None
    token = await _fetch_token(session)
    url = f"{MARZBAN_URL.rstrip('/')}/api/nodes"
    try:
        async with session.get(url, headers=_bearer_header(token), timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


async def fetch_nodes_usage(session: aiohttp.ClientSession, start: Optional[str] = None, end: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    GET /api/nodes/usage -> NodesUsageResponse (usages array)
    опционально передать start/end (ISO strings) как query params
    """
    if not MARZBAN_URL:
        return None
    token = await _fetch_token(session)
    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    url = f"{MARZBAN_URL.rstrip('/')}/api/nodes/usage"
    try:
        async with session.get(url, headers=_bearer_header(token), params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


async def fetch_users_usage(session: aiohttp.ClientSession, start: Optional[str] = None, end: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    """
    GET /api/users/usage -> UsersUsagesResponse
    возвращает массив {username, usages:[{node_id,node_name,used_traffic}]}
    """
    if not MARZBAN_URL:
        return None
    token = await _fetch_token(session)
    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    url = f"{MARZBAN_URL.rstrip('/')}/api/users/usage"
    try:
        async with session.get(url, headers=_bearer_header(token), params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None