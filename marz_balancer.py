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

# port to monitor for unique client IPs
MONITOR_PORT = int(os.getenv("MONITOR_PORT", "8443"))

# candidate node endpoints to try for live clients
# Оставляем только необходимые пути — ваш микросервис (/connections),
# плюс запасные /clients и /status для совместимости.
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
        # accept responses that contain clients list or count
        if norm.get("count", 0) > 0 or len(norm.get("clients", [])) > 0 or isinstance(res, (list, dict)):
            # attach raw meta fields if available (e.g. port / trusted)
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
        else:
            # try split by colon (last colon before port)
            if ":" in peer:
                # for IPv6 peer may contain many colons; split from right
                ip = peer.rsplit(":", 1)[0]
                # remove surrounding brackets
                ip = ip.strip("[]")
                ips.add(ip)
    return list(ips)


def get_unique_remote_ips(port: int) -> List[str]:
    """
    Try to get unique remote IPs connected to local TCP port `port`.
    Prefer `ss` parsing, fallback to /proc/net/tcp (IPv4).
    """
    # try ss first
    try:
        cmd = ["ss", "-tn", "state", "established", "sport", f":{port}"]
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True, timeout=3)
        ips = _parse_ss_output_for_remote_ips(out)
        if ips:
            return ips
    except Exception:
        pass

    # fallback: parse /proc/net/tcp for IPv4
    try:
        if os.path.exists("/proc/net/tcp"):
            ips = set()
            with open("/proc/net/tcp", "r", encoding="utf-8") as f:
                next(f)  # skip header
                for line in f:
                    cols = line.split()
                    if len(cols) < 4:
                        continue
                    local, remote, state = cols[1], cols[2], cols[3]
                    if state != "01":  # 01 = ESTABLISHED
                        continue
                    # local format: "0100007F:1F90" (hex ip:hex port)
                    try:
                        laddr_hex, lport_hex = local.split(":")
                        lport = int(lport_hex, 16)
                    except Exception:
                        continue
                    if lport != port:
                        continue
                    raddr_hex, rport_hex = remote.split(":")
                    try:
                        # convert little-endian hex IPv4 to dotted quad
                        import struct, socket
                        ip_int = int(raddr_hex, 16)
                        packed = struct.pack("<I", ip_int)
                        ip = socket.inet_ntoa(packed)
                        ips.add(ip)
                    except Exception:
                        continue
            return list(ips)
    except Exception:
        pass

    return []


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
                    # attach optional meta (port/trusted) if provided by node microservice
                    if res.get("port") is not None:
                        node_entries[i]["clients_port"] = res.get("port")
                    if res.get("meta") is not None:
                        node_entries[i]["clients_meta"] = res.get("meta")

                stats["nodes"] = node_entries

                # get unique remote IPs to MONITOR_PORT without blocking loop (local fallback)
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


# Lifespan handler replaces deprecated @APP.on_event("startup")
@asynccontextmanager
async def lifespan(app: FastAPI):
    # start background polling task
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


APP = FastAPI(lifespan=lifespan)


def _normalize_users_usage(data: Any) -> List[Dict[str, Any]]:
    """
    Normalize stats['users_usage'] into a list of user-usage entries.
    Handles cases where master returns a list or a dict with a key containing the list.
    """
    if not data:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # common keys: 'users', 'items', 'data', 'usages'
        for key in ("users", "items", "data", "usages"):
            v = data.get(key)
            if isinstance(v, list):
                return v
        for v in data.values():
            if isinstance(v, list):
                return v
    return []


@APP.get("/api/stats")
async def api_stats():
    return JSONResponse(content=stats)


@APP.get("/", response_class=HTMLResponse)
async def index():
    nodes = stats.get("nodes", [])
    last = stats.get("last_update")
    err = stats.get("error")
    system = stats.get("system")
    port_info = stats.get("port_8443", {})
    last_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last)) if last else "—"

    header = f"<div>Последнее обновление: {last_str}</div>"
    header += f"<div>Подключений к порту {MONITOR_PORT}: {port_info.get('unique_clients', '—')}</div>"
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
        if n.get("clients_port"):
            items += f"Clients port: {n.get('clients_port')}<br/>"
        if n.get("clients_meta"):
            items += f"Clients meta: {n.get('clients_meta')}<br/>"
        items += f"Uplink: {n.get('uplink') if n.get('uplink') is not None else '—'} Downlink: {n.get('downlink') if n.get('downlink') is not None else '—'}<br/>"
        if n.get("detected_path"):
            items += f"Detected path: {n.get('detected_path')}<br/>"
        if n.get("clients_error"):
            items += f"<div style='color:#b00'>Clients error: {n.get('clients_error')}</div>"
        if n.get("clients"):
            items += "<details><summary>Клиенты (пример)</summary><ul>"
            for c in n.get("clients")[:200]:
                if isinstance(c, dict):
                    identifier = c.get("id") or c.get("peer") or c.get("addr") or str(c)
                else:
                    identifier = str(c)
                items += f"<li>{identifier}</li>"
            items += "</ul></details>"
        items += "</div>"
    if not items:
        items = "<div>Ноды не обнаружены.</div>"

    users_list = _normalize_users_usage(stats.get("users_usage"))
    users_usage_summary = ""
    if users_list:
        users_usage_summary = "<h3>Users usage (sample)</h3><ul>"
        for u in users_list[:50]:
            uname = u.get("username") or str(u.get("user") if isinstance(u, dict) else u)
            usages = u.get("usages") or []
            nodes_used = ", ".join([str(x.get("node_name") or x.get("node_id")) for x in usages])
            users_usage_summary += f"<li>{uname}: {nodes_used}</li>"
        users_usage_summary += "</ul>"

    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Marzban nodes</title></head><body>
<h1>Marzban — ноды</h1>
{header}
<div style="color:#b00">{err or ''}</div>
<div>{items}</div>
{users_usage_summary}
<script>
setTimeout(()=>location.reload(), {int(POLL_INTERVAL*1000)});
</script>
</body></html>"""
    return HTMLResponse(content=html)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("marz_balancer:APP", host="0.0.0.0", port=APP_PORT, reload=True)