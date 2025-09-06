import asyncio
import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional

import aiohttp
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

APP = FastAPI()
CONFIG_PATH = Path(__file__).with_name("nodes.json")
POLL_INTERVAL = 5.0  # seconds

# candidate endpoints to try when clients_path missing or unknown
CANDIDATE_PATHS = [
    "/api/clients",
    "/clients",
    "/api/v1/clients",
    "/v1/clients",
    "/api/connections",
    "/connections",
    "/api/peers",
    "/peers",
    "/status",
    "/metrics",
]

# runtime state
stats: Dict[str, Dict[str, Any]] = {}
nodes: List[Dict[str, Any]] = []


def load_config():
    global nodes
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        nodes = json.load(f)
    # initialize stats and ensure nodes have optional fields
    for n in nodes:
        name = n.get("name") or n.get("url")
        n.setdefault("clients_path", None)
        n.setdefault("timeout", 5)
        stats[name] = {
            "name": name,
            "url": n.get("url"),
            "configured_path": n.get("clients_path"),
            "detected_path": None,
            "last_update": None,
            "count": 0,
            "clients": [],
            "error": None,
        }


async def try_fetch(session: aiohttp.ClientSession, base: str, path: str, timeout_s: int) -> Optional[Any]:
    url = f"{base.rstrip('/')}{path}"
    try:
        timeout = aiohttp.ClientTimeout(total=timeout_s)
        async with session.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                return {"error": f"{resp.status} {resp.reason}", "status": resp.status}
            # try parse json, if not JSON return raw text
            try:
                return await resp.json()
            except Exception:
                text = await resp.text()
                return {"raw": text}
    except Exception as ex:
        return {"error": str(ex)}


def normalize_response(data: Any) -> Dict[str, Any]:
    # returns dict: {"count": int, "clients": list}
    clients = []
    count = 0
    if isinstance(data, list):
        clients = data
        count = len(clients)
    elif isinstance(data, dict):
        if "clients" in data and isinstance(data["clients"], list):
            clients = data["clients"]
            count = len(clients)
        elif "connections" in data and isinstance(data["connections"], list):
            clients = data["connections"]
            count = len(clients)
        elif "peers" in data and isinstance(data["peers"], list):
            clients = data["peers"]
            count = len(clients)
        elif "count" in data and isinstance(data["count"], int):
            count = data["count"]
            clients = []
        elif "raw" in data:
            # raw text — can't parse
            clients = []
            count = 0
        elif "error" in data:
            clients = []
            count = 0
        else:
            # try to extract list-like fields
            for k, v in data.items():
                if isinstance(v, list):
                    clients = v
                    count = len(v)
                    break
    else:
        clients = []
        count = 0
    return {"count": count, "clients": clients}


async def fetch_node(session: aiohttp.ClientSession, node: dict):
    name = node.get("name") or node.get("url")
    base = node["url"]
    timeout_s = node.get("timeout", 5)

    # if configured path provided, try it first
    paths_to_try = []
    if node.get("clients_path"):
        paths_to_try.append(node["clients_path"])
    # if previously detected, try it
    if stats[name].get("detected_path"):
        paths_to_try.append(stats[name]["detected_path"])
    # then candidate list
    paths_to_try.extend([p for p in CANDIDATE_PATHS if p not in paths_to_try])

    last_error = None
    detected = None
    final_norm = {"count": 0, "clients": []}

    for p in paths_to_try:
        res = await try_fetch(session, base, p, timeout_s)
        if res is None:
            last_error = "no response"
            continue
        if isinstance(res, dict) and res.get("error"):
            last_error = res.get("error")
            continue
        # try normalize
        norm = normalize_response(res)
        # if we got meaningful data (count>0 or non-empty clients) accept this path
        if norm["count"] > 0 or len(norm["clients"]) > 0:
            detected = p
            final_norm = norm
            last_error = None
            break
        # sometimes endpoint returns count=0 but valid structure -> accept
        if isinstance(res, (list, dict)):
            # treat as valid even if empty list/dict (to avoid skipping correct but empty endpoint)
            detected = p
            final_norm = norm
            last_error = None
            break

    stats[name].update(
        {
            "last_update": time.time(),
            "count": final_norm["count"],
            "clients": final_norm["clients"],
            "error": last_error,
            "detected_path": detected,
        }
    )


async def poll_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            tasks = [fetch_node(session, node) for node in nodes]
            await asyncio.gather(*tasks)
            await asyncio.sleep(POLL_INTERVAL)


@APP.on_event("startup")
async def startup():
    load_config()
    asyncio.create_task(poll_loop())


@APP.get("/api/stats")
async def api_stats():
    return JSONResponse(content={"nodes": list(stats.values())})


@APP.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(
        """
<!doctype html><html><head><meta charset="utf-8"/><title>Marzban nodes stats</title></head><body>
<h1>Marzban nodes statistics</h1><div id="content">Loading...</div>
<script>
async function load(){
  try{
    const res = await fetch('/api/stats');
    const json = await res.json();
    const c = document.getElementById('content');
    c.innerHTML = '';
    json.nodes.forEach(n=>{
      const d = document.createElement('div');
      d.style.border='1px solid #ccc';d.style.margin='8px';d.style.padding='8px';
      let html = `<h3>${n.name}</h3>`;
      html += `<div>URL: ${n.url}</div>`;
      html += `<div>Configured path: ${n.configured_path || '—'}</div>`;
      html += `<div>Detected path: ${n.detected_path || '—'}</div>`;
      html += `<div>Last update: ${n.last_update ? new Date(n.last_update*1000).toLocaleString() : '—'}</div>`;
      if(n.error) html += `<div style="color:#b00">Error: ${n.error}</div>`;
      html += `<div>Connections: ${n.count}</div>`;
      if(n.clients && n.clients.length){
        html += '<details><summary>Clients</summary><ul>';
        n.clients.forEach(c2=>{
          if(typeof c2 === 'object'){ html += `<li>${c2.id||c2.peer||JSON.stringify(c2)}</li>`; }
          else html += `<li>${c2}</li>`;
        });
        html += '</ul></details>';
      }
      d.innerHTML = html;
      c.appendChild(d);
    });
  }catch(e){ document.getElementById('content').innerText='Error: '+e; }
}
load(); setInterval(load, 5000);
</script></body></html>
"""
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("marz_balancer:APP", host="0.0.0.0", port=8000, reload=True)