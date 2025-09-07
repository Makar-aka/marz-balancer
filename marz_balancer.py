import os
import time
import asyncio
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from contextlib import asynccontextmanager

import aiohttp
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

load_dotenv()

MARZBAN_URL = os.getenv("MARZBAN_URL", "").rstrip("/")
MARZBAN_ADMIN_USER = os.getenv("MARZBAN_ADMIN_USER", "")
MARZBAN_ADMIN_PASS = os.getenv("MARZBAN_ADMIN_PASS", "")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5"))
APP_PORT = int(os.getenv("APP_PORT", "8023"))
DB_PATH = os.getenv("STATS_DB_PATH", "/data/stats.db")

# runtime state
stats: Dict[str, Any] = {
    "nodes": [],
    "last_update": None,
    "error": None,
    "system": None,
    "nodes_usage": None,
    "users_usage": None,
}

# Initialize database
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                address TEXT,
                api_port INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS node_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                timestamp DATETIME NOT NULL,
                users_count INTEGER NOT NULL,
                FOREIGN KEY (node_id) REFERENCES nodes (id)
            )
        """)
        conn.commit()
init_db()

# Save node stats to the database
def save_node_stats(nodes):
    ts = datetime.utcnow().isoformat()
    print("Сохраняем данные о нодах:", nodes)  # Отладочный вывод
    with sqlite3.connect(DB_PATH) as conn:
        for n in nodes:
            conn.execute("""
                INSERT INTO nodes (name, address, api_port)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET address=excluded.address, api_port=excluded.api_port
            """, (n.get('name'), n.get('address'), n.get('api_port')))
            
            node_id = conn.execute("SELECT id FROM nodes WHERE name = ?", (n.get('name'),)).fetchone()[0]
            conn.execute("""
                INSERT INTO node_metrics (node_id, timestamp, users_count)
                VALUES (?, ?, ?)
            """, (node_id, ts, n.get('clients_count') or 0))
        conn.commit()

# Fetch token for authentication
async def _fetch_token(session: aiohttp.ClientSession) -> Optional[str]:
    if not MARZBAN_URL or not MARZBAN_ADMIN_USER or not MARZBAN_ADMIN_PASS:
        return None
    now = time.time()
    url = f"{MARZBAN_URL}/api/admin/token"
    data = {"username": MARZBAN_ADMIN_USER, "password": MARZBAN_ADMIN_PASS}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        async with session.post(url, data=data, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            j = await resp.json()
            return j.get("access_token") or j.get("token")
    except Exception as e:
        print("Ошибка при получении токена:", e)  # Отладочный вывод
        return None

# Polling loop to fetch data periodically
async def poll_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                print("Обновляем данные о нодах...")  # Отладочный вывод
                token = await _fetch_token(session)
                # Fetch nodes and save stats
                nodes = await _fetch_nodes(session, token)
                if nodes:
                    save_node_stats(nodes)
                    stats["nodes"] = nodes  # Обновляем stats["nodes"]
                stats["last_update"] = time.time()
            except Exception as ex:
                stats["error"] = str(ex)
                print("Ошибка в poll_loop:", ex)  # Отладочный вывод
            await asyncio.sleep(POLL_INTERVAL)

# Fetch nodes
async def _fetch_nodes(session: aiohttp.ClientSession, token: Optional[str]) -> Optional[List[Dict[str, Any]]]:
    if not MARZBAN_URL:
        return None
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    url = f"{MARZBAN_URL}/api/nodes"
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            nodes = await resp.json()
            print("Полученные данные о нодах:", nodes)  # Отладочный вывод
            return nodes
    except Exception as e:
        print("Ошибка при получении данных о нодах:", e)  # Отладочный вывод
        return None

# FastAPI app
APP = FastAPI()

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(poll_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

# API endpoint for stats
@APP.get("/api/stats")
async def api_stats():
    return JSONResponse(content=stats)

@APP.get("/", response_class=HTMLResponse)
async def index(request: Request):
    nodes = stats.get("nodes", [])
    last_update = stats.get("last_update")
    error = stats.get("error")

    print("Отладка: stats['nodes'] =", nodes)  # Отладочный вывод

    last_update_str = (
        datetime.fromtimestamp(last_update).strftime("%Y-%m-%d %H:%M:%S")
        if last_update
        else "—"
    )

    html = f"""
    <!doctype html>
    <html lang="ru">
    <head>
        <meta charset="utf-8">
        <title>MarzBalancer</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    </head>
    <body class="bg-light">
        <div class="container py-4">
            <h1 class="mb-4">MarzBalancer</h1>
            <div class="mb-3">
                <span class="badge bg-secondary">Последнее обновление: {last_update_str}</span>
            </div>
            <div style="color: red;">{error or ""}</div>
            <h2 class="mt-4">Ноды</h2>
            <ul class="list-group">
    """
    for node in nodes:
        html += f"""
            <li class="list-group-item">
                <b>{node.get('name') or '—'}</b><br>
                Адрес: {node.get('address') or '—'}<br>
                Порт API: {node.get('api_port') or '—'}<br>
                Клиенты: {node.get('clients_count') or '—'}
            </li>
        """
    if not nodes:
        html += "<li class='list-group-item'>Нет данных о нодах.</li>"

    html += """
            </ul>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("marz_balancer:APP", host="0.0.0.0", port=APP_PORT, reload=True)