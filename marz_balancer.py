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
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from bokeh.plotting import figure
from bokeh.embed import components

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
    except Exception:
        return None

# Polling loop to fetch data periodically
async def poll_loop():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                token = await _fetch_token(session)
                # Fetch nodes and save stats
                nodes = await _fetch_nodes(session, token)
                if nodes:
                    save_node_stats(nodes)
                stats["last_update"] = time.time()
            except Exception as ex:
                stats["error"] = str(ex)
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
            return await resp.json()
    except Exception:
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

# Interactive graph endpoint
@APP.get("/users_graph_interactive", response_class=HTMLResponse)
async def users_graph_interactive(request: Request, period: str = "1d"):
    now = datetime.utcnow()
    if period == "1d":
        start_time = now - timedelta(days=1)
    elif period == "1w":
        start_time = now - timedelta(weeks=1)
    elif period == "1m":
        start_time = now - timedelta(days=30)
    else:
        start_time = now - timedelta(days=1)

    with sqlite3.connect(DB_PATH) as conn:
        query = """
            SELECT n.name, m.timestamp, m.users_count
            FROM node_metrics m
            JOIN nodes n ON m.node_id = n.id
            WHERE m.timestamp >= ?
            ORDER BY m.timestamp
        """
        df = pd.read_sql_query(query, conn, params=(start_time.isoformat(),))

    if df.empty:
        return HTMLResponse("<h3>Нет данных для графика</h3>")

    # Create Bokeh plot
    p = figure(title="Пользователи по нодам", x_axis_type="datetime", height=400, sizing_mode="stretch_width")
    for node_name in df['name'].unique():
        node_data = df[df['name'] == node_name]
        p.line(pd.to_datetime(node_data['timestamp']), node_data['users_count'], legend_label=node_name)

    p.legend.click_policy = "hide"
    script, div = components(p)

    html = f"""
    <!doctype html>
    <html lang="ru">
    <head>
        <meta charset="utf-8">
        <title>График пользователей по нодам</title>
        <link href="https://cdn.bokeh.org/bokeh/release/bokeh-2.4.3.min.css" rel="stylesheet">
        <script src="https://cdn.bokeh.org/bokeh/release/bokeh-2.4.3.min.js"></script>
    </head>
    <body>
        <h1>График пользователей по нодам</h1>
        {div}
        {script}
    </body>
    </html>
    """
    return HTMLResponse(content=html)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("marz_balancer:APP", host="0.0.0.0", port=APP_PORT, reload=True)