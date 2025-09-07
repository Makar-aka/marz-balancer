import aiohttp
from datetime import datetime
from typing import Dict, Any, List, Optional
import redis.asyncio as redis

from config import (
    TELEGRAM_ENABLED, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    NODE_REMINDER_INTERVAL, REDIS_URL
)

# Redis client
redis_client = None

async def init_redis():
    """Инициализация Redis-клиента"""
    global redis_client
    if REDIS_URL:
        try:
            redis_client = await redis.from_url(REDIS_URL)
            return True
        except Exception as e:
            print(f"Ошибка подключения к Redis: {e}")
    return False

async def send_telegram_message(message: str):
    """Отправка сообщения в Telegram"""
    if not TELEGRAM_ENABLED or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    
    try:
        async with aiohttp.ClientSession() as session:
            api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML"
            }
            async with session.post(api_url, json=payload) as response:
                return await response.json()
    except Exception as e:
        print(f"Ошибка отправки в Telegram: {e}")
        return False

async def get_node_status_from_redis(node_id) -> Optional[str]:
    """Получение статуса ноды из Redis"""
    if not redis_client:
        return None
    try:
        status = await redis_client.get(f"node:{node_id}:status")
        return status.decode('utf-8') if status else None
    except Exception as e:
        print(f"Ошибка получения статуса из Redis: {e}")
        return None

async def save_node_status_to_redis(node_id, status: str):
    """Сохранение статуса ноды в Redis"""
    if not redis_client:
        return False
    try:
        await redis_client.set(f"node:{node_id}:status", status)
        return True
    except Exception as e:
        print(f"Ошибка сохранения статуса в Redis: {e}")
        return False

async def set_node_last_notified(node_id):
    """Записать время последнего уведомления о ноде"""
    if not redis_client:
        return False
    try:
        current_time = datetime.utcnow().timestamp()
        await redis_client.set(f"node:{node_id}:last_notified", str(current_time))
        return True
    except Exception as e:
        print(f"Ошибка сохранения времени уведомления: {e}")
        return False

async def get_node_last_notified(node_id) -> Optional[float]:
    """Получить время последнего уведомления о ноде"""
    if not redis_client:
        return None
    try:
        result = await redis_client.get(f"node:{node_id}:last_notified")
        return float(result) if result else None
    except Exception:
        return None

async def check_node_status_changes(nodes: List[Dict[str, Any]]):
    """Проверка изменений статуса нод и отправка уведомлений"""
    if not TELEGRAM_ENABLED or not redis_client:
        return
    
    for node in nodes:
        node_id = node.get("id")
        if not node_id:
            continue
            
        current_status = node.get("status")
        previous_status = await get_node_status_from_redis(node_id)
        
        if previous_status is not None and previous_status != current_status:
            node_name = node.get("name") or node.get("address") or f"Node {node_id}"
            
            # Если нода стала недоступной
            if current_status != "connected" and previous_status == "connected":
                message = f"⚠️ <b>Нода недоступна:</b> {node_name}\n"
                message += f"Текущий статус: {current_status}"
                await send_telegram_message(message)
                await set_node_last_notified(node_id)
                
            # Если нода стала доступна
            elif current_status == "connected" and previous_status != "connected":
                message = f"✅ <b>Нода снова доступна:</b> {node_name}"
                await send_telegram_message(message)
                
        # Сохраняем текущий статус
        await save_node_status_to_redis(node_id, current_status)

async def check_offline_nodes_reminders(nodes: List[Dict[str, Any]]):
    """Напоминания о нодах, которые долго остаются недоступными"""
    if not TELEGRAM_ENABLED or not redis_client:
        return
        
    current_time = datetime.utcnow().timestamp()
    reminder_interval_seconds = NODE_REMINDER_INTERVAL * 3600
    
    for node in nodes:
        node_id = node.get("id")
        if not node_id:
            continue
            
        current_status = node.get("status")
        
        # Если нода не подключена, проверяем, нужно ли напомнить о ней
        if current_status != "connected":
            last_notified = await get_node_last_notified(node_id)
            
            # Если давно не уведомляли (или вообще не уведомляли)
            if last_notified is None or (current_time - last_notified) > reminder_interval_seconds:
                node_name = node.get("name") or node.get("address") or f"Node {node_id}"
                hours_offline = "неизвестно"
                if last_notified:
                    hours = (current_time - last_notified) / 3600
                    hours_offline = f"{hours:.1f}"
                    
                message = f"⚠️ <b>Напоминание:</b> Нода {node_name} остаётся недоступной {hours_offline} часов\n"
                message += f"Статус: {current_status}"
                
                await send_telegram_message(message)
                await set_node_last_notified(node_id)