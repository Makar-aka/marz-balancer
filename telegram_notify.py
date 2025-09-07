import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Set
import redis.asyncio as redis
import aiohttp

from config import (
    TELEGRAM_ENABLED, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    NODE_REMINDER_INTERVAL, REDIS_URL
)

# Redis client
redis_client = None
first_run = True

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

async def send_telegram_message(message):
    """Отправка сообщения в Telegram"""
    if not TELEGRAM_ENABLED or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
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

async def get_node_status_from_redis(node_id):
    """Получение статуса ноды из Redis"""
    if not redis_client:
        return None
    try:
        status = await redis_client.get(f"node:{node_id}:status")
        return status.decode('utf-8') if status else None
    except Exception:
        return None

async def save_node_status_to_redis(node_id, status):
    """Сохранение статуса ноды в Redis"""
    if not redis_client:
        return
    try:
        await redis_client.set(f"node:{node_id}:status", status)
    except Exception:
        pass

async def set_node_last_notified(node_id):
    """Записать время последнего уведомления о ноде"""
    if not redis_client:
        return
    try:
        current_time = datetime.utcnow().timestamp()
        await redis_client.set(f"node:{node_id}:last_notified", str(current_time))
    except Exception:
        pass

async def get_node_last_notified(node_id):
    """Получить время последнего уведомления о ноде"""
    if not redis_client:
        return None
    try:
        result = await redis_client.get(f"node:{node_id}:last_notified")
        return float(result) if result else None
    except Exception:
        return None

async def set_node_down_time(node_id):
    """Записать время, когда нода стала недоступна"""
    if not redis_client:
        return
    try:
        # Проверяем, не записано ли уже время недоступности
        if not await redis_client.exists(f"node:{node_id}:down_time"):
            current_time = datetime.utcnow().timestamp()
            await redis_client.set(f"node:{node_id}:down_time", str(current_time))
    except Exception as e:
        print(f"Ошибка сохранения времени недоступности: {e}")

async def get_node_down_time(node_id):
    """Получить время, когда нода стала недоступна"""
    if not redis_client:
        return None
    try:
        result = await redis_client.get(f"node:{node_id}:down_time")
        return float(result) if result else None
    except Exception:
        return None

async def clear_node_down_time(node_id):
    """Удалить время недоступности ноды (когда она снова доступна)"""
    if not redis_client:
        return
    try:
        await redis_client.delete(f"node:{node_id}:down_time")
    except Exception as e:
        print(f"Ошибка удаления времени недоступности: {e}")

def format_downtime(seconds):
    """Форматирует время недоступности в читаемом виде"""
    if seconds < 60:
        return f"{int(seconds)} сек"
    
    minutes = seconds / 60
    if minutes < 60:
        return f"{int(minutes)} мин"
    
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.1f} ч"
    
    days = hours / 24
    return f"{days:.1f} д"

async def save_node_ids_to_redis(node_ids):
    """Сохраняет список ID нод в Redis"""
    if not redis_client:
        return
    try:
        await redis_client.delete("marzban:node_ids")
        if node_ids:
            await redis_client.sadd("marzban:node_ids", *node_ids)
    except Exception as e:
        print(f"Ошибка сохранения ID нод в Redis: {e}")

async def get_node_ids_from_redis():
    """Получает список ID нод из Redis"""
    if not redis_client:
        return set()
    try:
        node_ids = await redis_client.smembers("marzban:node_ids")
        return {node_id.decode('utf-8') for node_id in node_ids} if node_ids else set()
    except Exception as e:
        print(f"Ошибка получения ID нод из Redis: {e}")
        return set()

async def check_nodes_changes(nodes):
    """Отслеживает добавление/удаление нод"""
    current_node_ids = {str(n.get("id")) for n in nodes if n.get("id") is not None}
    previous_node_ids = await get_node_ids_from_redis()
    
    if not previous_node_ids:
        await save_node_ids_to_redis(current_node_ids)
        return
    
    # Проверяем новые ноды
    new_nodes = current_node_ids - previous_node_ids
    if new_nodes:
        new_node_names = []
        for node_id in new_nodes:
            node = next((n for n in nodes if str(n.get("id")) == node_id), None)
            if node:
                node_name = node.get("name") or node.get("address") or f"Node {node_id}"
                new_node_names.append(node_name)
        
        if new_node_names:
            if len(new_node_names) == 1:
                message = f"🆕 <b>Добавлена нода:</b> {new_node_names[0]}"
            else:
                message = f"🆕 <b>Новые ноды ({len(new_node_names)}):</b>\n"
                for i, name in enumerate(new_node_names):
                    prefix = "└" if i == len(new_node_names) - 1 else "├"
                    message += f"{prefix} {name}\n"
            await send_telegram_message(message)
    
    # Проверяем удаленные ноды
    removed_nodes = previous_node_ids - current_node_ids
    if removed_nodes:
        if len(removed_nodes) == 1:
            node_id = list(removed_nodes)[0]
            message = f"❌ <b>Удалена нода:</b> {node_id}"
        else:
            message = f"❌ <b>Удалены ноды ({len(removed_nodes)}):</b>\n"
            for i, node_id in enumerate(list(removed_nodes)[:5]):
                prefix = "└" if i == len(removed_nodes) - 1 or i == 4 else "├"
                message += f"{prefix} {node_id}\n"
            if len(removed_nodes) > 5:
                message += f"└ и ещё {len(removed_nodes) - 5}...\n"
        await send_telegram_message(message)
    
    # Сохраняем текущий список нод
    if new_nodes or removed_nodes:
        await save_node_ids_to_redis(current_node_ids)

async def send_startup_notification(nodes):
    """Отправляет уведомление о запуске мониторинга"""
    online_nodes = sum(1 for n in nodes if n.get("status") == "connected")
    total_nodes = len(nodes)
    
    message = f"🚀 <b>Marzban Monitor</b> запущен\n\n"
    message += f"📊 <b>Статистика:</b>\n"
    message += f"├ Всего нод: {total_nodes}\n"
    message += f"└ Онлайн: {online_nodes}/{total_nodes}\n"
    
    if total_nodes > 0 and online_nodes < total_nodes:
        offline_nodes = [n.get("name") or n.get("address") or f"Node {n.get('id')}" 
                         for n in nodes if n.get("status") != "connected"]
        if offline_nodes:
            message += f"\n❌ <b>Недоступны:</b> "
            if len(offline_nodes) <= 5:
                message += ", ".join(offline_nodes)
            else:
                message += ", ".join(offline_nodes[:4]) + f" и ещё {len(offline_nodes)-4}"
    
    await send_telegram_message(message)

async def check_node_status_changes(nodes):
    """Проверка изменений статуса нод и отправка уведомлений"""
    for node in nodes:
        node_id = node.get("id")
        if not node_id:
            continue
            
        current_status = node.get("status")
        previous_status = await get_node_status_from_redis(node_id)
        
        if previous_status is not None and previous_status != current_status:
            node_name = node.get("name") or node.get("address") or f"Node {node_id}"
            
            if current_status != "connected" and previous_status == "connected":
                # Нода стала недоступной
                await set_node_down_time(node_id)
                message = f"⚠️ <b>Нода отключена:</b> {node_name}\n"
                message += f"└ Статус: <code>{current_status}</code>"
                await send_telegram_message(message)
                await set_node_last_notified(node_id)
                
            elif current_status == "connected" and previous_status != "connected":
                # Нода восстановилась
                down_time = await get_node_down_time(node_id)
                message = f"✅ <b>Нода онлайн:</b> {node_name}"
                
                if down_time:
                    current_time = datetime.utcnow().timestamp()
                    downtime_seconds = current_time - down_time
                    formatted_downtime = format_downtime(downtime_seconds)
                    message += f"\n└ Простой: <b>{formatted_downtime}</b>"
                
                await send_telegram_message(message)
                await clear_node_down_time(node_id)
                
        await save_node_status_to_redis(node_id, current_status)

async def check_offline_nodes_reminders(nodes):
    """Напоминания о нодах, которые долго остаются недоступными"""
    current_time = datetime.utcnow().timestamp()
    reminder_interval_seconds = NODE_REMINDER_INTERVAL * 60  # минуты в секунды
    
    offline_nodes = []
    for node in nodes:
        node_id = node.get("id")
        if not node_id or node.get("status") == "connected":
            continue
            
        current_status = node.get("status")
        last_notified = await get_node_last_notified(node_id)
        
        if last_notified is None or (current_time - last_notified) > reminder_interval_seconds:
            node_name = node.get("name") or node.get("address") or f"Node {node_id}"
            
            # Получаем время начала недоступности
            down_time = await get_node_down_time(node_id)
            total_downtime = "неизвестно"
            
            if down_time:
                downtime_seconds = current_time - down_time
                total_downtime = format_downtime(downtime_seconds)
            
            offline_nodes.append((node_name, current_status, total_downtime))
            await set_node_last_notified(node_id)
    
    # Отправляем одно общее уведомление для всех недоступных нод
    if offline_nodes:
        if len(offline_nodes) == 1:
            node_name, status, downtime = offline_nodes[0]
            message = f"⚠️ <b>Нода всё ещё недоступна:</b> {node_name}\n"
            message += f"├ Статус: <code>{status}</code>\n"
            message += f"└ Время простоя: <b>{downtime}</b>"
        else:
            message = f"⚠️ <b>Недоступные ноды ({len(offline_nodes)}):</b>\n"
            for i, (node_name, status, downtime) in enumerate(offline_nodes):
                prefix = "└" if i == len(offline_nodes) - 1 else "├"
                message += f"{prefix} {node_name} • <b>{downtime}</b>\n"
        
        await send_telegram_message(message)

async def process_notifications(nodes, is_first_run=False):
    """Обрабатывает все уведомления для нод"""
    if not TELEGRAM_ENABLED:
        return
        
    if not redis_client:
        if not await init_redis():
            return
            
    # Отправляем уведомление о запуске при первом запуске
    if is_first_run:
        await send_startup_notification(nodes)
        
    # Проверяем изменения статуса нод
    await check_node_status_changes(nodes)
    
    # Отправляем напоминания о недоступных нодах
    await check_offline_nodes_reminders(nodes)
    
    # Проверяем добавление/удаление нод
    await check_nodes_changes(nodes)