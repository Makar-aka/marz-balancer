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
    for node_id in new_nodes:
        node = next((n for n in nodes if str(n.get("id")) == node_id), None)
        if node:
            node_name = node.get("name") or node.get("address") or f"Node {node_id}"
            message = f"🆕 <b>Обнаружена новая нода:</b> {node_name}"
            await send_telegram_message(message)
    
    # Проверяем удаленные ноды
    removed_nodes = previous_node_ids - current_node_ids
    if removed_nodes:
        message = f"❌ <b>Удалено нод:</b> {len(removed_nodes)}"
        if len(removed_nodes) <= 5:  # Если немного нод, показываем их ID
            message += "\nID удаленных нод: " + ", ".join(removed_nodes)
        await send_telegram_message(message)
    
    # Сохраняем текущий список нод
    if new_nodes or removed_nodes:
        await save_node_ids_to_redis(current_node_ids)

async def send_startup_notification(nodes):
    """Отправляет уведомление о запуске мониторинга"""
    online_nodes = sum(1 for n in nodes if n.get("status") == "connected")
    total_nodes = len(nodes)
    
    message = f"🚀 <b>Мониторинг Marzban запущен</b>\n\n"
    message += f"Всего нод: {total_nodes}\n"
    message += f"Онлайн нод: {online_nodes}\n"
    
    if total_nodes > 0:
        offline_nodes = [n.get("name") or n.get("address") or f"Node {n.get('id')}" 
                         for n in nodes if n.get("status") != "connected"]
        if offline_nodes:
            message += f"\nНедоступные ноды ({len(offline_nodes)}):\n"
            message += "\n".join([f"- {name}" for name in offline_nodes[:5]])
            if len(offline_nodes) > 5:
                message += f"\n...и ещё {len(offline_nodes) - 5}"
    
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
                # Нода стала недоступной, сохраняем время
                await set_node_down_time(node_id)
                message = f"⚠️ <b>Нода недоступна:</b> {node_name}\n"
                message += f"Текущий статус: {current_status}"
                await send_telegram_message(message)
                await set_node_last_notified(node_id)
                
            elif current_status == "connected" and previous_status != "connected":
                # Нода восстановилась, проверяем сколько времени была недоступна
                down_time = await get_node_down_time(node_id)
                message = f"✅ <b>Нода снова доступна:</b> {node_name}"
                
                if down_time:
                    current_time = datetime.utcnow().timestamp()
                    downtime_seconds = current_time - down_time
                    formatted_downtime = format_downtime(downtime_seconds)
                    message += f"\nВремя отвала: {formatted_downtime}"
                
                await send_telegram_message(message)
                
                # Очищаем информацию о времени недоступности
                await clear_node_down_time(node_id)
                
        await save_node_status_to_redis(node_id, current_status)

async def check_offline_nodes_reminders(nodes):
    """Напоминания о нодах, которые долго остаются недоступными"""
    current_time = datetime.utcnow().timestamp()
    # Теперь интервал в минутах вместо часов
    reminder_interval_seconds = NODE_REMINDER_INTERVAL * 60  # минуты * 60 = секунды
    
    for node in nodes:
        node_id = node.get("id")
        if not node_id:
            continue
            
        current_status = node.get("status")
        
        if current_status != "connected":
            last_notified = await get_node_last_notified(node_id)
            
            if last_notified is None or (current_time - last_notified) > reminder_interval_seconds:
                node_name = node.get("name") or node.get("address") or f"Node {node_id}"
                
                # Получаем время начала недоступности
                down_time = await get_node_down_time(node_id)
                total_downtime = "неизвестно"
                
                if down_time:
                    downtime_seconds = current_time - down_time
                    total_downtime = format_downtime(downtime_seconds)
                
                # Время с последнего уведомления в минутах
                minutes_since_notify = "неизвестно"
                if last_notified:
                    minutes = (current_time - last_notified) / 60  # теперь в минутах
                    minutes_since_notify = f"{minutes:.1f}"
                    
                message = f"⚠️ <b>Напоминание:</b> Нода {node_name} остаётся недоступной\n"
                message += f"Общее время отвала: {total_downtime}\n"
                message += f"Статус: {current_status}"
                
                await send_telegram_message(message)
                await set_node_last_notified(node_id)

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