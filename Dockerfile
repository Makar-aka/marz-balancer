FROM python:3.11-slim

WORKDIR /app

# Копируем только requirements.txt для установки зависимостей
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir scapy
# Копируем основной скрипт и .env (если нужно)
COPY marz_balancer.py .
COPY .env .env

# Открываем порт (по умолчанию 8023, можно переопределить через переменную окружения)
EXPOSE 8023

# Запуск приложения
CMD ["python", "marz_balancer.py"]