FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем все Python-модули
COPY *.py .
COPY .env .

EXPOSE 8023

CMD ["python", "marz_balancer.py"]