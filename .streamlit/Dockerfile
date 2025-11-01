# DataCulture Platform - Docker Image
# Базовый образ Python 3.11 на основе slim-версии для оптимизации размера
FROM python:3.11-slim

# Метаданные образа
LABEL maintainer="DataCulture Team"
LABEL description="DataCulture Platform для ВШЭ - Streamlit приложение"

# Установка рабочей директории
WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

# Копирование файлов зависимостей
COPY requirements.txt .

# Установка Python зависимостей
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копирование приложения
COPY app.py .
COPY create_peresdachi_table.sql .
COPY icons/ ./icons/

# Создание директории для конфигурации Streamlit
RUN mkdir -p .streamlit

# Копирование конфигурации Streamlit (без secrets)
COPY .streamlit/config.toml .streamlit/config.toml

# Открытие порта Streamlit
EXPOSE 8501

# Проверка здоровья контейнера
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Запуск приложения
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
