#!/bin/bash
# Скрипт развертывания DataCulture Platform на Cloud.ru (Ubuntu 22.04)

set -e

echo "🚀 DataCulture Platform - Развертывание в Cloud.ru"
echo "=================================================="

# Проверка наличия secrets.toml
if [ ! -f "secrets.toml" ]; then
    echo "❌ Ошибка: файл secrets.toml не найден!"
    echo "📝 Создайте secrets.toml на основе .streamlit/secrets.toml.example"
    exit 1
fi

# Проверка наличия Docker
if ! command -v docker &> /dev/null; then
    echo "📦 Docker не установлен. Устанавливаю Docker..."
    
    # Обновление пакетов
    sudo apt-get update
    
    # Установка зависимостей
    sudo apt-get install -y \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg \
        lsb-release
    
    # Добавление GPG ключа Docker
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    
    # Добавление репозитория Docker
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    
    # Установка Docker
    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io
    
    echo "✅ Docker установлен"
fi

# Проверка наличия Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo "📦 Docker Compose не установлен. Устанавливаю..."
    
    # Установка Docker Compose
    sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    
    echo "✅ Docker Compose установлен"
fi

# Добавление текущего пользователя в группу docker
if ! groups $USER | grep &>/dev/null '\bdocker\b'; then
    echo "👤 Добавляю пользователя в группу docker..."
    sudo usermod -aG docker $USER
    echo "⚠️  Необходимо перелогиниться или выполнить: newgrp docker"
fi

# Остановка старых контейнеров
echo "🛑 Остановка старых контейнеров..."
docker-compose down 2>/dev/null || true

# Сборка образа
echo "🔨 Сборка Docker образа..."
docker-compose build --no-cache

# Запуск контейнеров
echo "▶️  Запуск приложения..."
docker-compose up -d

# Проверка статуса
echo ""
echo "✅ Развертывание завершено!"
echo ""
echo "📊 Статус контейнеров:"
docker-compose ps

echo ""
echo "🌐 Приложение доступно по адресу: http://localhost:8501"
echo "📝 Логи: docker-compose logs -f"
echo "🛑 Остановка: docker-compose down"
echo ""
echo "⚠️  Важно: Убедитесь, что порт 8501 открыт в настройках firewall!"
