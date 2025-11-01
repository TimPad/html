#!/bin/bash
# Quick Start - Развертывание в Cloud.ru

echo "🚀 DataCulture Platform - Quick Start"
echo ""

# Проверка secrets.toml
if [ ! -f "secrets.toml" ]; then
    echo "⚠️  Создайте secrets.toml перед запуском!"
    echo ""
    echo "Выполните:"
    echo "  1. cp .streamlit/secrets.toml.example secrets.toml"
    echo "  2. nano secrets.toml  # Заполните реальные значения"
    echo "  3. ./quickstart.sh"
    exit 1
fi

# Запуск развертывания
./deploy.sh

echo ""
echo "✅ Готово! Приложение запущено на http://localhost:8501"
