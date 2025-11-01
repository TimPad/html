# 🐳 Развертывание DataCulture Platform в Cloud.ru

Инструкция по развертыванию приложения на виртуальной машине Ubuntu 22.04 в Cloud.ru с использованием Docker.

## 📋 Предварительные требования

- ✅ Виртуальная машина Ubuntu 22.04 в Cloud.ru
- ✅ Минимум 2GB RAM, 2 CPU cores
- ✅ Открыт порт 8501 (или другой порт по выбору)
- ✅ SSH доступ к серверу

## 🚀 Быстрое развертывание

### Шаг 1: Подключение к серверу

```bash
ssh username@your-server-ip
```

### Шаг 2: Клонирование проекта

```bash
# Создание директории для проекта
mkdir -p ~/dataculture-platform
cd ~/dataculture-platform

# Загрузка файлов проекта (через git или scp)
# Вариант 1: Git
git clone <your-repo-url> .

# Вариант 2: SCP (с локальной машины)
# scp -r /path/to/project/* username@server-ip:~/dataculture-platform/
```

### Шаг 3: Настройка secrets

```bash
# Создание файла secrets.toml
cp .streamlit/secrets.toml.example secrets.toml

# Редактирование secrets.toml
nano secrets.toml
```

**Заполните реальные значения:**
```toml
[supabase]
url = "https://your-project.supabase.co"
key = "your-supabase-anon-key"

NEBIUS_API_KEY = "your-nebius-api-key"

STUDENTS_UPDATE_PASSWORD = "your-secure-password"
```

### Шаг 4: Запуск развертывания

```bash
# Сделать скрипт исполняемым
chmod +x deploy.sh

# Запустить развертывание
./deploy.sh
```

Скрипт автоматически:
- ✅ Установит Docker и Docker Compose (если не установлены)
- ✅ Соберет Docker образ
- ✅ Запустит контейнер с приложением
- ✅ Настроит автоматический перезапуск

### Шаг 5: Проверка

```bash
# Проверка статуса контейнера
docker-compose ps

# Просмотр логов
docker-compose logs -f

# Проверка доступности
curl http://localhost:8501/_stcore/health
```

Приложение будет доступно по адресу: `http://your-server-ip:8501`

## 🔧 Ручное развертывание

Если автоматический скрипт не подходит:

### 1. Установка Docker

```bash
# Обновление системы
sudo apt-get update
sudo apt-get upgrade -y

# Установка зависимостей
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release

# Добавление GPG ключа Docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Добавление репозитория
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Установка Docker
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io

# Добавление пользователя в группу docker
sudo usermod -aG docker $USER
newgrp docker
```

### 2. Установка Docker Compose

```bash
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
docker-compose --version
```

### 3. Сборка и запуск

```bash
# Сборка образа
docker-compose build

# Запуск в фоновом режиме
docker-compose up -d

# Просмотр логов
docker-compose logs -f
```

## 📊 Управление приложением

### Основные команды

```bash
# Запуск
docker-compose up -d

# Остановка
docker-compose down

# Перезапуск
docker-compose restart

# Просмотр логов
docker-compose logs -f

# Просмотр статуса
docker-compose ps

# Пересборка с обновлением кода
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### Обновление приложения

```bash
# 1. Получение новой версии кода
git pull

# 2. Остановка текущего контейнера
docker-compose down

# 3. Пересборка образа
docker-compose build --no-cache

# 4. Запуск обновленной версии
docker-compose up -d
```

## 🔒 Настройка Firewall

### UFW (Ubuntu Firewall)

```bash
# Включение UFW
sudo ufw enable

# Разрешение SSH
sudo ufw allow 22/tcp

# Разрешение Streamlit порта
sudo ufw allow 8501/tcp

# Проверка статуса
sudo ufw status
```

### Изменение порта

Если нужно использовать другой порт (например, 80 или 443):

**Вариант 1: Редактирование docker-compose.yml**
```yaml
ports:
  - "80:8501"  # Внешний порт 80 -> внутренний 8501
```

**Вариант 2: Использование Nginx как reverse proxy**

```bash
# Установка Nginx
sudo apt-get install -y nginx

# Создание конфигурации
sudo nano /etc/nginx/sites-available/dataculture
```

Содержимое конфигурации Nginx:
```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://localhost:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

```bash
# Активация конфигурации
sudo ln -s /etc/nginx/sites-available/dataculture /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

## 🔐 SSL/HTTPS (опционально)

Для использования HTTPS с Let's Encrypt:

```bash
# Установка Certbot
sudo apt-get install -y certbot python3-certbot-nginx

# Получение сертификата
sudo certbot --nginx -d your-domain.com

# Автоматическое обновление
sudo certbot renew --dry-run
```

## 📈 Мониторинг

### Просмотр ресурсов

```bash
# Использование ресурсов контейнером
docker stats dataculture-platform

# Логи с отметками времени
docker-compose logs -f --timestamps

# Последние 100 строк логов
docker-compose logs --tail=100
```

### Автоматический перезапуск

Контейнер настроен с `restart: unless-stopped`, что означает автоматический перезапуск при:
- Ошибке приложения
- Перезагрузке сервера
- Любом сбое контейнера

## 🛠 Решение проблем

### Проблема: Контейнер не запускается

```bash
# Проверка логов
docker-compose logs

# Проверка конфигурации
docker-compose config

# Пересборка без кэша
docker-compose build --no-cache
```

### Проблема: Не найден secrets.toml

```bash
# Проверка наличия файла
ls -la secrets.toml

# Проверка прав доступа
chmod 644 secrets.toml

# Проверка монтирования в контейнере
docker exec dataculture-platform ls -la .streamlit/
```

### Проблема: Порт уже занят

```bash
# Проверка занятых портов
sudo netstat -tulpn | grep 8501

# Остановка процесса на порту
sudo kill -9 <PID>

# Или изменение порта в docker-compose.yml
```

### Проблема: Недостаточно памяти

```bash
# Увеличение лимита памяти в docker-compose.yml
deploy:
  resources:
    limits:
      memory: 4G  # Увеличено до 4GB
```

## 🔄 Backup и восстановление

### Создание backup

```bash
# Backup конфигурации
tar -czf dataculture-backup-$(date +%Y%m%d).tar.gz \
    secrets.toml \
    docker-compose.yml \
    .streamlit/config.toml

# Сохранение образа
docker save dataculture-platform > dataculture-image.tar
```

### Восстановление

```bash
# Распаковка конфигурации
tar -xzf dataculture-backup-YYYYMMDD.tar.gz

# Загрузка образа
docker load < dataculture-image.tar

# Запуск
docker-compose up -d
```

## 📚 Дополнительная информация

### Структура проекта в контейнере

```
/app/
├── app.py                      # Основное приложение
├── requirements.txt            # Зависимости
├── create_peresdachi_table.sql # SQL схема
├── icons/                      # SVG иконки
└── .streamlit/
    ├── config.toml             # Конфигурация UI
    └── secrets.toml            # Секреты (монтируется извне)
```

### Переменные окружения

Можно переопределить через `docker-compose.yml`:
```yaml
environment:
  - STREAMLIT_SERVER_PORT=8501
  - STREAMLIT_SERVER_ADDRESS=0.0.0.0
  - STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
```

### Ограничения ресурсов

Текущие настройки:
- **CPU**: 1-2 cores
- **RAM**: 512MB-2GB
- **Логи**: max 10MB × 3 файла

Можно изменить в `docker-compose.yml` в секции `deploy.resources`.

## 📞 Поддержка

При возникновении проблем:
1. Проверьте логи: `docker-compose logs -f`
2. Проверьте статус: `docker-compose ps`
3. Проверьте healthcheck: `curl http://localhost:8501/_stcore/health`

---

**Powered by Docker 🐳 | Cloud.ru ☁️**
