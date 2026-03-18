# PasarGuardBot (Docker)

Telegram-бот для продажи VPN-подписок через панель PasarGuard.
Оплата через Telegram Stars. Поддержка промокодов, пробного периода, уведомлений об истечении подписки.

## Что хранить в GitHub
В репозиторий кладём код и Docker-файлы.
Секреты **никогда** не коммитим: `.env` должен быть только на VPS.

## Быстрый деплой на VPS (Docker Compose)

### 1) Подготовка папки
```bash
sudo mkdir -p /opt/PasarGuardBot
sudo chown -R $USER:$USER /opt/PasarGuardBot
```

### 2) Клонирование
```bash
cd /opt
git clone <YOUR_GITHUB_REPO_URL> PasarGuardBot
cd /opt/PasarGuardBot
```

### 3) Создать `.env` на VPS
```bash
cp .env.example .env
nano .env
```
Минимум:
- `BOT_TOKEN=...`
- `PANEL_URL=...`
- `PANEL_ADMIN_USERNAME=...`
- `PANEL_ADMIN_PASSWORD=...`

(опционально)
- `SUBS_LINK_TEMPLATE=...`
- `PANEL_PROXY_TYPE=...`
- `PANEL_INBOUND_TAG=...`
- `PANEL_PROXY_FLOW=...`

### 4) Запуск
```bash
sudo docker compose up -d --build
sudo docker compose logs -f
```

## Обновление
```bash
cd /opt/PasarGuardBot
git pull
sudo docker compose up -d --build
```

## Данные
Контейнер использует volume `./data:/data`, поэтому на VPS всё сохраняется в папке `data/`.
