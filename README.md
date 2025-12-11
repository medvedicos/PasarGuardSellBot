# MArzbanBot (Docker)

## Что хранить в GitHub
В репозиторий кладём код и Docker-файлы.
Секреты **никогда** не коммитим: `.env` должен быть только на VPS.

## Быстрый деплой на VPS (Docker Compose)

### 1) Подготовка папки
```bash
sudo mkdir -p /opt/MArzbanBot
sudo chown -R $USER:$USER /opt/MArzbanBot
```

### 2) Клонирование
```bash
cd /opt
git clone <YOUR_GITHUB_REPO_URL> MArzbanBot
cd /opt/MArzbanBot
```

### 3) Создать `.env` на VPS
```bash
nano .env
```
Минимум:
- `BOT_TOKEN=...`
- `MARZBAN_URL=...`
- `MARZBAN_ADMIN_USERNAME=...`
- `MARZBAN_ADMIN_PASSWORD=...`

(опционально)
- `SUBS_LINK_TEMPLATE=...`

### 4) Запуск
```bash
sudo docker compose up -d --build
sudo docker compose logs -f
```

## Обновление
```bash
cd /opt/MArzbanBot
git pull
sudo docker compose up -d --build
```

## Данные
Контейнер использует volume `./data:/data`, поэтому на VPS всё сохраняется в папке `data/`.
