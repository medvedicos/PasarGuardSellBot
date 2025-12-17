import os
import time
import logging
import asyncio
import uuid
import json
import re
from datetime import datetime, timedelta, timezone
import aiohttp
from marzpy import Marzban
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import LabeledPrice, Invoice, PreCheckoutQuery, Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

ADMIN_ID = 430301005

# Users database file for storing user_id -> username mapping
USERS_DB_FILE = "users_db.json"
PLANS_FILE = "plans.json"
PROMOS_FILE = "promos.json"
PROMO_USAGE_FILE = "promo_usage.json"
SETTINGS_FILE = "settings.json"

MAINTENANCE_TEXT = (
    "Бот временно недоступен. Проводятся технические работы. "
    "Просим извинения за доставленные неудобства."
)


def load_settings():
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    if "maintenance_mode" not in data:
                        data["maintenance_mode"] = False
                    if "star_rub_rate" not in data:
                        data["star_rub_rate"] = None
                    if "star_buy_url" not in data:
                        data["star_buy_url"] = "https://t.me/PremiumBot"
                    return data
        except Exception as e:
            logging.error(f"Error loading settings: {e}")
    return {"maintenance_mode": False, "star_rub_rate": None, "star_buy_url": "https://t.me/PremiumBot"}


def save_settings(data: dict):
    try:
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Error saving settings: {e}")


settings = load_settings()


def is_maintenance_mode() -> bool:
    return bool(settings.get("maintenance_mode"))


def set_maintenance_mode(value: bool):
    settings["maintenance_mode"] = bool(value)
    settings["updated_at"] = datetime.now(timezone.utc).isoformat()
    save_settings(settings)


def get_star_rub_rate() -> float | None:
    val = settings.get("star_rub_rate")
    if val is None:
        return None
    try:
        rate = float(val)
    except (TypeError, ValueError):
        return None
    if rate <= 0:
        return None
    return rate


def set_star_rub_rate(value: float | None):
    settings["star_rub_rate"] = value
    settings["updated_at"] = datetime.now(timezone.utc).isoformat()
    save_settings(settings)


def calc_price_rub_from_stars(stars: int) -> int | None:
    rate = get_star_rub_rate()
    if rate is None:
        return None
    try:
        stars_int = int(stars)
    except (TypeError, ValueError):
        return None
    if stars_int < 0:
        return None
    return int(round(stars_int * rate))


def get_discount_percent_for_plan(tg_user: types.User | None, plan_key: str) -> int | None:
    if tg_user is None:
        return None
    mb_username = build_marzban_username(tg_user)
    pending_code = get_user_pending_promo_code(mb_username)
    if not pending_code:
        return None
    promo = get_valid_promo(pending_code)
    if not promo or not promo_applies_to_plan(promo, plan_key):
        return None
    if pending_code in get_used_promos_for_tg_id(tg_user.id):
        return None
    try:
        percent = int(promo.get("percent"))
    except Exception:
        return None
    if percent <= 0 or percent >= 100:
        return None
    return percent


def get_star_buy_url() -> str:
    url = settings.get("star_buy_url")
    if not isinstance(url, str) or not url.strip():
        return "https://t.me/PremiumBot"
    return url.strip()


def set_star_buy_url(url: str):
    settings["star_buy_url"] = (url or "").strip()
    settings["updated_at"] = datetime.now(timezone.utc).isoformat()
    save_settings(settings)


def extract_tg_username(value: str) -> str | None:
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None

    # Accept:
    # - @username
    # - username
    # - https://t.me/username (with optional params)
    # - t.me/username
    s = re.sub(r"^https?://", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^(?:www\.)?", "", s, flags=re.IGNORECASE)

    if s.lower().startswith("t.me/"):
        s = s[5:]
    elif s.lower().startswith("telegram.me/"):
        s = s[12:]

    s = s.strip()
    if s.startswith("@"):
        s = s[1:]

    # take the first path segment
    s = s.split("/", 1)[0]
    s = s.split("?", 1)[0]
    s = s.strip()

    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", s):
        return None
    return s


def format_plan_price_text(plan_key: str, tg_user: types.User | None = None) -> str:
    plan = PLANS.get(plan_key, {})
    title = plan.get("title", plan_key)
    try:
        base_stars = int(plan.get("price"))
    except Exception:
        base_stars = 0

    base_rub = calc_price_rub_from_stars(base_stars)
    if base_rub is None:
        base_rub = plan.get("price_rub")

    percent = get_discount_percent_for_plan(tg_user, plan_key)
    if percent:
        discounted_stars = max(1, (base_stars * (100 - percent)) // 100)
        stars = discounted_stars
        if isinstance(base_rub, (int, float)):
            base_rub_int = int(base_rub)
            rub = max(1, (base_rub_int * (100 - percent)) // 100)
        else:
            rub = None
        discount_part = f" (-{percent}%)"
    else:
        stars = base_stars
        rub = base_rub if isinstance(base_rub, (int, float)) else None
        discount_part = ""

    rub_part = f" (~{int(rub)}₽)" if isinstance(rub, (int, float)) else ""
    return f"🗓 {title} — {stars} ⭐️{rub_part}{discount_part}"


def get_admin_keyboard():
    maintenance_on = is_maintenance_mode()
    maintenance_text = "🛠 Техработы: ВКЛ" if maintenance_on else "🛠 Техработы: ВЫКЛ"
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="💰 Сменить тарифы", callback_data="admin_prices")],
        [types.InlineKeyboardButton(text="🎟 Промокоды", callback_data="admin_promos")],
        [types.InlineKeyboardButton(text=maintenance_text, callback_data="admin_toggle_maintenance")],
    ])
    return kb


class MaintenanceMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if not is_maintenance_mode():
            return await handler(event, data)

        user = data.get("event_from_user") or getattr(event, "from_user", None)
        if user and user.id == ADMIN_ID:
            return await handler(event, data)

        # Do not block successful payment updates, otherwise subscriptions won't be activated.
        if isinstance(event, Message) and event.successful_payment is not None:
            return await handler(event, data)

        # Do not block pre-checkout, otherwise Stars payments won't pass.
        if isinstance(event, PreCheckoutQuery):
            return await handler(event, data)

        if isinstance(event, Message):
            await event.answer(MAINTENANCE_TEXT)
            return

        if isinstance(event, CallbackQuery):
            await event.answer(MAINTENANCE_TEXT, show_alert=True)
            return
        return

def load_users_db():
    """Load users database"""
    if os.path.exists(USERS_DB_FILE):
        try:
            with open(USERS_DB_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading users DB: {e}")
    return {}

def save_users_db(data):
    """Save users database"""
    try:
        with open(USERS_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Error saving users DB: {e}")

# Load users database at startup
users_db = load_users_db()


def load_promos_db():
    if os.path.exists(PROMOS_FILE):
        try:
            with open(PROMOS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading promos DB: {e}")
    return {}


def save_promos_db(data):
    try:
        with open(PROMOS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Error saving promos DB: {e}")


promos_db = load_promos_db()


def load_promo_usage_db():
    if os.path.exists(PROMO_USAGE_FILE):
        try:
            with open(PROMO_USAGE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading promo usage DB: {e}")
    return {}


def save_promo_usage_db(data):
    try:
        with open(PROMO_USAGE_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Error saving promo usage DB: {e}")


promo_usage_db = load_promo_usage_db()


def normalize_promo_code(code: str) -> str:
    return (code or "").strip().upper()


def promo_is_expired(promo: dict) -> bool:
    expires_at = promo.get("expires_at")
    if not expires_at:
        return False
    return int(expires_at) <= int(datetime.now(timezone.utc).timestamp())


def promo_applies_to_plan(promo: dict, plan_key: str) -> bool:
    plans = promo.get("plans")
    if not plans:
        return False
    if isinstance(plans, str) and plans == "*":
        return True
    if isinstance(plans, list) and "*" in plans:
        return True
    if isinstance(plans, list):
        return plan_key in plans
    return False


def get_valid_promo(code: str):
    code = normalize_promo_code(code)
    if not code:
        return None
    promo = promos_db.get(code)
    if not isinstance(promo, dict):
        return None
    if promo.get("active") is False:
        return None
    if promo_is_expired(promo):
        return None
    percent = promo.get("percent")
    if not isinstance(percent, int) or percent <= 0 or percent >= 100:
        return None
    return promo


def get_used_promos_for_tg_id(tg_id: int):
    entry = promo_usage_db.get(str(tg_id), {})
    used = entry.get("used_promos")
    if isinstance(used, list):
        return [normalize_promo_code(x) for x in used if isinstance(x, str)]
    return []


def mark_promo_used_for_tg_id(tg_id: int, code: str):
    code = normalize_promo_code(code)
    if not code:
        return
    key = str(tg_id)
    promo_usage_db.setdefault(key, {})
    used = promo_usage_db[key].get("used_promos")
    if not isinstance(used, list):
        used = []
    normalized_used = [normalize_promo_code(x) for x in used if isinstance(x, str)]
    if code not in normalized_used:
        used.append(code)
    promo_usage_db[key]["used_promos"] = used
    promo_usage_db[key]["updated_at"] = datetime.now(timezone.utc).isoformat()
    save_promo_usage_db(promo_usage_db)


def get_user_pending_promo_code(mb_username: str):
    entry = users_db.get(mb_username, {})
    code = entry.get("pending_promo")
    if isinstance(code, str) and code.strip():
        return normalize_promo_code(code)
    return None

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # e.g. https://yourdomain.com/webhook/telegram
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8443"))

MARZBAN_URL = os.getenv("MARZBAN_URL", "https://misavpn.top")
MARZBAN_ADMIN_USERNAME = os.getenv("MARZBAN_ADMIN_USERNAME")
MARZBAN_ADMIN_PASSWORD = os.getenv("MARZBAN_ADMIN_PASSWORD")
SUBS_LINK_TEMPLATE = os.getenv("SUBS_LINK_TEMPLATE", f"{MARZBAN_URL}/vpnsubs/{{username}}")

if not BOT_TOKEN:
    print("⚠️ BOT_TOKEN не установлен в .env файле")
    print("Скопируйте токен от BotFather и добавьте в .env:")
    print("BOT_TOKEN=your_token_here")
    import sys
    sys.exit(1)

# Fix SSL certificate verification issue on Windows BEFORE creating Bot
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# Initialize without session - will create it in async context
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Block non-admin messages during maintenance
dp.message.middleware(MaintenanceMiddleware())
dp.callback_query.middleware(MaintenanceMiddleware())
dp.pre_checkout_query.middleware(MaintenanceMiddleware())


def build_admin_promo_manage_view(code: str):
    code = normalize_promo_code(code)
    promo = promos_db.get(code)
    if not isinstance(promo, dict):
        return None, None

    percent = promo.get("percent")
    plans = promo.get("plans")
    expires_at = promo.get("expires_at")
    active = promo.get("active", True)

    if plans == "*" or (isinstance(plans, list) and "*" in plans):
        plans_text = "все тарифы"
    elif isinstance(plans, list):
        plans_text = ", ".join(plans)
    else:
        plans_text = "—"

    if expires_at:
        exp_dt = datetime.fromtimestamp(int(expires_at), tz=timezone.utc)
        exp_text = exp_dt.strftime("%d.%m.%Y")
    else:
        exp_text = "без срока"

    status_text = "✅ активен" if active else "⛔ отключён"

    text = (
        "🎟 <b>Промокод</b>\n\n"
        "<b>Код (нажмите, чтобы скопировать):</b>\n"
        f"<code>{code}</code>\n\n"
        f"<b>Скидка:</b> {percent}%\n"
        f"<b>Тарифы:</b> {plans_text}\n"
        f"<b>Срок:</b> до {exp_text}\n"
        f"<b>Статус:</b> {status_text}"
    )

    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⛔ Отключить", callback_data=f"admin_promo_disable:{code}"),
         types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"admin_promo_delete:{code}")],
        [types.InlineKeyboardButton(text="🔙 К списку", callback_data="admin_promo_list")],
    ])
    return text, kb

# Dictionary to track sent notifications (to avoid duplicates)
# Format: {user_id: {timestamp_threshold: True, ...}}
sent_notifications = {}

# product catalog (prices in Stars / XTR)
def load_plans():
    """Load plans from file or return defaults"""
    defaults = {
        "m1": {"title": "1 месяц", "days": 30, "price": 1, "price_rub": 259},
        "m3": {"title": "3 месяца", "days": 90, "price": 500, "price_rub": 829},
        "m6": {"title": "6 месяцев", "days": 180, "price": 1000, "price_rub": 1649},
        "y1": {"title": "1 год", "days": 365, "price": 2000, "price_rub": 3298},
    }
    
    if os.path.exists(PLANS_FILE):
        try:
            with open(PLANS_FILE, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
                # Merge defaults to ensure new fields exist
                for key, val in defaults.items():
                    if key in loaded:
                        if "price_rub" not in loaded[key]:
                            loaded[key]["price_rub"] = val["price_rub"]
                    else:
                        loaded[key] = val
                return loaded
        except Exception as e:
            logging.error(f"Error loading plans: {e}")
    
    return defaults

def save_plans(data):
    """Save plans to file"""
    try:
        with open(PLANS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Error saving plans: {e}")

PLANS = load_plans()

class AdminStates(StatesGroup):
    waiting_for_price = State()
    waiting_for_rate = State()
    waiting_for_star_buy_bot = State()


class AdminPromoStates(StatesGroup):
    waiting_for_code = State()
    waiting_for_percent = State()
    waiting_for_plans = State()
    waiting_for_days = State()


class PromoUserStates(StatesGroup):
    waiting_for_code = State()

# Get user subscription info from Marzban
async def marzban_get_user(username: str):
    """Fetch user info from Marzban"""
    try:
        api = Marzban(username=MARZBAN_ADMIN_USERNAME, password=MARZBAN_ADMIN_PASSWORD, panel_address=MARZBAN_URL)
        token_data = await api.get_token()
        token = token_data.get("access_token") if isinstance(token_data, dict) else token_data.access_token
        
        if not token:
            logging.error("Failed to get Marzban token")
            return None
        
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{MARZBAN_URL}/api/user/{username}"
            
            async with session.get(url, headers=headers, ssl=False) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logging.warning(f"Could not fetch user {username}: {resp.status}")
                    return None
    except Exception as e:
        logging.error(f"Error fetching user {username}: {e}")
        return None

# Format user info for display
def format_user_info(user_data: dict) -> str:
    """Format user subscription info for display"""
    if not user_data:
        return "❌ Подписка не найдена"
    
    username = user_data.get("username", "N/A")
    status = user_data.get("status", "unknown")
    expire = user_data.get("expire")
    used_traffic = user_data.get("used_traffic", 0)
    data_limit = user_data.get("data_limit")
    
    # Format status
    status_emoji = "✅" if status == "active" else "⛔"
    
    # Format expire date
    if expire:
        expire_dt = datetime.fromtimestamp(expire, tz=timezone.utc)
        expire_date = expire_dt.strftime("%d.%m.%Y")
        days_left = (expire_dt - datetime.now(timezone.utc)).days
        expire_text = f"{expire_date} ({days_left} дней)"
    else:
        expire_text = "Не установлено"
    
    # Format traffic
    def format_bytes(bytes_val):
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024:
                return f"{bytes_val:.1f} {unit}"
            bytes_val /= 1024
        return f"{bytes_val:.1f} PB"
    
    used_text = format_bytes(used_traffic) if used_traffic else "0 B"
    limit_text = format_bytes(data_limit) if data_limit else "∞"
    
    info = (
        f"👤 <b>Пользователь:</b> <code>{username}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"{status_emoji} <b>Статус:</b> {status.upper()}\n"
        f"📅 <b>Истекает:</b> {expire_text}\n"
        f"📊 <b>Трафик:</b> {used_text} / {limit_text}\n"
        f"━━━━━━━━━━━━━━━━━━━"
    )
    
    return info

# Create main menu keyboard
def get_main_keyboard():
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="👤 Личный кабинет", callback_data="cabinet")],
        [types.InlineKeyboardButton(text="💎 Купить подписку", callback_data="buy_menu")],
        [types.InlineKeyboardButton(text="🎁 Пробный период (3 дня)", callback_data="trial_subs")],
        [types.InlineKeyboardButton(text="🆘 Поддержка", url="https://t.me/Mizuvil")],
    ])
    return kb

# Create cabinet keyboard
def get_cabinet_keyboard(subs_link: str | None = None):
    rows = []
    if subs_link:
        rows.append([types.InlineKeyboardButton(text="🌍 Открыть подписку", url=subs_link)])

    rows.extend([
        [types.InlineKeyboardButton(text="💎 Купить/продлить", callback_data="renew_menu")],
        [types.InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_menu")],
    ])

    return types.InlineKeyboardMarkup(inline_keyboard=rows)

# Create buy menu keyboard
def get_buy_keyboard(tg_user: types.User | None = None):
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=format_plan_price_text("m1", tg_user=tg_user), callback_data="buy:m1")],
        [types.InlineKeyboardButton(text=format_plan_price_text("m3", tg_user=tg_user), callback_data="buy:m3")],
        [types.InlineKeyboardButton(text=format_plan_price_text("m6", tg_user=tg_user), callback_data="buy:m6")],
        [types.InlineKeyboardButton(text=format_plan_price_text("y1", tg_user=tg_user), callback_data="buy:y1")],
        [types.InlineKeyboardButton(text="⭐️ Купить звезды", url=get_star_buy_url())],
        [types.InlineKeyboardButton(text="🎟 Ввести промокод", callback_data="promo_enter:buy")],
        [types.InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")],
    ])
    return kb

# create user in marzban
async def marzban_create_user(username: str, expire_ts: int):
    try:
        api = Marzban(username=MARZBAN_ADMIN_USERNAME, password=MARZBAN_ADMIN_PASSWORD, panel_address=MARZBAN_URL)
        token_data = await api.get_token()
        token = token_data.get("access_token") if isinstance(token_data, dict) else token_data.access_token
        
        if not token:
            logging.error("Failed to get Marzban token")
            return None
        
        logging.info(f"Создаём пользователя: {username}, expire_ts: {expire_ts}")
        
        # Use direct HTTP POST instead of marzpy.add_user which has bugs
        # Create payload for direct API call
        payload = {
            "username": username,
            "proxies": {
                "vless": {
                    "id": str(uuid.uuid4()),
                    "flow": "xtls-rprx-vision"
                }
            },
            "inbounds": {"vless": ["Steal"]},
            "data_limit": 0,
            "expire": expire_ts,
            "status": "active"
        }
        
        logging.info(f"📤 Payload: {payload}")
        
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{MARZBAN_URL}/api/user"
            
            async with session.post(url, json=payload, headers=headers, ssl=False) as resp:
                logging.info(f"   API Response status: {resp.status}")
                data = await resp.json()
                
                if resp.status in (200, 201):
                    logging.info(f"✓ User created successfully via HTTP")
                    # Query user immediately to get final subscription_url (Marzban regenerates it on each query)
                    # This ensures we get the same URL that admin panel will show
                    await asyncio.sleep(0.1)  # Small delay to ensure server has fully written the user
                    
                    get_url = f"{MARZBAN_URL}/api/user/{username}"
                    async with session.get(get_url, headers=headers, ssl=False) as get_resp:
                        if get_resp.status == 200:
                            user_data = await get_resp.json()
                            logging.info(f"✓ Retrieved fresh user data with subscription_url")
                            return user_data
                        else:
                            logging.warning(f"Could not fetch user after creation (status {get_resp.status}), using create response")
                            return data
                elif resp.status == 409:
                    # User already exists, update instead of create
                    logging.info(f"ℹ User {username} already exists (409), updating expiry instead")
                    return await marzban_update_user(username, expire_ts)
                else:
                    logging.error(f"✗ Failed to create user: {resp.status}")
                    logging.error(f"   Response: {data}")
                    return None
            
    except Exception as e:
        logging.error(f"Ошибка создания пользователя: {type(e).__name__}: {e}", exc_info=True)
        return None

# Update user subscription (extend expiry date)
async def marzban_update_user(username: str, new_expire_ts: int):
    """Update user expiry date in Marzban"""
    try:
        api = Marzban(username=MARZBAN_ADMIN_USERNAME, password=MARZBAN_ADMIN_PASSWORD, panel_address=MARZBAN_URL)
        token_data = await api.get_token()
        token = token_data.get("access_token") if isinstance(token_data, dict) else token_data.access_token
        
        if not token:
            logging.error("Failed to get Marzban token")
            return None
        
        logging.info(f"Обновляем подписку: {username}, new_expire_ts: {new_expire_ts}")
        
        # Create payload for update (only expire field)
        payload = {
            "expire": new_expire_ts
        }
        
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{MARZBAN_URL}/api/user/{username}"
            
            async with session.put(url, json=payload, headers=headers, ssl=False) as resp:
                logging.info(f"   API Response status: {resp.status}")
                data = await resp.json()
                
                if resp.status in (200, 201):
                    logging.info(f"✓ User updated successfully")
                    # Get fresh data
                    async with session.get(url, headers=headers, ssl=False) as get_resp:
                        if get_resp.status == 200:
                            user_data = await get_resp.json()
                            logging.info(f"✓ Retrieved fresh user data after update")
                            return user_data
                        else:
                            return data
                else:
                    logging.error(f"✗ Failed to update user: {resp.status}")
                    logging.error(f"   Response: {data}")
                    return None
            
    except Exception as e:
        logging.error(f"Ошибка обновления подписки: {type(e).__name__}: {e}", exc_info=True)
        return None
def build_marzban_username(tg_user: types.User):
    if tg_user.username:
        # normalize: allowed a-z,0-9, underscore, 3-32 chars for many marzban rules
        raw = tg_user.username.lower()
        # keep only allowed chars:
        import re
        clean = re.sub(r'[^a-z0-9_]', '_', raw)[:28]
        return clean
    else:
        return f"user_{tg_user.id}"

# Admin command
@dp.message(Command("admin"))
async def cmd_admin(m: Message):
    if m.from_user.id != ADMIN_ID:
        return

    await m.answer("🛠 <b>Админ-панель</b>", reply_markup=get_admin_keyboard(), parse_mode="HTML")


@dp.callback_query(lambda cq: cq.data == "admin_toggle_maintenance")
async def cb_admin_toggle_maintenance(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    set_maintenance_mode(not is_maintenance_mode())
    try:
        await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=get_admin_keyboard(), parse_mode="HTML")
    except Exception as e:
        if "not modified" not in str(e):
            logging.error(f"Error editing admin panel message: {e}")
    await cq.answer("✅")


@dp.callback_query(lambda cq: cq.data == "admin_promos")
async def cb_admin_promos(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="➕ Создать промокод", callback_data="admin_promo_create")],
        [types.InlineKeyboardButton(text="📋 Список промокодов", callback_data="admin_promo_list")],
        [types.InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")],
    ])
    await cq.message.edit_text("🎟 <b>Промокоды</b>", reply_markup=kb, parse_mode="HTML")


@dp.callback_query(lambda cq: cq.data == "admin_promo_list")
async def cb_admin_promo_list(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    active_lines = []
    buttons = []
    now_ts = int(datetime.now(timezone.utc).timestamp())

    for code, promo in promos_db.items():
        if not isinstance(promo, dict):
            continue
        if promo.get("active") is False:
            continue
        expires_at = promo.get("expires_at")
        if expires_at and int(expires_at) <= now_ts:
            continue
        percent = promo.get("percent")
        plans = promo.get("plans")
        if plans == "*" or (isinstance(plans, list) and "*" in plans):
            plans_text = "все тарифы"
        elif isinstance(plans, list):
            plans_text = ", ".join(plans)
        else:
            plans_text = "—"

        if expires_at:
            exp_dt = datetime.fromtimestamp(int(expires_at), tz=timezone.utc)
            exp_text = exp_dt.strftime("%d.%m.%Y")
        else:
            exp_text = "без срока"

        active_lines.append(f"• <code>{code}</code> — {percent}% ({plans_text}), до {exp_text}")
        # Manage button (keep callback short; promo code is capped on creation)
        buttons.append([types.InlineKeyboardButton(text=f"⚙️ {code}", callback_data=f"admin_promo_manage:{code}")])

    text = "🎟 <b>Активные промокоды:</b>\n\n"
    if active_lines:
        text += "\n".join(active_lines)
    else:
        text += "Пока нет активных промокодов."

    # Put manage buttons first, then actions
    buttons.append([types.InlineKeyboardButton(text="➕ Создать промокод", callback_data="admin_promo_create")])
    buttons.append([types.InlineKeyboardButton(text="🔙 Назад", callback_data="admin_promos")])
    kb = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    await cq.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@dp.callback_query(lambda cq: cq.data.startswith("admin_promo_manage:"))
async def cb_admin_promo_manage(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    code = normalize_promo_code(cq.data.split(":", 1)[1])
    text, kb = build_admin_promo_manage_view(code)
    if not text:
        await cq.answer("Промокод не найден", show_alert=True)
        return
    await cq.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@dp.callback_query(lambda cq: cq.data.startswith("admin_promo_disable:"))
async def cb_admin_promo_disable(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    code = normalize_promo_code(cq.data.split(":", 1)[1])
    promo = promos_db.get(code)
    if not isinstance(promo, dict):
        await cq.answer("Промокод не найден", show_alert=True)
        return

    promo["active"] = False
    promos_db[code] = promo
    save_promos_db(promos_db)
    await cq.answer("✅ Промокод отключён", show_alert=True)
    await cb_admin_promo_manage(cq)


@dp.callback_query(lambda cq: cq.data.startswith("admin_promo_delete:"))
async def cb_admin_promo_delete(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    code = normalize_promo_code(cq.data.split(":", 1)[1])
    if code not in promos_db:
        await cq.answer("Промокод не найден", show_alert=True)
        return

    promos_db.pop(code, None)
    save_promos_db(promos_db)
    await cq.answer("🗑 Промокод удалён", show_alert=True)
    await cb_admin_promo_list(cq)


@dp.callback_query(lambda cq: cq.data == "admin_promo_create")
async def cb_admin_promo_create(cq: types.CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return

    await state.clear()
    await state.set_state(AdminPromoStates.waiting_for_code)
    await cq.message.edit_text(
        "➕ <b>Создание промокода</b>\n\n"
        "1) Введите промокод (например: <code>NEWYEAR</code>)\n"
        "• Лучше без пробелов\n",
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="admin_promos")]
        ])
    )


@dp.message(AdminPromoStates.waiting_for_code)
async def admin_promo_code(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return

    code = normalize_promo_code(m.text)
    if not code or len(code) < 3:
        await m.answer("❌ Введите промокод минимум из 3 символов.")
        return

    # keep callback_data safe (Telegram limit is 64 bytes)
    if len(code) > 32:
        await m.answer("❌ Слишком длинный промокод. Максимум 32 символа.")
        return

    await state.update_data(code=code)
    await state.set_state(AdminPromoStates.waiting_for_percent)
    await m.answer(
        f"2) Введите скидку в процентах для <code>{code}</code> (1-99)",
        parse_mode="HTML"
    )


@dp.message(AdminPromoStates.waiting_for_percent)
async def admin_promo_percent(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return

    try:
        percent = int(m.text)
        if percent <= 0 or percent >= 100:
            raise ValueError
    except Exception:
        await m.answer("❌ Введите число от 1 до 99.")
        return

    await state.update_data(percent=percent)
    await state.set_state(AdminPromoStates.waiting_for_plans)
    await m.answer(
        "3) На какие тарифы действует?\n\n"
        "• Напишите <code>all</code> — на все\n"
        "• Или список ключей через запятую: <code>m1,m3,m6,y1</code>",
        parse_mode="HTML"
    )


@dp.message(AdminPromoStates.waiting_for_plans)
async def admin_promo_plans(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return

    raw = (m.text or "").strip().lower()
    if raw in ("all", "*", "все"):
        plans = ["*"]
    else:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        unknown = [p for p in parts if p not in PLANS]
        if unknown:
            await m.answer(f"❌ Неизвестные планы: {', '.join(unknown)}. Доступно: {', '.join(PLANS.keys())}")
            return
        if not parts:
            await m.answer("❌ Введите 'all' или список планов через запятую.")
            return
        plans = parts

    await state.update_data(plans=plans)
    await state.set_state(AdminPromoStates.waiting_for_days)
    await m.answer("4) Срок жизни в днях (например: <code>7</code>)", parse_mode="HTML")


@dp.message(AdminPromoStates.waiting_for_days)
async def admin_promo_days(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return

    try:
        days = int(m.text)
        if days <= 0:
            raise ValueError
    except Exception:
        await m.answer("❌ Введите количество дней (целое число > 0).")
        return

    data = await state.get_data()
    code = data.get("code")
    percent = data.get("percent")
    plans = data.get("plans")

    expires_at = int((datetime.now(timezone.utc) + timedelta(days=days)).timestamp())
    promos_db[code] = {
        "percent": int(percent),
        "plans": plans,
        "expires_at": expires_at,
        "active": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": m.from_user.id,
    }
    save_promos_db(promos_db)

    await state.clear()

    # After creation, open promo manage menu immediately (with copy-friendly code rendering)
    text, kb = build_admin_promo_manage_view(code)
    if text and kb:
        await m.answer(text, parse_mode="HTML", reply_markup=kb)
    else:
        exp_dt = datetime.fromtimestamp(expires_at, tz=timezone.utc)
        await m.answer(
            f"✅ Промокод <code>{code}</code> создан: -{percent}% до {exp_dt.strftime('%d.%m.%Y')}",
            parse_mode="HTML"
        )

@dp.callback_query(lambda cq: cq.data == "admin_prices")
async def cb_admin_prices(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    rate = get_star_rub_rate()
    rate_text = f"{rate:g} ₽/⭐️" if isinstance(rate, (int, float)) else "не задан"

    star_buy_url = get_star_buy_url()
    star_buy_username = extract_tg_username(star_buy_url) or extract_tg_username(star_buy_url.replace("https://", ""))
    star_buy_text = f"@{star_buy_username}" if star_buy_username else star_buy_url

    buttons = []
    buttons.append([types.InlineKeyboardButton(text=f"💱 Курс ₽/⭐️: {rate_text}", callback_data="admin_edit_rate")])
    buttons.append([types.InlineKeyboardButton(text=f"⭐️ Бот покупки звёзд: {star_buy_text}", callback_data="admin_edit_star_buy_bot")])
    for key, plan in PLANS.items():
        buttons.append([types.InlineKeyboardButton(
            text=f"{plan['title']} — {plan['price']} ⭐️", 
            callback_data=f"edit_price:{key}"
        )])
    
    buttons.append([types.InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")])
    kb = types.InlineKeyboardMarkup(inline_keyboard=buttons)

    await cq.message.edit_text(
        "💰 <b>Настройка тарифов</b>\n\n"
        "Выберите тариф для изменения цены (в ⭐️), или задайте курс ₽/⭐️.",
        reply_markup=kb,
        parse_mode="HTML",
    )


@dp.callback_query(lambda cq: cq.data == "admin_edit_star_buy_bot")
async def cb_admin_edit_star_buy_bot(cq: types.CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return

    current_url = get_star_buy_url()
    current_username = extract_tg_username(current_url) or ""
    current_display = f"@{current_username}" if current_username else current_url

    await state.clear()
    await state.set_state(AdminStates.waiting_for_star_buy_bot)
    await cq.message.edit_text(
        "⭐️ <b>Бот для покупки звёзд</b>\n\n"
        f"Текущий: <b>{current_display}</b>\n\n"
        "Отправьте username (например <code>@PremiumBot</code>) или ссылку (например <code>https://t.me/PremiumBot</code>).",
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel_edit")]
        ]),
    )
    await cq.answer()


@dp.callback_query(lambda cq: cq.data == "admin_edit_rate")
async def cb_admin_edit_rate(cq: types.CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return

    rate = get_star_rub_rate()
    current = f"{rate:g}" if isinstance(rate, (int, float)) else "не задан"
    await state.clear()
    await state.set_state(AdminStates.waiting_for_rate)
    await cq.message.edit_text(
        "💱 <b>Курс ₽/⭐️</b>\n\n"
        f"Текущий курс: <b>{current}</b>\n\n"
        "Введите курс в рублях за 1 ⭐️ (например: <code>2.6</code>).",
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel_edit")]
        ]),
    )
    await cq.answer()

@dp.callback_query(lambda cq: cq.data == "admin_back")
async def cb_admin_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=get_admin_keyboard(), parse_mode="HTML")

@dp.callback_query(lambda cq: cq.data.startswith("edit_price:"))
async def cb_admin_edit_price(cq: types.CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
        
    plan_key = cq.data.split(":")[1]
    plan = PLANS.get(plan_key)
    
    if not plan:
        await cq.answer("Тариф не найден", show_alert=True)
        return
        
    await state.update_data(plan_key=plan_key)
    await state.set_state(AdminStates.waiting_for_price)
    
    await cq.message.edit_text(
        f"✏️ Введите новую цену для тарифа <b>{plan['title']}</b> (в Telegram Stars):", 
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel_edit")]
        ])
    )

@dp.callback_query(lambda cq: cq.data == "admin_cancel_edit")
async def cb_admin_cancel_edit(cq: types.CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
        
    await state.clear()
    await cb_admin_prices(cq)

@dp.message(AdminStates.waiting_for_price)
async def process_new_price(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return
        
    try:
        new_price = int(m.text)
        if new_price < 1:
            raise ValueError
    except ValueError:
        await m.answer("❌ Пожалуйста, введите корректное число (больше 0).")
        return
        
    data = await state.get_data()
    plan_key = data.get("plan_key")
    
    if plan_key in PLANS:
        PLANS[plan_key]["price"] = new_price

        rub = calc_price_rub_from_stars(new_price)
        if rub is not None:
            PLANS[plan_key]["price_rub"] = rub
        save_plans(PLANS)
        
        await m.answer(f"✅ Цена для тарифа <b>{PLANS[plan_key]['title']}</b> изменена на {new_price} ⭐️", parse_mode="HTML")
        
        # Show admin menu again
        await m.answer("🛠 <b>Админ-панель</b>", reply_markup=get_admin_keyboard(), parse_mode="HTML")
    else:
        await m.answer("❌ Ошибка: тариф не найден.")
        
    await state.clear()


@dp.message(AdminStates.waiting_for_rate)
async def process_new_rate(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return

    raw = (m.text or "").strip().replace(",", ".")
    try:
        rate = float(raw)
        if rate <= 0:
            raise ValueError
    except ValueError:
        await m.answer("❌ Пожалуйста, введите корректное число (например 2.6).")
        return

    set_star_rub_rate(rate)

    # Keep stored RUB prices consistent with the new rate
    for _, plan in PLANS.items():
        rub = calc_price_rub_from_stars(plan.get("price"))
        if rub is not None:
            plan["price_rub"] = rub
    save_plans(PLANS)

    await m.answer(f"✅ Курс установлен: <b>{rate:g}</b> ₽ за 1 ⭐️", parse_mode="HTML")
    await m.answer("🛠 <b>Админ-панель</b>", reply_markup=get_admin_keyboard(), parse_mode="HTML")
    await state.clear()


@dp.message(AdminStates.waiting_for_star_buy_bot)
async def process_new_star_buy_bot(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return

    username = extract_tg_username(m.text or "")
    if not username:
        await m.answer(
            "❌ Не удалось распознать username.\n\n"
            "Пример: <code>@PremiumBot</code> или <code>https://t.me/PremiumBot</code>",
            parse_mode="HTML",
        )
        return

    set_star_buy_url(f"https://t.me/{username}")
    await m.answer(f"✅ Бот для покупки звёзд установлен: <b>@{username}</b>", parse_mode="HTML")
    await m.answer("🛠 <b>Админ-панель</b>", reply_markup=get_admin_keyboard(), parse_mode="HTML")
    await state.clear()

# start
@dp.message(Command("start"))
async def cmd_start(m: Message):
    logging.info(f"Команда /start от {m.from_user.username or m.from_user.id}")
    text = (
        "👋 <b>Привет! Я MiSa VPN Bot</b> — твой проводник в свободный интернет!\n\n"
        "🚀 <b>Почему выбирают нас?</b>\n"
        "• Высокая скорость без ограничений\n"
        "• Работает Instagram, YouTube, Netflix и др.\n"
        "• Анонимность и защита твоих данных\n"
        "• Простая настройка за 1 минуту\n\n"
        "👇 <b>Жми кнопку ниже, чтобы подключиться:</b>"
    )
    await m.answer(text, reply_markup=get_main_keyboard(), parse_mode="HTML")

# user pressed button in main menu
@dp.callback_query(lambda cq: cq.data == "cabinet")
async def cb_cabinet(cq: types.CallbackQuery):
    """Show user cabinet with subscription info"""
    tg_user = cq.from_user
    mb_username = build_marzban_username(tg_user)
    
    # Get user info from Marzban
    user_data = await marzban_get_user(mb_username)
    
    subs_link = None
    if user_data is None:
        text = (
            "👤 <b>Личный кабинет</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n\n"
            "❌ <b>Подписка не активна</b>\n\n"
            "Чтобы получить доступ к VPN, оформите подписку или используйте пробный период."
        )
    else:
        text = (
            "👤 <b>Личный кабинет</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n\n"
            f"{format_user_info(user_data)}\n\n"
            "🌍 <i>Подключение:</i> нажмите кнопку <b>Открыть подписку</b> ниже."
        )
        subs_link = user_data.get("subscription_url")
        if not subs_link:
            subs_link = SUBS_LINK_TEMPLATE.format(username=mb_username)
    
    # Edit previous message instead of sending new one
    try:
        await cq.message.edit_text(text, reply_markup=get_cabinet_keyboard(subs_link=subs_link), parse_mode="HTML")
    except Exception as e:
        if "not modified" in str(e):
            # Message is the same, just answer with notification
            await cq.answer("✅", show_alert=False)
        else:
            logging.error(f"Error editing message: {e}")
            await cq.answer("❌ Ошибка при обновлении", show_alert=True)
    else:
        await cq.answer()

@dp.callback_query(lambda cq: cq.data == "buy_menu")
async def cb_buy_menu(cq: types.CallbackQuery):
    """Show buy menu"""
    tg_user = cq.from_user
    text = build_buy_menu_text(tg_user)
    try:
        await cq.message.edit_text(text, reply_markup=get_buy_keyboard(tg_user), parse_mode="HTML")
    except Exception as e:
        if "not modified" not in str(e):
            logging.error(f"Error editing message: {e}")
        await cq.answer()
    else:
        await cq.answer()


@dp.callback_query(lambda cq: cq.data.startswith("promo_enter:"))
async def cb_promo_enter(cq: types.CallbackQuery, state: FSMContext):
    scope = cq.data.split(":", 1)[1]
    await state.clear()
    await state.update_data(promo_scope=scope)
    await state.set_state(PromoUserStates.waiting_for_code)
    await cq.message.edit_text(
        "🎟 <b>Введите промокод</b>\n\n"
        "Отправьте промокод одним сообщением.",
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🔙 Назад", callback_data="buy_menu" if scope == "buy" else "renew_menu")]
        ])
    )
    await cq.answer()


@dp.message(PromoUserStates.waiting_for_code, F.text)
async def promo_user_entered(m: Message, state: FSMContext):
    tg_user = m.from_user
    mb_username = build_marzban_username(tg_user)
    code = normalize_promo_code(m.text)

    data = await state.get_data()
    scope = data.get("promo_scope")

    promo = get_valid_promo(code)
    if not promo:
        await m.answer("❌ Промокод недействителен или истёк.")
        return

    used = get_used_promos_for_tg_id(tg_user.id)
    if code in used:
        await m.answer("❌ Этот промокод уже использован на вашем аккаунте.")
        return

    users_db.setdefault(mb_username, {})
    users_db[mb_username]["pending_promo"] = code
    users_db[mb_username]["pending_promo_set_at"] = datetime.now(timezone.utc).isoformat()
    save_users_db(users_db)

    expires_at = promo.get("expires_at")
    exp_txt = ""
    if expires_at:
        exp_dt = datetime.fromtimestamp(int(expires_at), tz=timezone.utc)
        exp_txt = f" (до {exp_dt.strftime('%d.%m.%Y')})"

    await state.clear()
    await m.answer(
        f"✅ Промокод <code>{code}</code> применён: -{promo['percent']}%{exp_txt}\n"
        f"Скидка будет учтена при оплате подходящего тарифа.",
        parse_mode="HTML"
    )

    if scope == "renew":
        text = f"🔄 <b>Продление подписки:</b>\n\nВыберите срок продления:\n\n🎟 <b>Промокод:</b> <code>{code}</code> (-{promo['percent']}%){exp_txt}"
        await m.answer(text, reply_markup=get_renew_keyboard(tg_user), parse_mode="HTML")
    else:
        text = (
            "💎 <b>Выберите тарифный план:</b>\n\n"
            "⚡️ Высокая скорость\n"
            "🌍 Локации по всему миру\n"
            "♾ Безлимитный трафик\n\n"
            f"🎟 <b>Промокод:</b> <code>{code}</code> (-{promo['percent']}%){exp_txt}"
        )
        await m.answer(text, reply_markup=get_buy_keyboard(tg_user), parse_mode="HTML")

@dp.callback_query(lambda cq: cq.data == "trial_subs")
async def cb_trial_subs(cq: types.CallbackQuery):
    """Activate trial subscription"""
    tg_user = cq.from_user
    mb_username = build_marzban_username(tg_user)
    
    # Check if trial already used
    user_info = users_db.get(mb_username, {})
    if user_info.get("trial_used"):
        await cq.answer("❌ Вы уже использовали пробный период!", show_alert=True)
        return

    # Check if user already has active subscription
    user_data = await marzban_get_user(mb_username)
    if user_data and user_data.get("status") == "active":
        expire_ts = user_data.get("expire")
        if expire_ts and expire_ts > datetime.now(timezone.utc).timestamp():
            await cq.answer("❌ У вас уже есть активная подписка!", show_alert=True)
            return

    # Activate trial
    days = 3
    expire_dt = datetime.now(timezone.utc) + timedelta(days=days)
    expire_ts = int(expire_dt.timestamp())
    
    # Create or update user
    if user_data:
        res = await marzban_update_user(mb_username, expire_ts)
    else:
        res = await marzban_create_user(mb_username, expire_ts)
        
    if res:
        # Update DB
        if mb_username not in users_db:
            users_db[mb_username] = {}
            
        users_db[mb_username].update({
            "tg_id": tg_user.id,
            "tg_username": tg_user.username,
            "expire_ts": expire_ts,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "trial_used": True
        })
        save_users_db(users_db)
        
        subs_link = res.get("subscription_url")
        if not subs_link:
            subs_link = SUBS_LINK_TEMPLATE.format(username=mb_username)
            
        text = (
            f"🎁 <b>Пробный период активирован!</b>\n\n"
            f"📅 <b>Действует до:</b> {expire_dt.strftime('%d.%m.%Y')}\n\n"
            f"🔗 <b>Ваша ссылка для подключения:</b>\n<code>{subs_link}</code>\n\n"
            f"💡 Вставьте её в ваше VPN-приложение."
        )
        await cq.message.edit_text(text, reply_markup=get_main_keyboard(), parse_mode="HTML")
    else:
        await cq.answer("❌ Ошибка активации. Попробуйте позже.", show_alert=True)

@dp.callback_query(lambda cq: cq.data == "back_to_menu")
async def cb_back_to_menu(cq: types.CallbackQuery):
    """Go back to main menu"""
    text = (
        "👋 <b>Привет! Я MiSa VPN Bot</b> — твой проводник в свободный интернет!\n\n"
        "🚀 <b>Почему выбирают нас?</b>\n"
        "• Высокая скорость без ограничений\n"
        "• Работает Instagram, YouTube, Netflix и др.\n"
        "• Анонимность и защита твоих данных\n"
        "• Простая настройка за 1 минуту\n\n"
        "👇 <b>Жми кнопку ниже, чтобы подключиться:</b>"
    )
    try:
        await cq.message.edit_text(text, reply_markup=get_main_keyboard(), parse_mode="HTML")
    except Exception as e:
        if "not modified" not in str(e):
            logging.error(f"Error editing message: {e}")
        await cq.answer()
    else:
        await cq.answer()

@dp.callback_query(lambda cq: cq.data == "get_link")
async def cb_get_link(cq: types.CallbackQuery):
    """Send subscription link"""
    tg_user = cq.from_user
    mb_username = build_marzban_username(tg_user)
    
    # Get user info from Marzban
    user_data = await marzban_get_user(mb_username)
    
    if user_data is None:
        text = (
            "❌ <b>Подписка не найдена</b>\n\n"
            "У вас нет активной подписки. Купите подписку, чтобы начать использовать VPN."
        )
        try:
            await cq.message.edit_text(text, reply_markup=get_cabinet_keyboard(subs_link=None), parse_mode="HTML")
        except Exception as e:
            logging.warning(f"Could not edit message: {e}")
            await cq.answer("❌ Подписка не найдена", show_alert=True)
    else:
        subs_link = user_data.get("subscription_url")
        if not subs_link:
            subs_link = SUBS_LINK_TEMPLATE.format(username=mb_username)
        
        text = (
            f"<b>🔗 Ваша ссылка для подключения:</b>\n\n"
            f"<code>{subs_link}</code>\n"
            f"👆 <i>Нажмите на ссылку, чтобы скопировать</i>\n\n"
            f"🌍 <a href='{subs_link}'>Открыть в браузере</a>\n\n"
            f"💡 Вставьте эту ссылку в ваше VPN-приложение (V2Ray, Hiddify, Streisand и др.)"
        )
        try:
            await cq.message.edit_text(text, reply_markup=get_cabinet_keyboard(subs_link=subs_link), parse_mode="HTML")
        except Exception as e:
            logging.warning(f"Could not edit message: {e}")
            await cq.answer(subs_link, show_alert=False)
    
    await cq.answer()

@dp.callback_query(lambda cq: cq.data == "renew_menu")
async def cb_renew_menu(cq: types.CallbackQuery):
    """Show renewal plans menu"""
    tg_user = cq.from_user
    text = build_renew_menu_text(tg_user)
    try:
        await cq.message.edit_text(text, reply_markup=get_renew_keyboard(tg_user), parse_mode="HTML")
    except Exception as e:
        if "not modified" not in str(e):
            logging.error(f"Error editing message: {e}")
        await cq.answer()
    else:
        await cq.answer()

# Create renewal keyboard (same as buy but with different callback)
def get_renew_keyboard(tg_user: types.User | None = None):
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=format_plan_price_text("m1", tg_user=tg_user), callback_data="renew:m1")],
        [types.InlineKeyboardButton(text=format_plan_price_text("m3", tg_user=tg_user), callback_data="renew:m3")],
        [types.InlineKeyboardButton(text=format_plan_price_text("m6", tg_user=tg_user), callback_data="renew:m6")],
        [types.InlineKeyboardButton(text=format_plan_price_text("y1", tg_user=tg_user), callback_data="renew:y1")],
        [types.InlineKeyboardButton(text="⭐️ Купить звезды", url=get_star_buy_url())],
        [types.InlineKeyboardButton(text="🎟 Ввести промокод", callback_data="promo_enter:renew")],
        [types.InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_cabinet")],
    ])
    return kb


def build_buy_menu_text(tg_user: types.User) -> str:
    mb_username = build_marzban_username(tg_user)
    pending_code = get_user_pending_promo_code(mb_username)
    promo_line = "🎟 <b>Промокод:</b> не задан"
    if pending_code:
        promo = get_valid_promo(pending_code)
        if promo:
            expires_at = promo.get("expires_at")
            exp_txt = ""
            if expires_at:
                exp_dt = datetime.fromtimestamp(int(expires_at), tz=timezone.utc)
                exp_txt = f" (до {exp_dt.strftime('%d.%m.%Y')})"
            promo_line = f"🎟 <b>Промокод:</b> <code>{pending_code}</code> (-{promo['percent']}%){exp_txt}"
        else:
            promo_line = f"🎟 <b>Промокод:</b> <code>{pending_code}</code> (недействителен)"

    return (
        "💎 <b>Выберите тарифный план:</b>\n\n"
        "⚡️ Высокая скорость\n"
        "🌍 Локации по всему миру\n"
        "♾ Безлимитный трафик\n\n"
        f"{promo_line}"
    )


def build_renew_menu_text(tg_user: types.User) -> str:
    mb_username = build_marzban_username(tg_user)
    pending_code = get_user_pending_promo_code(mb_username)
    promo_line = "🎟 <b>Промокод:</b> не задан"
    if pending_code:
        promo = get_valid_promo(pending_code)
        if promo:
            expires_at = promo.get("expires_at")
            exp_txt = ""
            if expires_at:
                exp_dt = datetime.fromtimestamp(int(expires_at), tz=timezone.utc)
                exp_txt = f" (до {exp_dt.strftime('%d.%m.%Y')})"
            promo_line = f"🎟 <b>Промокод:</b> <code>{pending_code}</code> (-{promo['percent']}%){exp_txt}"
        else:
            promo_line = f"🎟 <b>Промокод:</b> <code>{pending_code}</code> (недействителен)"

    return (
        "🔄 <b>Продление подписки:</b>\n\n"
        "Выберите срок продления:\n\n"
        f"{promo_line}"
    )

@dp.callback_query(lambda cq: cq.data == "back_to_cabinet")
async def cb_back_to_cabinet(cq: types.CallbackQuery):
    """Go back to cabinet"""
    tg_user = cq.from_user
    mb_username = build_marzban_username(tg_user)
    
    user_data = await marzban_get_user(mb_username)
    
    subs_link = None
    if user_data is None:
        text = (
            "👤 <b>Мой профиль</b>\n\n"
            "❌ <b>Нет активной подписки</b>\n"
            "Оформите подписку, чтобы получить доступ к VPN."
        )
    else:
        text = f"👤 <b>Мой профиль</b>\n\n{format_user_info(user_data)}"
        subs_link = user_data.get("subscription_url")
        if not subs_link:
            subs_link = SUBS_LINK_TEMPLATE.format(username=mb_username)
    
    try:
        await cq.message.edit_text(text, reply_markup=get_cabinet_keyboard(subs_link=subs_link), parse_mode="HTML")
    except Exception as e:
        if "not modified" not in str(e):
            logging.error(f"Error editing message: {e}")
        await cq.answer()
    else:
        await cq.answer()

# user pressed renew
@dp.callback_query(lambda cq: cq.data.startswith("renew:"))
async def cb_renew(cq: types.CallbackQuery):
    plan_key = cq.data.split(":",1)[1]
    plan = PLANS.get(plan_key)
    if not plan:
        await cq.answer("Ошибка: план не найден.", show_alert=True)
        return
    
    # Delete previous message with menu
    try:
        await cq.message.delete()
    except Exception as e:
        logging.warning(f"Could not delete message: {e}")
    
    # Create invoice (Stars) with optional promo discount
    tg_user = cq.from_user
    mb_username = build_marzban_username(tg_user)
    base_price = int(plan["price"])
    final_price = base_price
    applied_promo = None

    pending_code = get_user_pending_promo_code(mb_username)
    if pending_code:
        promo = get_valid_promo(pending_code)
        if promo and promo_applies_to_plan(promo, plan_key) and pending_code not in get_used_promos_for_tg_id(tg_user.id):
            percent = int(promo["percent"])
            discounted = (base_price * (100 - percent)) // 100
            final_price = max(1, int(discounted))
            applied_promo = pending_code

    prices = [LabeledPrice(label=plan["title"], amount=final_price)]

    invoice_kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⭐️ Оплатить", pay=True)],
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="invoice_back:renew")],
    ])
    
    await bot.send_invoice(
        chat_id=cq.from_user.id,
        title=f"Продление: {plan['title']}",
        description=(
            f"Продление подписки на {plan['title']}"
            + (f"\nПромокод {applied_promo}: -{get_valid_promo(applied_promo)['percent']}%" if applied_promo else "")
            + (f"\nИтого: {final_price} ⭐ (было {base_price} ⭐)" if applied_promo else f"\nИтого: {base_price} ⭐")
        ),
        payload=f"renew:{plan_key}:{cq.from_user.id}:{applied_promo}" if applied_promo else f"renew:{plan_key}:{cq.from_user.id}",
        provider_token="",
        start_parameter=f"renew_{plan_key}",
        currency="XTR",
        prices=prices,
        reply_markup=invoice_kb,
    )
    await cq.answer()

# user pressed buy
@dp.callback_query(lambda cq: cq.data.startswith("buy:"))
async def cb_buy(cq: types.CallbackQuery):
    plan_key = cq.data.split(":",1)[1]
    plan = PLANS.get(plan_key)
    if not plan:
        await cq.answer("Ошибка: план не найден.", show_alert=True)
        return
    
    # Delete previous message with menu
    try:
        await cq.message.delete()
    except Exception as e:
        logging.warning(f"Could not delete message: {e}")
    
    # Create invoice (Stars) with optional promo discount
    tg_user = cq.from_user
    mb_username = build_marzban_username(tg_user)
    base_price = int(plan["price"])
    final_price = base_price
    applied_promo = None

    pending_code = get_user_pending_promo_code(mb_username)
    if pending_code:
        promo = get_valid_promo(pending_code)
        if promo and promo_applies_to_plan(promo, plan_key) and pending_code not in get_used_promos_for_tg_id(tg_user.id):
            percent = int(promo["percent"])
            discounted = (base_price * (100 - percent)) // 100
            final_price = max(1, int(discounted))
            applied_promo = pending_code

    prices = [LabeledPrice(label=plan["title"], amount=final_price)]

    invoice_kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⭐️ Оплатить", pay=True)],
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="invoice_back:buy")],
    ])
    
    await bot.send_invoice(
        chat_id=cq.from_user.id,
        title=f"{plan['title']} на {plan['days']} дней",
        description=(
            f"Подписка {plan['title']}"
            + (f"\nПромокод {applied_promo}: -{get_valid_promo(applied_promo)['percent']}%" if applied_promo else "")
            + (f"\nИтого: {final_price} ⭐ (было {base_price} ⭐)" if applied_promo else f"\nИтого: {base_price} ⭐")
        ),
        payload=f"purchase:{plan_key}:{cq.from_user.id}:{applied_promo}" if applied_promo else f"purchase:{plan_key}:{cq.from_user.id}",
        provider_token="",
        start_parameter=f"buy_{plan_key}",
        currency="XTR",
        prices=prices,
        reply_markup=invoice_kb,
    )
    await cq.answer()


@dp.callback_query(lambda cq: cq.data.startswith("invoice_back:"))
async def cb_invoice_back(cq: types.CallbackQuery):
    scope = cq.data.split(":", 1)[1]
    # Best-effort delete invoice message
    try:
        await cq.message.delete()
    except Exception as e:
        logging.warning(f"Could not delete invoice message: {e}")

    if scope == "renew":
        text = build_renew_menu_text(cq.from_user)
        await bot.send_message(cq.from_user.id, text, reply_markup=get_renew_keyboard(cq.from_user), parse_mode="HTML")
    else:
        text = build_buy_menu_text(cq.from_user)
        await bot.send_message(cq.from_user.id, text, reply_markup=get_buy_keyboard(cq.from_user), parse_mode="HTML")

    await cq.answer()

# handle pre_checkout (confirm invoice)
@dp.pre_checkout_query()
async def pre_checkout(pre: PreCheckoutQuery):
    # For Stars flow, just answer True
    await bot.answer_pre_checkout_query(pre.id, ok=True)

# handle successful payment
@dp.message(lambda msg: msg.successful_payment is not None)
async def on_success(m: Message):
    sp = m.successful_payment
    # For Stars transaction info there is star_transaction metadata
    star_tx = getattr(sp, "star_transaction", None)
    logging.info("Successful payment: %s, star_tx=%s", sp.total_amount, star_tx)
    # payload: we set earlier payload to "purchase:plan:userid" or "renew:plan:userid"
    payload = sp.invoice_payload
    try:
        parts = payload.split(":")
        payment_type = parts[0]
        plan_key = parts[1] if len(parts) > 1 else None
        buyer_id_str = parts[2] if len(parts) > 2 else None
        applied_promo = normalize_promo_code(parts[3]) if len(parts) > 3 and parts[3] else None
    except Exception:
        payment_type = "purchase"
        plan_key = None
        applied_promo = None
    plan = PLANS.get(plan_key) if plan_key else None

    # Build marzban username
    tg_user = m.from_user
    mb_username = build_marzban_username(tg_user)

    # compute expire timestamp (UTC) in seconds
    days = plan["days"] if plan else 30
    
    # Determine expiry date - ALWAYS add to existing expiry if user exists
    user_data = await marzban_get_user(mb_username)
    if user_data and user_data.get("expire"):
        # User already has subscription, add days to current expiry
        current_expire_ts = user_data.get("expire")
        expire_dt = datetime.fromtimestamp(current_expire_ts, tz=timezone.utc) + timedelta(days=days)
        if payment_type == "renew":
            logging.info(f"Renewal: {plan_key} ({days} дней added to existing subscription)")
        else:
            logging.info(f"Purchase: {plan_key} ({days} дней added to existing subscription)")
    else:
        # No existing subscription, create new one from now
        expire_dt = datetime.now(timezone.utc) + timedelta(days=days)
        if payment_type == "renew":
            logging.info(f"Renewal: {plan_key} ({days} дней - new subscription)")
        else:
            logging.info(f"Purchase: {plan_key} ({days} дней - new subscription)")
    
    expire_ts = int(expire_dt.timestamp())
    logging.info(f"Plan: {plan_key}, Expire datetime: {expire_dt}, Expire timestamp: {expire_ts}")

    # Create or update user in Marzban
    if user_data:
        # User already exists, always update
        res = await marzban_update_user(mb_username, expire_ts)
        action_text = "продлена"
    else:
        # User doesn't exist, create new
        res = await marzban_create_user(mb_username, expire_ts)
        action_text = "активирована"
        
    logging.info(f"Operation result: {res}")
    if res is None:
        text = f"❌ Оплата принята, но не удалось {'продлить' if payment_type == 'renew' else 'создать'} подписку. Администратор уведомлён."
        await m.answer(text, reply_markup=get_main_keyboard(), parse_mode="HTML")
        return

    # Mark promo as used (one time per Telegram account)
    if applied_promo and plan_key:
        promo = get_valid_promo(applied_promo)
        if promo and promo_applies_to_plan(promo, plan_key):
            mark_promo_used_for_tg_id(tg_user.id, applied_promo)

            # Clear pending promo (stored by marzban username)
            users_db.setdefault(mb_username, {})
            if normalize_promo_code(users_db[mb_username].get("pending_promo")) == applied_promo:
                users_db[mb_username].pop("pending_promo", None)
                users_db[mb_username].pop("pending_promo_set_at", None)
                save_users_db(users_db)
    
    # Save or update user to database for notification system
    users_db[mb_username] = {
        "tg_id": tg_user.id,
        "tg_username": tg_user.username,
        "expire_ts": expire_ts,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    save_users_db(users_db)
    logging.info(f"Updated user {mb_username} (TG ID: {tg_user.id}) in database")
    
    # Get subscription URL (prefer API response, fallback to template)
    subs_link = res.get("subscription_url")
    if not subs_link:
        subs_link = SUBS_LINK_TEMPLATE.format(username=mb_username)

    logging.info(f"Subscription link: {subs_link}")

    # Fetch fresh user data for a nice cabinet view (fallback to res)
    fresh_user_data = await marzban_get_user(mb_username)
    if not fresh_user_data:
        fresh_user_data = res

    cabinet_text = (
        "✅ <b>Оплата прошла успешно!</b>\n"
        f"Подписка {action_text}.\n"
        f"📅 <b>Действует до:</b> {expire_dt.strftime('%d.%m.%Y')}\n\n"
        "👤 <b>Личный кабинет</b>\n"
        "━━━━━━━━━━━━━━━━━━━\n\n"
        f"{format_user_info(fresh_user_data)}\n\n"
        "🌍 <i>Подключение:</i> нажмите кнопку <b>Открыть подписку</b> ниже."
    )
    await m.answer(cabinet_text, reply_markup=get_cabinet_keyboard(subs_link=subs_link), parse_mode="HTML")

# Logic for checking subscriptions
async def run_subscription_check():
    logging.info("🔍 Checking subscriptions...")
    
    try:
        # Get all users from Marzban
        api = Marzban(username=MARZBAN_ADMIN_USERNAME, password=MARZBAN_ADMIN_PASSWORD, panel_address=MARZBAN_URL)
        token_data = await api.get_token()
        token = token_data.get("access_token") if isinstance(token_data, dict) else token_data.access_token
        
        if not token:
            logging.error("Failed to get Marzban token for subscription check")
            return
        
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {token}"}
            
            async with session.get(f"{MARZBAN_URL}/api/users", headers=headers, ssl=False) as resp:
                if resp.status != 200:
                    logging.warning(f"Could not fetch users: {resp.status}")
                    return
                
                users_data = await resp.json()
                users = users_data.get("users", []) if isinstance(users_data, dict) else users_data
                
                now = datetime.now(timezone.utc)
                
                for user in users:
                    username = user.get("username")
                    expire_ts = user.get("expire")
                    
                    # Skip if no expire date
                    if not expire_ts or not username:
                        continue
                    
                    # Skip if user not in our database
                    if username not in users_db:
                        continue
                    
                    user_info = users_db[username]
                    tg_id = user_info.get("tg_id")
                    
                    if not tg_id:
                        continue
                    
                    expire_dt = datetime.fromtimestamp(expire_ts, tz=timezone.utc)
                    time_left = (expire_dt - now).total_seconds()
                    
                    # Only send notification if subscription is active and about to expire
                    if time_left <= 0:
                        # Already expired
                        continue
                    
                    # Notification thresholds
                    thresholds = {
                        "72h": (3 * 24 * 3600, "⏰ <b>3 дня</b> до истечения"),
                        "24h": (24 * 3600, "⏰ <b>1 день</b> до истечения"),
                        "1h": (3600, "⏰ <b>1 час</b> до истечения")
                    }
                    
                    for threshold_key, (threshold_seconds, threshold_msg) in thresholds.items():
                        # Initialize notifications tracking if needed
                        if username not in sent_notifications:
                            sent_notifications[username] = {}
                        
                        # If subscription expires within threshold and we haven't sent this notification yet
                        if 0 < time_left <= threshold_seconds and threshold_key not in sent_notifications[username]:
                            try:
                                days_left = int(time_left / 86400)
                                hours_left = int((time_left % 86400) / 3600)
                                
                                text = (
                                    f"🔔 <b>Уведомление о подписке</b>\n\n"
                                    f"{threshold_msg}\n"
                                    f"Осталось: {days_left}д {hours_left}ч\n\n"
                                    f"👤 <b>Логин:</b> <code>{username}</code>\n\n"
                                    f"Продлите подписку, чтобы не потерять доступ к VPN"
                                )
                                
                                kb = types.InlineKeyboardMarkup(inline_keyboard=[
                                    [types.InlineKeyboardButton(text="💎 Продлить подписку", callback_data="buy_menu")],
                                ])
                                
                                await bot.send_message(tg_id, text, reply_markup=kb, parse_mode="HTML")
                                sent_notifications[username][threshold_key] = True
                                
                                logging.info(f"✅ Sent {threshold_key} notification to user {username} (TG ID: {tg_id})")
                            
                            except Exception as e:
                                logging.error(f"Error sending notification to {tg_id}: {e}")

    except Exception as e:
        logging.error(f"Error in subscription check task: {e}", exc_info=True)

# Background task for checking subscription expiration
async def check_subscriptions_task():
    """Check all users' subscriptions and send notifications"""
    logging.info("🔔 Background task started: subscription checker")
    
    while True:
        await asyncio.sleep(3600)  # Check every hour
        await run_subscription_check()

@dp.message(Command("check_subs"))
async def cmd_check_subs(m: Message):
    if m.from_user.id != ADMIN_ID:
        return
    
    await m.answer("🔍 Запускаю принудительную проверку подписок...")
    await run_subscription_check()
    await m.answer("✅ Проверка завершена. Проверьте логи для деталей.")

@dp.message(Command("test_notify"))
async def cmd_test_notify(m: Message):
    if m.from_user.id != ADMIN_ID:
        return
        
    text = (
        f"🔔 <b>Уведомление о подписке (ТЕСТ)</b>\n\n"
        f"⏰ <b>3 дня</b> до истечения\n"
        f"Осталось: 2д 23ч\n\n"
        f"👤 <b>Логин:</b> <code>test_user</code>\n\n"
        f"Продлите подписку, чтобы не потерять доступ к VPN"
    )
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="💎 Продлить подписку", callback_data="buy_menu")],
    ])
    
    await m.answer(text, reply_markup=kb, parse_mode="HTML")

if __name__ == "__main__":
    # Fix SSL certificate verification issue on Windows (duplicate but safe)
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context
    
    async def main():
        logging.info("Бот запускается...")
        try:
            # Get bot info
            me = await bot.get_me()
            logging.info(f"Бот запущен как @{me.username}")
            
            # Start background task for subscription checks
            asyncio.create_task(check_subscriptions_task())
            
            # Start polling with proper error handling
            logging.info("Начинаем polling...")
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
        except KeyboardInterrupt:
            logging.info("Бот остановлен пользователем")
        except Exception as e:
            logging.error(f"Ошибка при polling: {e}", exc_info=True)
        finally:
            logging.info("Закрываем сессию бота...")
            await bot.session.close()
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Остановка по Ctrl+C")
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        logging.info("Бот остановлен")
