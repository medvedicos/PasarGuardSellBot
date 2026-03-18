import os
import time
import logging
import asyncio
import uuid
import json
import re
from datetime import datetime, timedelta, timezone
import aiohttp
# PasarGuard panel integration via direct HTTP API calls
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import LabeledPrice, Invoice, PreCheckoutQuery, Message, CallbackQuery, BufferedInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from dotenv import load_dotenv
import qrcode

load_dotenv()
logging.basicConfig(level=logging.INFO)

ADMIN_ID = 430301005

# Users database file for storing user_id -> username mapping
USERS_DB_FILE = "users_db.json"
PLANS_FILE = "plans.json"
PROMOS_FILE = "promos.json"
PROMO_USAGE_FILE = "promo_usage.json"
REFERRALS_FILE = "referrals.json"
TICKETS_FILE = "tickets.json"
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
                    if "required_channel" not in data:
                        data["required_channel"] = None
                    if "referral_bonus_days" not in data:
                        data["referral_bonus_days"] = 3
                    if "user_group_ids" not in data:
                        data["user_group_ids"] = None
                    return data
        except Exception as e:
            logging.error(f"Error loading settings: {e}")
    return {"maintenance_mode": False, "star_rub_rate": None, "star_buy_url": "https://t.me/PremiumBot", "required_channel": None, "referral_bonus_days": 3, "user_group_ids": None}


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
    panel_username = build_panel_username(tg_user)
    pending_code = get_user_pending_promo_code(panel_username)
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


def build_subscription_qr_png_bytes(subs_link: str) -> bytes:
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10,
        border=2,
    )
    qr.add_data(subs_link)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    import io
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def get_admin_keyboard():
    maintenance_on = is_maintenance_mode()
    maintenance_text = "🛠 Техработы: ВКЛ" if maintenance_on else "🛠 Техработы: ВЫКЛ"
    channel = get_required_channel()
    channel_text = f"📢 Канал: @{channel}" if channel else "📢 Канал: не задан"
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="💰 Сменить тарифы", callback_data="admin_prices")],
        [types.InlineKeyboardButton(text="🎟 Промокоды", callback_data="admin_promos")],
        [types.InlineKeyboardButton(text=channel_text, callback_data="admin_channel")],
        [types.InlineKeyboardButton(text="🗂 Группы новых юзеров", callback_data="admin_groups")],
        [types.InlineKeyboardButton(text="📨 Рассылка", callback_data="admin_broadcast")],
        [types.InlineKeyboardButton(text="🎫 Тикеты", callback_data="admin_tickets")],
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


# ── Referrals DB ──────────────────────────────────────────────
def load_referrals_db():
    if os.path.exists(REFERRALS_FILE):
        try:
            with open(REFERRALS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading referrals DB: {e}")
    return {}


def save_referrals_db(data):
    try:
        with open(REFERRALS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Error saving referrals DB: {e}")


referrals_db = load_referrals_db()


def get_referral_bonus_days() -> int:
    try:
        return int(settings.get("referral_bonus_days", 3))
    except (TypeError, ValueError):
        return 3


def set_referral_bonus_days(days: int):
    settings["referral_bonus_days"] = days
    settings["updated_at"] = datetime.now(timezone.utc).isoformat()
    save_settings(settings)


# ── Tickets DB ────────────────────────────────────────────────
def load_tickets_db():
    if os.path.exists(TICKETS_FILE):
        try:
            with open(TICKETS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading tickets DB: {e}")
    return {}


def save_tickets_db(data):
    try:
        with open(TICKETS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Error saving tickets DB: {e}")


tickets_db = load_tickets_db()

# next ticket id
_next_ticket_id = max((int(k) for k in tickets_db if k.isdigit()), default=0) + 1


def create_ticket(tg_id: int, tg_username: str | None, text: str) -> str:
    global _next_ticket_id
    tid = str(_next_ticket_id)
    _next_ticket_id += 1
    tickets_db[tid] = {
        "tg_id": tg_id,
        "tg_username": tg_username,
        "status": "open",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "messages": [{"from": "user", "text": text, "ts": datetime.now(timezone.utc).isoformat()}],
    }
    save_tickets_db(tickets_db)
    return tid


def get_open_ticket_for_user(tg_id: int):
    for tid, t in tickets_db.items():
        if t.get("tg_id") == tg_id and t.get("status") == "open":
            return tid, t
    return None, None


# ── Required channel helpers ──────────────────────────────────
def get_required_channel() -> str | None:
    ch = settings.get("required_channel")
    if isinstance(ch, str) and ch.strip():
        return ch.strip().lstrip("@")
    return None


def set_required_channel(value: str | None):
    settings["required_channel"] = value
    settings["updated_at"] = datetime.now(timezone.utc).isoformat()
    save_settings(settings)


async def check_channel_subscription(user_id: int) -> bool:
    channel = get_required_channel()
    if not channel:
        return True
    try:
        member = await bot.get_chat_member(f"@{channel}", user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception as e:
        logging.warning(f"Channel check failed for {user_id}: {e}")
        return True  # if check fails, don't block


def get_channel_not_subscribed_text() -> str:
    channel = get_required_channel()
    return (
        f"📢 <b>Для продолжения подпишитесь на наш канал:</b>\n\n"
        f"👉 @{channel}\n\n"
        f"После подписки нажмите кнопку ниже."
    )


def get_channel_check_keyboard(return_to: str = "back_to_menu"):
    channel = get_required_channel()
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📢 Подписаться", url=f"https://t.me/{channel}")],
        [types.InlineKeyboardButton(text="✅ Я подписался", callback_data=f"check_sub:{return_to}")],
    ])
    return kb


# ── User group IDs helpers ────────────────────────────────────
def get_user_group_ids() -> list[int]:
    """Return group IDs for new users: from settings, fallback to env PANEL_GROUP_IDS."""
    ids = settings.get("user_group_ids")
    if isinstance(ids, list) and ids:
        return [int(x) for x in ids]
    return PANEL_GROUP_IDS  # fallback to env var


def set_user_group_ids(ids: list[int]):
    settings["user_group_ids"] = ids
    settings["updated_at"] = datetime.now(timezone.utc).isoformat()
    save_settings(settings)


async def fetch_panel_groups() -> list[dict] | None:
    """Fetch available groups from PasarGuard panel."""
    try:
        token = await get_panel_token()
        if not token:
            return None
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {token}"}
            async with session.get(f"{PANEL_URL}/api/groups", headers=headers, ssl=False) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("groups", []) if isinstance(data, dict) else data
                return None
    except Exception as e:
        logging.error(f"Error fetching panel groups: {e}")
        return None


def get_user_pending_promo_code(panel_username: str):
    entry = users_db.get(panel_username, {})
    code = entry.get("pending_promo")
    if isinstance(code, str) and code.strip():
        return normalize_promo_code(code)
    return None

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # e.g. https://yourdomain.com/webhook/telegram
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8443"))

PANEL_URL = os.getenv("PANEL_URL", "https://example.com")
PANEL_ADMIN_USERNAME = os.getenv("PANEL_ADMIN_USERNAME")
PANEL_ADMIN_PASSWORD = os.getenv("PANEL_ADMIN_PASSWORD")
SUBS_LINK_TEMPLATE = os.getenv("SUBS_LINK_TEMPLATE", f"{PANEL_URL}/sub/{{username}}")

# PasarGuard user creation configuration
PANEL_PROXY_TYPE = os.getenv("PANEL_PROXY_TYPE", "vless")
PANEL_PROXY_FLOW = os.getenv("PANEL_PROXY_FLOW", "xtls-rprx-vision")
# Group IDs for new users (comma-separated, e.g. "2" or "2,3")
_raw_group_ids = os.getenv("PANEL_GROUP_IDS", "2")
PANEL_GROUP_IDS = [int(x.strip()) for x in _raw_group_ids.split(",") if x.strip().isdigit()]

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


class BroadcastStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_confirm = State()


class TicketUserStates(StatesGroup):
    waiting_for_message = State()


class AdminTicketStates(StatesGroup):
    waiting_for_reply = State()


class AdminChannelStates(StatesGroup):
    waiting_for_channel = State()


class PromoUserStates(StatesGroup):
    waiting_for_code = State()

# Get admin token from PasarGuard panel
async def get_panel_token() -> str | None:
    """Authenticate with PasarGuard panel and return access token"""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{PANEL_URL}/api/admin/token"
            data = aiohttp.FormData()
            data.add_field("username", PANEL_ADMIN_USERNAME)
            data.add_field("password", PANEL_ADMIN_PASSWORD)
            async with session.post(url, data=data, ssl=False) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    return result.get("access_token")
                else:
                    logging.error(f"Failed to get panel token: {resp.status}")
                    return None
    except Exception as e:
        logging.error(f"Error getting panel token: {e}")
        return None

# Get user subscription info from PasarGuard
async def panel_get_user(username: str):
    """Fetch user info from PasarGuard"""
    try:
        token = await get_panel_token()
        if not token:
            return None

        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{PANEL_URL}/api/user/{username}"

            async with session.get(url, headers=headers, ssl=False) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logging.warning(f"Could not fetch user {username}: {resp.status}")
                    return None
    except Exception as e:
        logging.error(f"Error fetching user {username}: {e}")
        return None

def parse_expire(expire_val) -> datetime | None:
    """Parse expire value from PasarGuard (ISO string or unix timestamp)"""
    if expire_val is None:
        return None
    if isinstance(expire_val, str):
        try:
            # ISO format: "2026-03-25T00:00:00Z"
            dt = datetime.fromisoformat(expire_val.replace("Z", "+00:00"))
            return dt
        except ValueError:
            pass
        try:
            return datetime.fromtimestamp(float(expire_val), tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            return None
    if isinstance(expire_val, (int, float)):
        try:
            return datetime.fromtimestamp(expire_val, tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            return None
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
    expire_dt = parse_expire(expire)
    if expire_dt:
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
        [types.InlineKeyboardButton(text="👥 Реферальная программа", callback_data="referral_menu")],
        [types.InlineKeyboardButton(text="🆘 Поддержка", callback_data="support_menu")],
    ])
    return kb

# Create cabinet keyboard
def get_cabinet_keyboard(subs_link: str | None = None):
    rows = []
    if subs_link:
        rows.append([types.InlineKeyboardButton(text="🌍 Открыть подписку", url=subs_link)])
        rows.append([types.InlineKeyboardButton(text="📱 QR-код", callback_data="cabinet_qr")])

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

# create user in PasarGuard
async def panel_create_user(username: str, expire_ts: int):
    try:
        token = await get_panel_token()
        if not token:
            return None

        logging.info(f"Создаём пользователя: {username}, expire_ts: {expire_ts}")

        payload = {
            "username": username,
            "proxy_settings": {
                PANEL_PROXY_TYPE: {
                    "id": str(uuid.uuid4()),
                    "flow": PANEL_PROXY_FLOW
                }
            },
            "group_ids": get_user_group_ids(),
            "data_limit": 0,
            "expire": expire_ts,
            "status": "active"
        }

        logging.info(f"📤 Payload: {payload}")

        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{PANEL_URL}/api/user"

            async with session.post(url, json=payload, headers=headers, ssl=False) as resp:
                logging.info(f"   API Response status: {resp.status}")
                data = await resp.json()

                if resp.status in (200, 201):
                    logging.info(f"✓ User created successfully")
                    await asyncio.sleep(0.1)

                    get_url = f"{PANEL_URL}/api/user/{username}"
                    async with session.get(get_url, headers=headers, ssl=False) as get_resp:
                        if get_resp.status == 200:
                            user_data = await get_resp.json()
                            logging.info(f"✓ Retrieved fresh user data with subscription_url")
                            return user_data
                        else:
                            logging.warning(f"Could not fetch user after creation (status {get_resp.status}), using create response")
                            return data
                elif resp.status == 409:
                    logging.info(f"ℹ User {username} already exists (409), updating expiry instead")
                    return await panel_update_user(username, expire_ts)
                else:
                    logging.error(f"✗ Failed to create user: {resp.status}")
                    logging.error(f"   Response: {data}")
                    return None

    except Exception as e:
        logging.error(f"Ошибка создания пользователя: {type(e).__name__}: {e}", exc_info=True)
        return None

# Update user subscription (extend expiry date)
async def panel_update_user(username: str, new_expire_ts: int):
    """Update user expiry date in PasarGuard"""
    try:
        token = await get_panel_token()
        if not token:
            return None

        logging.info(f"Обновляем подписку: {username}, new_expire_ts: {new_expire_ts}")

        payload = {
            "expire": new_expire_ts
        }

        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{PANEL_URL}/api/user/{username}"

            async with session.put(url, json=payload, headers=headers, ssl=False) as resp:
                logging.info(f"   API Response status: {resp.status}")
                data = await resp.json()

                if resp.status in (200, 201):
                    logging.info(f"✓ User updated successfully")
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
def build_panel_username(tg_user: types.User):
    if tg_user.username:
        # normalize: allowed a-z,0-9, underscore, 3-32 chars
        raw = tg_user.username.lower()
        # keep only allowed chars:
        import re
        clean = re.sub(r'[^a-z0-9_]', '_', raw)[:28]
        return clean
    else:
        return f"user_{tg_user.id}"

async def clear_chat_history(chat_id: int, up_to_message_id: int):
    """Delete recent messages in chat to simulate a fresh start."""
    for msg_id in range(up_to_message_id, max(up_to_message_id - 50, 0), -1):
        try:
            await bot.delete_message(chat_id, msg_id)
        except Exception:
            pass  # message already deleted, too old, or not ours


# Admin command
@dp.message(Command("admin"))
async def cmd_admin(m: Message):
    if m.from_user.id != ADMIN_ID:
        return

    await clear_chat_history(m.chat.id, m.message_id)
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

# start (with referral deep link support: /start ref_12345)
@dp.message(Command("start"))
async def cmd_start(m: Message):
    tg_user = m.from_user
    logging.info(f"Команда /start от {tg_user.username or tg_user.id}")

    # Handle referral deep link: /start ref_<REFERRER_TG_ID>
    args = m.text.split(maxsplit=1)
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referrer_id = int(args[1][4:])
        except (ValueError, IndexError):
            referrer_id = None

        my_id = str(tg_user.id)
        if referrer_id and referrer_id != tg_user.id and my_id not in referrals_db:
            # Record this user as referred
            referrals_db[my_id] = {
                "referred_by": referrer_id,
                "registered_at": datetime.now(timezone.utc).isoformat(),
                "rewarded": False,
            }
            # Add to referrer's list
            ref_key = str(referrer_id)
            referrals_db.setdefault(ref_key, {})
            ref_list = referrals_db[ref_key].get("referred_users", [])
            if tg_user.id not in ref_list:
                ref_list.append(tg_user.id)
            referrals_db[ref_key]["referred_users"] = ref_list
            save_referrals_db(referrals_db)
            logging.info(f"Referral: {tg_user.id} referred by {referrer_id}")

    await clear_chat_history(m.chat.id, m.message_id)

    text = (
        "👋 <b>Привет! Я MiSa Link</b> — твой проводник в свободный интернет!\n\n"
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
    panel_username = build_panel_username(tg_user)
    
    # Get user info from panel
    user_data = await panel_get_user(panel_username)
    
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
            subs_link = SUBS_LINK_TEMPLATE.format(username=panel_username)
    
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


@dp.callback_query(lambda cq: cq.data == "cabinet_qr")
async def cb_cabinet_qr(cq: types.CallbackQuery):
    tg_user = cq.from_user
    panel_username = build_panel_username(tg_user)

    user_data = await panel_get_user(panel_username)
    subs_link = None
    if user_data is not None:
        subs_link = user_data.get("subscription_url")
    if not subs_link:
        subs_link = SUBS_LINK_TEMPLATE.format(username=panel_username)

    if user_data is None:
        await cq.answer("❌ Подписка не активна", show_alert=True)
        return

    try:
        png_bytes = build_subscription_qr_png_bytes(subs_link)
    except Exception as e:
        logging.error(f"QR generation error: {e}")
        await cq.answer("❌ Не удалось создать QR-код", show_alert=True)
        return

    try:
        await cq.message.delete()
    except Exception as e:
        logging.warning(f"Could not delete cabinet message: {e}")

    qr_kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬇️ Скачать QR-код", callback_data="qr_download")],
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="qr_back")],
    ])

    photo = BufferedInputFile(png_bytes, filename="subscription_qr.png")
    await bot.send_photo(
        chat_id=tg_user.id,
        photo=photo,
        caption="📱 <b>QR-код подписки</b>\n\nОтсканируйте QR-код в вашем VPN-приложении.",
        parse_mode="HTML",
        reply_markup=qr_kb,
    )
    await cq.answer()


@dp.callback_query(lambda cq: cq.data == "qr_download")
async def cb_qr_download(cq: types.CallbackQuery):
    tg_user = cq.from_user
    panel_username = build_panel_username(tg_user)
    user_data = await panel_get_user(panel_username)

    subs_link = None
    if user_data is not None:
        subs_link = user_data.get("subscription_url")
    if not subs_link:
        subs_link = SUBS_LINK_TEMPLATE.format(username=panel_username)

    try:
        png_bytes = build_subscription_qr_png_bytes(subs_link)
    except Exception as e:
        logging.error(f"QR generation error: {e}")
        await cq.answer("❌ Не удалось создать QR-код", show_alert=True)
        return

    doc = BufferedInputFile(png_bytes, filename="subscription_qr.png")
    await bot.send_document(chat_id=tg_user.id, document=doc, caption="📱 QR-код подписки")
    await cq.answer("✅ Отправил", show_alert=False)


@dp.callback_query(lambda cq: cq.data == "qr_back")
async def cb_qr_back(cq: types.CallbackQuery):
    tg_user = cq.from_user
    panel_username = build_panel_username(tg_user)

    try:
        await cq.message.delete()
    except Exception as e:
        logging.warning(f"Could not delete QR message: {e}")

    user_data = await panel_get_user(panel_username)
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
            subs_link = SUBS_LINK_TEMPLATE.format(username=panel_username)

    await bot.send_message(
        chat_id=tg_user.id,
        text=text,
        reply_markup=get_cabinet_keyboard(subs_link=subs_link),
        parse_mode="HTML",
    )
    await cq.answer()

@dp.callback_query(lambda cq: cq.data == "buy_menu")
async def cb_buy_menu(cq: types.CallbackQuery):
    """Show buy menu"""
    tg_user = cq.from_user

    # Check channel subscription
    if not await check_channel_subscription(tg_user.id):
        await cq.message.edit_text(
            get_channel_not_subscribed_text(),
            reply_markup=get_channel_check_keyboard("buy_menu"),
            parse_mode="HTML",
        )
        await cq.answer()
        return

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
    panel_username = build_panel_username(tg_user)
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

    users_db.setdefault(panel_username, {})
    users_db[panel_username]["pending_promo"] = code
    users_db[panel_username]["pending_promo_set_at"] = datetime.now(timezone.utc).isoformat()
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

@dp.callback_query(lambda cq: cq.data.startswith("check_sub:"))
async def cb_check_sub(cq: types.CallbackQuery):
    """Re-check channel subscription and redirect to original menu"""
    return_to = cq.data.split(":", 1)[1]
    ok = await check_channel_subscription(cq.from_user.id)
    if not ok:
        await cq.answer("❌ Вы ещё не подписались на канал!", show_alert=True)
        return
    await cq.answer("✅ Спасибо за подписку!")

    tg_user = cq.from_user
    if return_to == "buy_menu":
        text = build_buy_menu_text(tg_user)
        try:
            await cq.message.edit_text(text, reply_markup=get_buy_keyboard(tg_user), parse_mode="HTML")
        except Exception:
            pass
    elif return_to == "trial_subs":
        # Directly activate trial without re-checking channel
        panel_username = build_panel_username(tg_user)
        user_info = users_db.get(panel_username, {})
        if user_info.get("trial_used"):
            await cq.message.edit_text(
                "❌ Вы уже использовали пробный период!",
                reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_menu")],
                ]),
            )
            return
        user_data = await panel_get_user(panel_username)
        if user_data and user_data.get("status") == "active":
            expire_dt_check = parse_expire(user_data.get("expire"))
            if expire_dt_check and expire_dt_check > datetime.now(timezone.utc):
                await cq.message.edit_text(
                    "❌ У вас уже есть активная подписка!",
                    reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                        [types.InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_menu")],
                    ]),
                )
                return
        days = 3
        expire_dt = datetime.now(timezone.utc) + timedelta(days=days)
        expire_ts = int(expire_dt.timestamp())
        if user_data:
            res = await panel_update_user(panel_username, expire_ts)
        else:
            res = await panel_create_user(panel_username, expire_ts)
        if res:
            users_db.setdefault(panel_username, {})
            users_db[panel_username].update({
                "tg_id": tg_user.id, "tg_username": tg_user.username,
                "expire_ts": expire_ts, "updated_at": datetime.now(timezone.utc).isoformat(),
                "trial_used": True,
            })
            save_users_db(users_db)
            subs_link = res.get("subscription_url") or SUBS_LINK_TEMPLATE.format(username=panel_username)
            text = (
                f"🎁 <b>Пробный период активирован!</b>\n\n"
                f"📅 <b>Действует до:</b> {expire_dt.strftime('%d.%m.%Y')}\n\n"
                f"🔗 <b>Ваша ссылка для подключения:</b>\n<code>{subs_link}</code>\n\n"
                f"💡 Вставьте её в ваше VPN-приложение."
            )
            await cq.message.edit_text(text, reply_markup=get_main_keyboard(), parse_mode="HTML")
        else:
            await cq.message.edit_text(
                "❌ Ошибка активации. Попробуйте позже.",
                reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_menu")],
                ]),
            )
    else:
        await cb_back_to_menu(cq)


@dp.callback_query(lambda cq: cq.data == "trial_subs")
async def cb_trial_subs(cq: types.CallbackQuery):
    """Activate trial subscription"""
    tg_user = cq.from_user

    # Check channel subscription
    if not await check_channel_subscription(tg_user.id):
        await cq.message.edit_text(
            get_channel_not_subscribed_text(),
            reply_markup=get_channel_check_keyboard("trial_subs"),
            parse_mode="HTML",
        )
        await cq.answer()
        return

    panel_username = build_panel_username(tg_user)
    
    # Check if trial already used
    user_info = users_db.get(panel_username, {})
    if user_info.get("trial_used"):
        await cq.answer("❌ Вы уже использовали пробный период!", show_alert=True)
        return

    # Check if user already has active subscription
    user_data = await panel_get_user(panel_username)
    if user_data and user_data.get("status") == "active":
        expire_dt_check = parse_expire(user_data.get("expire"))
        if expire_dt_check and expire_dt_check > datetime.now(timezone.utc):
            await cq.answer("❌ У вас уже есть активная подписка!", show_alert=True)
            return

    # Activate trial
    days = 3
    expire_dt = datetime.now(timezone.utc) + timedelta(days=days)
    expire_ts = int(expire_dt.timestamp())
    
    # Create or update user
    if user_data:
        res = await panel_update_user(panel_username, expire_ts)
    else:
        res = await panel_create_user(panel_username, expire_ts)
        
    if res:
        # Update DB
        if panel_username not in users_db:
            users_db[panel_username] = {}
            
        users_db[panel_username].update({
            "tg_id": tg_user.id,
            "tg_username": tg_user.username,
            "expire_ts": expire_ts,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "trial_used": True
        })
        save_users_db(users_db)
        
        subs_link = res.get("subscription_url")
        if not subs_link:
            subs_link = SUBS_LINK_TEMPLATE.format(username=panel_username)
            
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
        "👋 <b>Привет! Я MiSa Link</b> — твой проводник в свободный интернет!\n\n"
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
    panel_username = build_panel_username(tg_user)
    
    # Get user info from panel
    user_data = await panel_get_user(panel_username)
    
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
            subs_link = SUBS_LINK_TEMPLATE.format(username=panel_username)
        
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
    panel_username = build_panel_username(tg_user)
    pending_code = get_user_pending_promo_code(panel_username)
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
    panel_username = build_panel_username(tg_user)
    pending_code = get_user_pending_promo_code(panel_username)
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
    panel_username = build_panel_username(tg_user)
    
    user_data = await panel_get_user(panel_username)
    
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
            subs_link = SUBS_LINK_TEMPLATE.format(username=panel_username)
    
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
    panel_username = build_panel_username(tg_user)
    base_price = int(plan["price"])
    final_price = base_price
    applied_promo = None

    pending_code = get_user_pending_promo_code(panel_username)
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
    panel_username = build_panel_username(tg_user)
    base_price = int(plan["price"])
    final_price = base_price
    applied_promo = None

    pending_code = get_user_pending_promo_code(panel_username)
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

    # Build panel username
    tg_user = m.from_user
    panel_username = build_panel_username(tg_user)

    # compute expire timestamp (UTC) in seconds
    days = plan["days"] if plan else 30
    
    # Determine expiry date - ALWAYS add to existing expiry if user exists
    user_data = await panel_get_user(panel_username)
    current_expire_dt = parse_expire(user_data.get("expire")) if user_data else None
    if user_data and current_expire_dt:
        # User already has subscription, add days to current expiry
        expire_dt = current_expire_dt + timedelta(days=days)
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

    # Create or update user in PasarGuard
    if user_data:
        # User already exists, always update
        res = await panel_update_user(panel_username, expire_ts)
        action_text = "продлена"
    else:
        # User doesn't exist, create new
        res = await panel_create_user(panel_username, expire_ts)
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

            # Clear pending promo (stored by panel username)
            users_db.setdefault(panel_username, {})
            if normalize_promo_code(users_db[panel_username].get("pending_promo")) == applied_promo:
                users_db[panel_username].pop("pending_promo", None)
                users_db[panel_username].pop("pending_promo_set_at", None)
                save_users_db(users_db)
    
    # Save or update user to database for notification system
    users_db[panel_username] = {
        "tg_id": tg_user.id,
        "tg_username": tg_user.username,
        "expire_ts": expire_ts,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    save_users_db(users_db)
    logging.info(f"Updated user {panel_username} (TG ID: {tg_user.id}) in database")
    
    # ── Referral reward: give bonus days to referrer on first purchase ──
    my_ref = referrals_db.get(str(tg_user.id))
    if my_ref and isinstance(my_ref, dict) and not my_ref.get("rewarded") and my_ref.get("referred_by"):
        referrer_id = my_ref["referred_by"]
        bonus_days = get_referral_bonus_days()
        if bonus_days > 0:
            # Find referrer's panel username
            referrer_panel = None
            for pu, info in users_db.items():
                if isinstance(info, dict) and info.get("tg_id") == referrer_id:
                    referrer_panel = pu
                    break
            if referrer_panel:
                ref_user_data = await panel_get_user(referrer_panel)
                if ref_user_data:
                    ref_expire_dt = parse_expire(ref_user_data.get("expire"))
                    if ref_expire_dt and ref_expire_dt > datetime.now(timezone.utc):
                        new_ref_expire = int((ref_expire_dt + timedelta(days=bonus_days)).timestamp())
                        await panel_update_user(referrer_panel, new_ref_expire)
                        try:
                            await bot.send_message(
                                referrer_id,
                                f"🎉 <b>Реферальный бонус!</b>\n\n"
                                f"Ваш друг оформил подписку. Вам начислено <b>+{bonus_days} дней</b>!",
                                parse_mode="HTML",
                            )
                        except Exception as e:
                            logging.warning(f"Could not notify referrer {referrer_id}: {e}")
            my_ref["rewarded"] = True
            save_referrals_db(referrals_db)

    # Get subscription URL (prefer API response, fallback to template)
    subs_link = res.get("subscription_url")
    if not subs_link:
        subs_link = SUBS_LINK_TEMPLATE.format(username=panel_username)

    logging.info(f"Subscription link: {subs_link}")

    # Fetch fresh user data for a nice cabinet view (fallback to res)
    fresh_user_data = await panel_get_user(panel_username)
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
        # Get all users from PasarGuard
        token = await get_panel_token()
        if not token:
            logging.error("Failed to get panel token for subscription check")
            return

        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {token}"}

            async with session.get(f"{PANEL_URL}/api/users", headers=headers, ssl=False) as resp:
                if resp.status != 200:
                    logging.warning(f"Could not fetch users: {resp.status}")
                    return
                
                users_data = await resp.json()
                users = users_data.get("users", []) if isinstance(users_data, dict) else users_data
                
                now = datetime.now(timezone.utc)
                
                for user in users:
                    username = user.get("username")
                    expire_val = user.get("expire")

                    # Skip if no expire date
                    if not expire_val or not username:
                        continue

                    # Skip if user not in our database
                    if username not in users_db:
                        continue

                    user_info = users_db[username]
                    tg_id = user_info.get("tg_id")

                    if not tg_id:
                        continue

                    expire_dt = parse_expire(expire_val)
                    if not expire_dt:
                        continue
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

# ══════════════════════════════════════════════════════════════
#  REFERRAL MENU
# ══════════════════════════════════════════════════════════════
@dp.callback_query(lambda cq: cq.data == "referral_menu")
async def cb_referral_menu(cq: types.CallbackQuery):
    tg_user = cq.from_user
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start=ref_{tg_user.id}"

    my_data = referrals_db.get(str(tg_user.id), {})
    referred_list = my_data.get("referred_users", []) if isinstance(my_data, dict) else []
    count = len(referred_list)
    bonus_days = get_referral_bonus_days()

    text = (
        "👥 <b>Реферальная программа</b>\n"
        "━━━━━━━━━━━━━━━━━━━\n\n"
        f"Приглашайте друзей и получайте <b>+{bonus_days} дней</b> к подписке "
        f"за каждого друга, который оформит подписку!\n\n"
        f"📊 <b>Вы привели:</b> {count} чел.\n\n"
        f"🔗 <b>Ваша реферальная ссылка:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"👆 <i>Нажмите, чтобы скопировать</i>"
    )

    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📤 Поделиться ссылкой", switch_inline_query=f"Подключайся к VPN! {ref_link}")],
        [types.InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_menu")],
    ])

    try:
        await cq.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        if "not modified" not in str(e):
            logging.error(f"Error editing referral menu: {e}")
    await cq.answer()


# ══════════════════════════════════════════════════════════════
#  SUPPORT / TICKET SYSTEM
# ══════════════════════════════════════════════════════════════
@dp.callback_query(lambda cq: cq.data == "support_menu")
async def cb_support_menu(cq: types.CallbackQuery):
    tg_user = cq.from_user
    tid, ticket = get_open_ticket_for_user(tg_user.id)

    if tid:
        msgs = ticket.get("messages", [])
        last_from = msgs[-1]["from"] if msgs else "—"
        status_text = "ожидает ответа админа" if last_from == "user" else "есть ответ"
        text = (
            "🎫 <b>Поддержка</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n\n"
            f"У вас есть открытый тикет <b>#{tid}</b> ({status_text}).\n\n"
            "Отправьте сообщение, оно будет добавлено к тикету.\n"
            "Или закройте тикет кнопкой ниже."
        )
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="💬 Написать в тикет", callback_data="ticket_write")],
            [types.InlineKeyboardButton(text="📋 История", callback_data=f"ticket_history:{tid}")],
            [types.InlineKeyboardButton(text="❌ Закрыть тикет", callback_data=f"ticket_close:{tid}")],
            [types.InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_menu")],
        ])
    else:
        text = (
            "🆘 <b>Поддержка</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n\n"
            "Опишите вашу проблему, и наш специалист ответит в ближайшее время."
        )
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="📝 Создать тикет", callback_data="ticket_write")],
            [types.InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_menu")],
        ])

    try:
        await cq.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        if "not modified" not in str(e):
            logging.error(f"Error editing support menu: {e}")
    await cq.answer()


@dp.callback_query(lambda cq: cq.data == "ticket_write")
async def cb_ticket_write(cq: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(TicketUserStates.waiting_for_message)
    await cq.message.edit_text(
        "📝 <b>Напишите ваше сообщение:</b>\n\n"
        "Опишите проблему одним сообщением.",
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="support_menu")]
        ]),
    )
    await cq.answer()


@dp.message(TicketUserStates.waiting_for_message, F.text)
async def ticket_user_message(m: Message, state: FSMContext):
    tg_user = m.from_user
    text = m.text.strip()
    if not text:
        await m.answer("❌ Пожалуйста, отправьте текстовое сообщение.")
        return

    tid, ticket = get_open_ticket_for_user(tg_user.id)
    if tid:
        # Append to existing ticket
        ticket["messages"].append({
            "from": "user",
            "text": text,
            "ts": datetime.now(timezone.utc).isoformat(),
        })
        save_tickets_db(tickets_db)
    else:
        tid = create_ticket(tg_user.id, tg_user.username, text)

    await state.clear()
    await m.answer(
        f"✅ <b>Тикет #{tid}</b> — сообщение отправлено!\n\n"
        "Ожидайте ответа от поддержки.",
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_menu")],
        ]),
    )

    # Notify admin
    display_name = f"@{tg_user.username}" if tg_user.username else str(tg_user.id)
    try:
        await bot.send_message(
            ADMIN_ID,
            f"🎫 <b>Тикет #{tid}</b> от {display_name}\n\n"
            f"{text}",
            parse_mode="HTML",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="💬 Ответить", callback_data=f"admin_ticket_reply:{tid}")],
                [types.InlineKeyboardButton(text="❌ Закрыть", callback_data=f"admin_ticket_close:{tid}")],
            ]),
        )
    except Exception as e:
        logging.error(f"Could not notify admin about ticket: {e}")


@dp.callback_query(lambda cq: cq.data.startswith("ticket_history:"))
async def cb_ticket_history(cq: types.CallbackQuery):
    tid = cq.data.split(":", 1)[1]
    ticket = tickets_db.get(tid)
    if not ticket or ticket.get("tg_id") != cq.from_user.id:
        await cq.answer("Тикет не найден", show_alert=True)
        return

    lines = [f"🎫 <b>Тикет #{tid}</b>\n"]
    for msg in ticket.get("messages", [])[-10:]:
        sender = "👤 Вы" if msg["from"] == "user" else "👨‍💻 Поддержка"
        lines.append(f"{sender}: {msg['text']}")

    text = "\n\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "…"

    await cq.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🔙 Назад", callback_data="support_menu")],
        ]),
    )
    await cq.answer()


@dp.callback_query(lambda cq: cq.data.startswith("ticket_close:"))
async def cb_ticket_close(cq: types.CallbackQuery):
    tid = cq.data.split(":", 1)[1]
    ticket = tickets_db.get(tid)
    if not ticket:
        await cq.answer("Тикет не найден", show_alert=True)
        return
    # Allow user or admin to close
    if ticket.get("tg_id") != cq.from_user.id and cq.from_user.id != ADMIN_ID:
        return
    ticket["status"] = "closed"
    ticket["closed_at"] = datetime.now(timezone.utc).isoformat()
    save_tickets_db(tickets_db)
    await cq.answer("✅ Тикет закрыт", show_alert=True)
    if cq.from_user.id == ADMIN_ID:
        await cb_admin_tickets(cq)
    else:
        await cb_support_menu(cq)


# ══════════════════════════════════════════════════════════════
#  ADMIN: TICKETS
# ══════════════════════════════════════════════════════════════
@dp.callback_query(lambda cq: cq.data == "admin_tickets")
async def cb_admin_tickets(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    open_tickets = [(tid, t) for tid, t in tickets_db.items() if t.get("status") == "open"]
    if not open_tickets:
        text = "🎫 <b>Тикеты</b>\n\nОткрытых тикетов нет."
    else:
        lines = ["🎫 <b>Открытые тикеты:</b>\n"]
        for tid, t in open_tickets[-20:]:
            uname = t.get("tg_username")
            display = f"@{uname}" if uname else str(t.get("tg_id"))
            msgs_count = len(t.get("messages", []))
            lines.append(f"• #{tid} — {display} ({msgs_count} сообщ.)")
        text = "\n".join(lines)

    buttons = []
    for tid, t in open_tickets[-10:]:
        uname = t.get("tg_username")
        display = f"@{uname}" if uname else str(t.get("tg_id"))
        buttons.append([types.InlineKeyboardButton(text=f"#{tid} {display}", callback_data=f"admin_ticket_view:{tid}")])
    buttons.append([types.InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")])

    kb = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    try:
        await cq.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        if "not modified" not in str(e):
            logging.error(f"Error in admin tickets: {e}")
    await cq.answer()


@dp.callback_query(lambda cq: cq.data.startswith("admin_ticket_view:"))
async def cb_admin_ticket_view(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    tid = cq.data.split(":", 1)[1]
    ticket = tickets_db.get(tid)
    if not ticket:
        await cq.answer("Тикет не найден", show_alert=True)
        return

    uname = ticket.get("tg_username")
    display = f"@{uname}" if uname else str(ticket.get("tg_id"))
    lines = [f"🎫 <b>Тикет #{tid}</b> от {display}\n"]

    for msg in ticket.get("messages", [])[-15:]:
        sender = "👤 Клиент" if msg["from"] == "user" else "👨‍💻 Вы"
        lines.append(f"{sender}: {msg['text']}")

    text = "\n\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "…"

    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="💬 Ответить", callback_data=f"admin_ticket_reply:{tid}")],
        [types.InlineKeyboardButton(text="❌ Закрыть тикет", callback_data=f"admin_ticket_close:{tid}")],
        [types.InlineKeyboardButton(text="🔙 К списку", callback_data="admin_tickets")],
    ])
    await cq.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await cq.answer()


@dp.callback_query(lambda cq: cq.data.startswith("admin_ticket_reply:"))
async def cb_admin_ticket_reply(cq: types.CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
    tid = cq.data.split(":", 1)[1]
    await state.clear()
    await state.update_data(ticket_id=tid)
    await state.set_state(AdminTicketStates.waiting_for_reply)
    await cq.message.edit_text(
        f"💬 <b>Ответ на тикет #{tid}</b>\n\nОтправьте текст ответа:",
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data=f"admin_ticket_view:{tid}")]
        ]),
    )
    await cq.answer()


@dp.message(AdminTicketStates.waiting_for_reply, F.text)
async def admin_ticket_reply_msg(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return
    data = await state.get_data()
    tid = data.get("ticket_id")
    ticket = tickets_db.get(tid)
    if not ticket:
        await m.answer("❌ Тикет не найден.")
        await state.clear()
        return

    reply_text = m.text.strip()
    ticket["messages"].append({
        "from": "admin",
        "text": reply_text,
        "ts": datetime.now(timezone.utc).isoformat(),
    })
    save_tickets_db(tickets_db)
    await state.clear()

    await m.answer(f"✅ Ответ отправлен в тикет #{tid}")

    # Notify user
    user_tg_id = ticket.get("tg_id")
    if user_tg_id:
        try:
            await bot.send_message(
                user_tg_id,
                f"💬 <b>Ответ поддержки (тикет #{tid}):</b>\n\n{reply_text}",
                parse_mode="HTML",
                reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="💬 Ответить", callback_data="ticket_write")],
                    [types.InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_menu")],
                ]),
            )
        except Exception as e:
            logging.error(f"Could not send ticket reply to user {user_tg_id}: {e}")
            await m.answer(f"⚠️ Не удалось отправить ответ пользователю: {e}")


@dp.callback_query(lambda cq: cq.data.startswith("admin_ticket_close:"))
async def cb_admin_ticket_close(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    tid = cq.data.split(":", 1)[1]
    ticket = tickets_db.get(tid)
    if not ticket:
        await cq.answer("Тикет не найден", show_alert=True)
        return
    ticket["status"] = "closed"
    ticket["closed_at"] = datetime.now(timezone.utc).isoformat()
    save_tickets_db(tickets_db)

    # Notify user
    user_tg_id = ticket.get("tg_id")
    if user_tg_id:
        try:
            await bot.send_message(
                user_tg_id,
                f"🎫 Тикет #{tid} закрыт поддержкой.\nЕсли проблема не решена, создайте новый тикет.",
                reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_menu")],
                ]),
            )
        except Exception:
            pass

    await cq.answer("✅ Тикет закрыт", show_alert=True)
    await cb_admin_tickets(cq)


# ══════════════════════════════════════════════════════════════
#  ADMIN: CHANNEL CONFIG
# ══════════════════════════════════════════════════════════════
@dp.callback_query(lambda cq: cq.data == "admin_channel")
async def cb_admin_channel(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    channel = get_required_channel()
    if channel:
        text = f"📢 <b>Обязательный канал:</b> @{channel}\n\nПользователи должны подписаться перед покупкой."
    else:
        text = "📢 <b>Обязательный канал:</b> не задан\n\nПользователи могут покупать без подписки."

    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="✏️ Задать канал", callback_data="admin_channel_set")],
        [types.InlineKeyboardButton(text="🗑 Убрать канал", callback_data="admin_channel_remove")],
        [types.InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")],
    ])
    await cq.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await cq.answer()


@dp.callback_query(lambda cq: cq.data == "admin_channel_set")
async def cb_admin_channel_set(cq: types.CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await state.set_state(AdminChannelStates.waiting_for_channel)
    await cq.message.edit_text(
        "📢 Отправьте username канала (например <code>@mychannel</code>).\n\n"
        "⚠️ Бот должен быть администратором этого канала!",
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="admin_channel")]
        ]),
    )
    await cq.answer()


@dp.message(AdminChannelStates.waiting_for_channel, F.text)
async def admin_channel_input(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return
    raw = m.text.strip().lstrip("@")
    if not raw or len(raw) < 3:
        await m.answer("❌ Введите корректный username канала.")
        return

    # Verify bot is admin of the channel
    try:
        chat = await bot.get_chat(f"@{raw}")
        me = await bot.get_me()
        member = await bot.get_chat_member(chat.id, me.id)
        if member.status not in ("administrator", "creator"):
            await m.answer("❌ Бот не является администратором этого канала. Добавьте бота и повторите.")
            return
    except Exception as e:
        await m.answer(f"❌ Не удалось проверить канал @{raw}: {e}")
        return

    set_required_channel(raw)
    await state.clear()
    await m.answer(f"✅ Канал установлен: @{raw}\n\nТеперь пользователи должны подписаться перед покупкой.", parse_mode="HTML")
    await m.answer("🛠 <b>Админ-панель</b>", reply_markup=get_admin_keyboard(), parse_mode="HTML")


@dp.callback_query(lambda cq: cq.data == "admin_channel_remove")
async def cb_admin_channel_remove(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    set_required_channel(None)
    await cq.answer("✅ Обязательный канал убран", show_alert=True)
    await cb_admin_channel(cq)


# ══════════════════════════════════════════════════════════════
#  ADMIN: BROADCAST
# ══════════════════════════════════════════════════════════════
@dp.callback_query(lambda cq: cq.data == "admin_broadcast")
async def cb_admin_broadcast(cq: types.CallbackQuery, state: FSMContext):
    if cq.from_user.id != ADMIN_ID:
        return
    await state.clear()

    total = len(set(info.get("tg_id") for info in users_db.values() if isinstance(info, dict) and info.get("tg_id")))

    await state.set_state(BroadcastStates.waiting_for_message)
    await cq.message.edit_text(
        f"📨 <b>Рассылка</b>\n\n"
        f"Всего пользователей: <b>{total}</b>\n\n"
        f"Отправьте текст рассылки (поддерживается HTML-разметка).",
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back")]
        ]),
    )
    await cq.answer()


@dp.message(BroadcastStates.waiting_for_message, F.text)
async def broadcast_text_entered(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return
    await state.update_data(broadcast_text=m.text)
    await state.set_state(BroadcastStates.waiting_for_confirm)

    await m.answer(
        f"📨 <b>Превью рассылки:</b>\n\n{m.text}\n\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Отправить? Напишите <b>да</b> для подтверждения.",
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back")]
        ]),
    )


@dp.message(BroadcastStates.waiting_for_confirm, F.text)
async def broadcast_confirm(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return

    if m.text.strip().lower() not in ("да", "yes", "д", "y"):
        await m.answer("❌ Рассылка отменена.")
        await state.clear()
        return

    data = await state.get_data()
    text = data.get("broadcast_text")
    await state.clear()

    if not text:
        await m.answer("❌ Текст рассылки пуст.")
        return

    # Collect unique tg_ids
    tg_ids = set()
    for info in users_db.values():
        if isinstance(info, dict) and info.get("tg_id"):
            tg_ids.add(info["tg_id"])

    sent = 0
    failed = 0
    status_msg = await m.answer(f"📨 Рассылка... 0/{len(tg_ids)}")

    for i, tg_id in enumerate(tg_ids, 1):
        try:
            await bot.send_message(tg_id, text, parse_mode="HTML")
            sent += 1
        except Exception:
            failed += 1

        if i % 25 == 0:
            try:
                await status_msg.edit_text(f"📨 Рассылка... {i}/{len(tg_ids)} (✅ {sent} / ❌ {failed})")
            except Exception:
                pass
            await asyncio.sleep(1)  # Telegram rate limit

    try:
        await status_msg.edit_text(
            f"✅ <b>Рассылка завершена</b>\n\n"
            f"Всего: {len(tg_ids)}\n"
            f"✅ Доставлено: {sent}\n"
            f"❌ Не доставлено: {failed}",
            parse_mode="HTML",
        )
    except Exception:
        await m.answer(f"✅ Рассылка завершена: {sent} доставлено, {failed} не доставлено")


# ══════════════════════════════════════════════════════════════
#  ADMIN: USER GROUPS CONFIGURATION
# ══════════════════════════════════════════════════════════════
@dp.callback_query(lambda cq: cq.data == "admin_groups")
async def cb_admin_groups(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    groups = await fetch_panel_groups()
    current_ids = get_user_group_ids()

    if not groups:
        await cq.answer("❌ Не удалось загрузить группы с панели", show_alert=True)
        return

    lines = ["🗂 <b>Группы для новых пользователей</b>\n"]
    buttons = []
    for g in groups:
        gid = g.get("id")
        name = g.get("name", "?")
        tags = ", ".join(g.get("inbound_tags", []))
        is_selected = gid in current_ids
        mark = "✅" if is_selected else "◻️"
        lines.append(f"{mark} <b>{name}</b> (id={gid}) — {tags}")
        action = "group_off" if is_selected else "group_on"
        buttons.append([types.InlineKeyboardButton(text=f"{mark} {name}", callback_data=f"admin_{action}:{gid}")])

    buttons.append([types.InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")])
    text = "\n".join(lines) + "\n\n<i>Нажмите на группу, чтобы включить/выключить.</i>"

    kb = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    await cq.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await cq.answer()


@dp.callback_query(lambda cq: cq.data.startswith("admin_group_on:") or cq.data.startswith("admin_group_off:"))
async def cb_admin_group_toggle(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return

    action, gid_str = cq.data.split(":", 1)
    try:
        gid = int(gid_str)
    except ValueError:
        await cq.answer("Ошибка", show_alert=True)
        return

    current = get_user_group_ids()

    if action == "admin_group_on":
        if gid not in current:
            current.append(gid)
    else:
        current = [x for x in current if x != gid]

    if not current:
        await cq.answer("⚠️ Нужна хотя бы одна группа!", show_alert=True)
        return

    set_user_group_ids(current)
    await cq.answer("✅ Сохранено")
    # Refresh the groups menu
    await cb_admin_groups(cq)


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
