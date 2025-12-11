import os
import time
import logging
import asyncio
import uuid
import json
from datetime import datetime, timedelta, timezone
import aiohttp
from marzpy import Marzban
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import LabeledPrice, Invoice, PreCheckoutQuery, Message
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
        [types.InlineKeyboardButton(text="👤 Мой профиль", callback_data="cabinet")],
        [types.InlineKeyboardButton(text="💎 Купить подписку", callback_data="buy_menu")],
        [types.InlineKeyboardButton(text="🎁 Пробный период (3 дня)", callback_data="trial_subs")],
        [types.InlineKeyboardButton(text="🆘 Поддержка", url="https://t.me/Mizuvil")],
    ])
    return kb

# Create cabinet keyboard
def get_cabinet_keyboard():
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🔗 Подключить (Ссылка)", callback_data="get_link")],
        [types.InlineKeyboardButton(text="🔄 Продлить", callback_data="renew_menu")],
        [types.InlineKeyboardButton(text="🔙 Главное меню", callback_data="back_to_menu")],
    ])
    return kb

# Create buy menu keyboard
def get_buy_keyboard():
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=f"🗓 {PLANS['m1']['title']} — {PLANS['m1']['price']} ⭐️ (~{PLANS['m1']['price_rub']}₽)", callback_data="buy:m1")],
        [types.InlineKeyboardButton(text=f"🗓 {PLANS['m3']['title']} — {PLANS['m3']['price']} ⭐️ (~{PLANS['m3']['price_rub']}₽)", callback_data="buy:m3")],
        [types.InlineKeyboardButton(text=f"🗓 {PLANS['m6']['title']} — {PLANS['m6']['price']} ⭐️ (~{PLANS['m6']['price_rub']}₽)", callback_data="buy:m6")],
        [types.InlineKeyboardButton(text=f"🗓 {PLANS['y1']['title']} — {PLANS['y1']['price']} ⭐️ (~{PLANS['y1']['price_rub']}₽)", callback_data="buy:y1")],
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
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="💰 Сменить тарифы", callback_data="admin_prices")],
    ])
    
    await m.answer("🛠 <b>Админ-панель</b>", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(lambda cq: cq.data == "admin_prices")
async def cb_admin_prices(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
        
    buttons = []
    for key, plan in PLANS.items():
        buttons.append([types.InlineKeyboardButton(
            text=f"{plan['title']} — {plan['price']} ⭐️", 
            callback_data=f"edit_price:{key}"
        )])
    
    buttons.append([types.InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")])
    kb = types.InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await cq.message.edit_text("💰 <b>Выберите тариф для изменения цены:</b>", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(lambda cq: cq.data == "admin_back")
async def cb_admin_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
        
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="💰 Сменить тарифы", callback_data="admin_prices")],
    ])
    
    await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=kb, parse_mode="HTML")

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
        save_plans(PLANS)
        
        await m.answer(f"✅ Цена для тарифа <b>{PLANS[plan_key]['title']}</b> изменена на {new_price} ⭐️", parse_mode="HTML")
        
        # Show admin menu again
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="💰 Сменить тарифы", callback_data="admin_prices")],
        ])
        await m.answer("🛠 <b>Админ-панель</b>", reply_markup=kb, parse_mode="HTML")
    else:
        await m.answer("❌ Ошибка: тариф не найден.")
        
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
    
    if user_data is None:
        text = (
            "👤 <b>Мой профиль</b>\n\n"
            "❌ <b>Нет активной подписки</b>\n"
            "Оформите подписку, чтобы получить доступ к VPN."
        )
    else:
        text = f"👤 <b>Мой профиль</b>\n\n{format_user_info(user_data)}"
    
    # Edit previous message instead of sending new one
    try:
        await cq.message.edit_text(text, reply_markup=get_cabinet_keyboard(), parse_mode="HTML")
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
    text = (
        "💎 <b>Выберите тарифный план:</b>\n\n"
        "⚡️ Высокая скорость\n"
        "🌍 Локации по всему миру\n"
        "♾ Безлимитный трафик\n"
    )
    try:
        await cq.message.edit_text(text, reply_markup=get_buy_keyboard(), parse_mode="HTML")
    except Exception as e:
        if "not modified" not in str(e):
            logging.error(f"Error editing message: {e}")
        await cq.answer()
    else:
        await cq.answer()

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
            await cq.message.edit_text(text, reply_markup=get_cabinet_keyboard(), parse_mode="HTML")
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
            await cq.message.edit_text(text, reply_markup=get_cabinet_keyboard(), parse_mode="HTML")
        except Exception as e:
            logging.warning(f"Could not edit message: {e}")
            await cq.answer(subs_link, show_alert=False)
    
    await cq.answer()

@dp.callback_query(lambda cq: cq.data == "renew_menu")
async def cb_renew_menu(cq: types.CallbackQuery):
    """Show renewal plans menu"""
    text = "🔄 <b>Продление подписки:</b>\n\nВыберите срок продления:"
    try:
        await cq.message.edit_text(text, reply_markup=get_renew_keyboard(), parse_mode="HTML")
    except Exception as e:
        if "not modified" not in str(e):
            logging.error(f"Error editing message: {e}")
        await cq.answer()
    else:
        await cq.answer()

# Create renewal keyboard (same as buy but with different callback)
def get_renew_keyboard():
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=f"🗓 {PLANS['m1']['title']} — {PLANS['m1']['price']} ⭐️ (~{PLANS['m1']['price_rub']}₽)", callback_data="renew:m1")],
        [types.InlineKeyboardButton(text=f"🗓 {PLANS['m3']['title']} — {PLANS['m3']['price']} ⭐️ (~{PLANS['m3']['price_rub']}₽)", callback_data="renew:m3")],
        [types.InlineKeyboardButton(text=f"🗓 {PLANS['m6']['title']} — {PLANS['m6']['price']} ⭐️ (~{PLANS['m6']['price_rub']}₽)", callback_data="renew:m6")],
        [types.InlineKeyboardButton(text=f"🗓 {PLANS['y1']['title']} — {PLANS['y1']['price']} ⭐️ (~{PLANS['y1']['price_rub']}₽)", callback_data="renew:y1")],
        [types.InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_cabinet")],
    ])
    return kb

@dp.callback_query(lambda cq: cq.data == "back_to_cabinet")
async def cb_back_to_cabinet(cq: types.CallbackQuery):
    """Go back to cabinet"""
    tg_user = cq.from_user
    mb_username = build_marzban_username(tg_user)
    
    user_data = await marzban_get_user(mb_username)
    
    if user_data is None:
        text = (
            "👤 <b>Мой профиль</b>\n\n"
            "❌ <b>Нет активной подписки</b>\n"
            "Оформите подписку, чтобы получить доступ к VPN."
        )
    else:
        text = f"👤 <b>Мой профиль</b>\n\n{format_user_info(user_data)}"
    
    try:
        await cq.message.edit_text(text, reply_markup=get_cabinet_keyboard(), parse_mode="HTML")
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
    
    # Create invoice (Stars)
    prices = [LabeledPrice(label=plan["title"], amount=plan["price"])]
    
    await bot.send_invoice(
        chat_id=cq.from_user.id,
        title=f"Продление: {plan['title']}",
        description=f"Продление подписки на {plan['title']} — {plan['price']} ⭐",
        payload=f"renew:{plan_key}:{cq.from_user.id}",
        provider_token="",
        start_parameter=f"renew_{plan_key}",
        currency="XTR",
        prices=prices,
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
    
    # Create invoice (Stars)
    prices = [LabeledPrice(label=plan["title"], amount=plan["price"])]
    
    await bot.send_invoice(
        chat_id=cq.from_user.id,
        title=f"{plan['title']} на {plan['days']} дней",
        description=f"Подписка {plan['title']} — {plan['price']} ⭐",
        payload=f"purchase:{plan_key}:{cq.from_user.id}",
        provider_token="",
        start_parameter=f"buy_{plan_key}",
        currency="XTR",
        prices=prices,
    )
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
        payment_type, plan_key, buyer_id_str = payload.split(":")
    except Exception:
        payment_type = "purchase"
        plan_key = None
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
    
    # Save or update user to database for notification system
    users_db[mb_username] = {
        "tg_id": tg_user.id,
        "tg_username": tg_user.username,
        "expire_ts": expire_ts,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    save_users_db(users_db)
    logging.info(f"Updated user {mb_username} (TG ID: {tg_user.id}) in database")
    
    # Get subscription URL from the API response
    subs_link = res.get("subscription_url")
    if not subs_link:
        subs_link = SUBS_LINK_TEMPLATE.format(username=mb_username)
    
    logging.info(f"Subscription link: {subs_link}")
    
    text = (
        f"✅ <b>Оплата прошла успешно!</b>\n\n"
        f"Подписка {action_text}.\n"
        f"📅 <b>Действует до:</b> {expire_dt.strftime('%d.%m.%Y')}\n\n"
        f"🔗 <b>Ваша ссылка для подключения:</b>\n<code>{subs_link}</code>\n\n"
        f"👆 <i>Нажмите на ссылку, чтобы скопировать</i>\n"
        f"💡 Вставьте её в ваше VPN-приложение."
    )
    await m.answer(text, reply_markup=get_main_keyboard(), parse_mode="HTML")

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
