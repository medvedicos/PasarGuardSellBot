#!/usr/bin/env python3
"""Test user creation with correct proxy format"""
import asyncio
import uuid
from marzpy import Marzban
from dotenv import load_dotenv
import os

load_dotenv()

MARZBAN_URL = os.getenv("MARZBAN_URL", "https://misavpn.top")
MARZBAN_ADMIN_USERNAME = os.getenv("MARZBAN_ADMIN_USERNAME")
MARZBAN_ADMIN_PASSWORD = os.getenv("MARZBAN_ADMIN_PASSWORD")

async def test_create_user():
    """Test creating a user with correct proxy format"""
    from marzpy.api.user import User
    from datetime import datetime, timedelta, timezone
    
    # Create username
    test_username = f"tg_test_{uuid.uuid4().hex[:6]}"
    
    # Create expiry (30 days from now)
    expire_dt = datetime.now(timezone.utc) + timedelta(days=30)
    expire_ts = int(expire_dt.replace(hour=0, minute=0, second=0).timestamp())
    
    print(f"📝 Creating test user: {test_username}")
    print(f"📅 Expiry timestamp: {expire_ts}")
    print(f"📅 Expiry date: {expire_dt.date()}")
    
    try:
        api = Marzban(
            username=MARZBAN_ADMIN_USERNAME,
            password=MARZBAN_ADMIN_PASSWORD,
            panel_address=MARZBAN_URL
        )
        
        # Get token
        token_data = await api.get_token()
        token = token_data.get("access_token") if isinstance(token_data, dict) else token_data.access_token
        print(f"✓ Got Marzban token")
        
        # Create user with correct proxy format
        user = User(
            username=test_username,
            proxies={
                "vless": {
                    "id": str(uuid.uuid4()),
                    "flow": "xtls-rprx-vision"
                }
            },
            inbounds={"vless": ["Steal"]},
            data_limit=0,
            expire=expire_ts
        )
        user.status = "active"
        
        print(f"📦 User object created, adding to Marzban...")
        
        try:
            result = await api.add_user(user=user, token=token)
            print(f"✓ User created successfully: {result}")
        except (AttributeError, TypeError) as e:
            print(f"⚠️  AttributeError (user likely created anyway): {e}")
        
        # Now verify the user exists
        print(f"\n🔍 Verifying user was created...")
        
        # Direct HTTP verification
        import aiohttp
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{MARZBAN_URL}/api/user/{test_username}"
            print(f"📡 Query: GET {url}")
            
            async with session.get(url, headers=headers, ssl=False) as resp:
                print(f"   Status: {resp.status}")
                data = await resp.json()
                if resp.status == 200:
                    print(f"✓ User found in Marzban!")
                    print(f"   Proxies: {data.get('proxies')}")
                    print(f"   Inbounds: {data.get('inbounds')}")
                    print(f"   Status: {data.get('status')}")
                else:
                    print(f"✗ User NOT found: {data}")
    
    except Exception as e:
        print(f"✗ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_create_user())
