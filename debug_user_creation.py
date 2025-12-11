#!/usr/bin/env python3
"""Debug user creation - see exactly what we're sending"""
import asyncio
import uuid
import json
from marzpy import Marzban
from marzpy.api.user import User
from dotenv import load_dotenv
import os

load_dotenv()

MARZBAN_URL = os.getenv("MARZBAN_URL", "https://misavpn.top")
MARZBAN_ADMIN_USERNAME = os.getenv("MARZBAN_ADMIN_USERNAME")
MARZBAN_ADMIN_PASSWORD = os.getenv("MARZBAN_ADMIN_PASSWORD")

async def debug_user_creation():
    """Debug what gets sent to Marzban"""
    from datetime import datetime, timedelta, timezone
    import aiohttp
    
    test_username = f"tg_debug_{uuid.uuid4().hex[:6]}"
    expire_dt = datetime.now(timezone.utc) + timedelta(days=30)
    expire_ts = int(expire_dt.replace(hour=0, minute=0, second=0).timestamp())
    
    print(f"Creating user: {test_username}")
    print(f"Expire: {expire_ts}")
    
    # Get token directly
    api = Marzban(
        username=MARZBAN_ADMIN_USERNAME,
        password=MARZBAN_ADMIN_PASSWORD,
        panel_address=MARZBAN_URL
    )
    
    token_data = await api.get_token()
    token = token_data.get("access_token") if isinstance(token_data, dict) else token_data.access_token
    print(f"✓ Got token: {token[:20]}...")
    
    # Create user object
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
    
    # See what the User object looks like
    print("\n📦 User object attributes:")
    for attr in dir(user):
        if not attr.startswith('_'):
            try:
                val = getattr(user, attr)
                if not callable(val):
                    print(f"   {attr}: {val}")
            except:
                pass
    
    # Try to see if it has a dict method
    print("\n📝 User object dict:")
    if hasattr(user, '__dict__'):
        print(json.dumps({k: v for k, v in user.__dict__.items() if not k.startswith('_')}, indent=2, default=str))
    
    # Try direct HTTP POST
    print("\n🌐 Trying direct HTTP POST to /api/user")
    
    payload = {
        "username": test_username,
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
    
    print(f"📤 Payload:")
    print(json.dumps(payload, indent=2))
    
    async with aiohttp.ClientSession() as session:
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{MARZBAN_URL}/api/user"
        
        async with session.post(url, json=payload, headers=headers, ssl=False) as resp:
            print(f"\n📥 Response status: {resp.status}")
            data = await resp.json()
            print(f"   Response: {json.dumps(data, indent=2)}")
            
            if resp.status == 201 or resp.status == 200:
                print(f"✓ User created via HTTP!")
                
                # Verify
                print(f"\n🔍 Verifying...")
                async with session.get(f"{MARZBAN_URL}/api/user/{test_username}", headers=headers, ssl=False) as v_resp:
                    print(f"   Status: {v_resp.status}")
                    v_data = await v_resp.json()
                    if v_resp.status == 200:
                        print(f"   ✓ User found!")
                        print(f"   Proxies: {v_data.get('proxies')}")
                    else:
                        print(f"   ✗ Not found: {v_data}")

if __name__ == "__main__":
    asyncio.run(debug_user_creation())
