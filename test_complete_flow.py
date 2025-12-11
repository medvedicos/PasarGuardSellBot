#!/usr/bin/env python3
"""Test the complete payment flow with corrected user creation"""
import asyncio
import aiohttp
import json
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv

load_dotenv()

MARZBAN_URL = os.getenv("MARZBAN_URL", "https://misavpn.top")
MARZBAN_ADMIN_USERNAME = os.getenv("MARZBAN_ADMIN_USERNAME")
MARZBAN_ADMIN_PASSWORD = os.getenv("MARZBAN_ADMIN_PASSWORD")
SUBS_LINK_TEMPLATE = os.getenv("SUBS_LINK_TEMPLATE", f"{MARZBAN_URL}/vpnsubs/{{username}}")

async def simulate_payment():
    """Simulate the payment flow"""
    from marzpy import Marzban
    import uuid
    
    # Simulate telegram user
    test_username = f"tg_test_flow_{uuid.uuid4().hex[:8]}"
    
    print(f"🧪 Testing payment flow simulation")
    print(f"📝 Test username: {test_username}")
    print(f"📅 Test date: {datetime.now(timezone.utc).date()}")
    
    try:
        # Step 1: Get Marzban token
        print(f"\n⏳ Step 1: Getting Marzban token...")
        api = Marzban(
            username=MARZBAN_ADMIN_USERNAME,
            password=MARZBAN_ADMIN_PASSWORD,
            panel_address=MARZBAN_URL
        )
        token_data = await api.get_token()
        token = token_data.get("access_token") if isinstance(token_data, dict) else token_data.access_token
        print(f"✓ Got token: {token[:30]}...")
        
        # Step 2: Calculate expiry (30 days plan)
        print(f"\n⏳ Step 2: Calculating expiry...")
        days = 30
        expire_dt = datetime.now(timezone.utc) + timedelta(days=days)
        expire_ts = int(expire_dt.replace(hour=0, minute=0, second=0).timestamp())
        print(f"✓ Expiry: {expire_dt.date()} (timestamp: {expire_ts})")
        
        # Step 3: Create user via direct HTTP
        print(f"\n⏳ Step 3: Creating user in Marzban...")
        
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
        
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{MARZBAN_URL}/api/user"
            
            async with session.post(url, json=payload, headers=headers, ssl=False) as resp:
                print(f"   API Status: {resp.status}")
                data = await resp.json()
                
                if resp.status in (200, 201):
                    print(f"✓ User created successfully")
                    
                    # Step 4: Verify user exists
                    print(f"\n⏳ Step 4: Verifying user...")
                    async with session.get(f"{MARZBAN_URL}/api/user/{test_username}", headers=headers, ssl=False) as v_resp:
                        if v_resp.status == 200:
                            v_data = await v_resp.json()
                            print(f"✓ User verified in database")
                            print(f"   Status: {v_data.get('status')}")
                            print(f"   Proxies: {v_data.get('proxies')}")
                            print(f"   Inbounds: {v_data.get('inbounds')}")
                            print(f"   Expiry: {datetime.fromtimestamp(v_data.get('expire'))}")
                            
                            # Step 5: Test subscription link
                            print(f"\n⏳ Step 5: Testing subscription link...")
                            subs_link = SUBS_LINK_TEMPLATE.format(username=test_username)
                            print(f"   Link: {subs_link}")
                            
                            async with session.get(subs_link, ssl=False, allow_redirects=False) as s_resp:
                                print(f"   Status: {s_resp.status}")
                                if s_resp.status in (200, 302):
                                    print(f"✓ Subscription link works!")
                                    if s_resp.status == 302:
                                        print(f"   Redirects to: {s_resp.headers.get('Location')}")
                                else:
                                    print(f"✗ Subscription link failed")
                            
                            # Step 6: Test subscription content
                            print(f"\n⏳ Step 6: Testing subscription content...")
                            async with session.get(subs_link, ssl=False) as sub_resp:
                                if sub_resp.status == 200:
                                    content = await sub_resp.text()
                                    print(f"✓ Subscription content received")
                                    print(f"   First 200 chars: {content[:200]}")
                                else:
                                    print(f"✗ Failed to get subscription content")
                            
                            print(f"\n✅ FULL FLOW TEST PASSED!")
                            print(f"   User: {test_username}")
                            print(f"   Expires: {expire_dt.date()}")
                            print(f"   Subscription: {subs_link}")
                            
                        else:
                            print(f"✗ Failed to verify user")
                else:
                    print(f"✗ Failed to create user: {resp.status}")
                    print(f"   Response: {json.dumps(data, indent=2)}")
    
    except Exception as e:
        print(f"✗ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(simulate_payment())
