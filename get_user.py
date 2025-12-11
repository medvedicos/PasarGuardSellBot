import asyncio
import aiohttp
from marzpy import Marzban
from dotenv import load_dotenv

load_dotenv()

async def get_user():
    api = Marzban(
        username='siriusvpnadminmarzban',
        password='mIllIOnERm9773vpn',
        panel_address='https://misavpn.top'
    )
    token_data = await api.get_token()
    token = token_data.get('access_token')
    
    async with aiohttp.ClientSession() as session:
        headers = {'Authorization': f'Bearer {token}'}
        async with session.get('https://misavpn.top/api/user/tg_mizuvil', headers=headers, ssl=False) as resp:
            data = await resp.json()
            print('User data:')
            print(f"Status: {resp.status}")
            print(f"Data: {data}")
            if isinstance(data, dict):
                print(f"\nProxies: {data.get('proxies')}")
                print(f"Inbounds: {data.get('inbounds')}")
                print(f"Subscription URL: {data.get('subscription_url')}")

asyncio.run(get_user())
