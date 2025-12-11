import asyncio
import aiohttp
from marzpy import Marzban
from dotenv import load_dotenv

load_dotenv()

async def list_users():
    api = Marzban(
        username='siriusvpnadminmarzban',
        password='mIllIOnERm9773vpn',
        panel_address='https://misavpn.top'
    )
    token_data = await api.get_token()
    token = token_data.get('access_token')
    
    async with aiohttp.ClientSession() as session:
        headers = {'Authorization': f'Bearer {token}'}
        async with session.get('https://misavpn.top/api/users?skip=0&limit=100', headers=headers, ssl=False) as resp:
            data = await resp.json()
            print(f'Response type: {type(data)}')
            print(f'Response: {data}')
            if isinstance(data, dict):
                users = data.get('users', [])
                print(f'\nTotal users: {len(users)}')
                for user in users:
                    print(f"  - Username: {user.get('username')}, Proxies: {list(user.get('proxies', {}).keys())}, Inbounds: {user.get('inbounds')}")

asyncio.run(list_users())
