import asyncio
import aiohttp
from marzpy import Marzban
from dotenv import load_dotenv

load_dotenv()

async def get_inbounds():
    api = Marzban(
        username='siriusvpnadminmarzban',
        password='mIllIOnERm9773vpn',
        panel_address='https://misavpn.top'
    )
    token_data = await api.get_token()
    token = token_data.get('access_token')
    
    async with aiohttp.ClientSession() as session:
        headers = {'Authorization': f'Bearer {token}'}
        async with session.get('https://misavpn.top/api/inbounds', headers=headers, ssl=False) as resp:
            data = await resp.json()
            print('Inbounds:')
            print(f"Type of data: {type(data)}")
            print(f"Data: {data}")
            if isinstance(data, list):
                for inbound in data:
                    if isinstance(inbound, dict):
                        print(f"  - ID/Tag: {inbound.get('tag')}, Protocol: {inbound.get('protocol')}, Port: {inbound.get('port')}")
                    elif isinstance(inbound, str):
                        print(f"  - Tag: {inbound}")

asyncio.run(get_inbounds())
