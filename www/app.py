import asyncio
from aiohttp import web
import os, time, json
from datetime import datetime
import logging; logging.basicConfig(level=logging.INFO)
def index(request):
    return web.Response(body=b'<h1>Awesome</h1>',content_type='text/html')

async def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)
    service = await loop.create_server(app.make_handler(), '127.0.0.1', 8000)
    logging.info('server starting on http://127.0.0.1:8000')
    return service

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop=loop))
loop.run_forever()