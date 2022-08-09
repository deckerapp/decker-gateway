"""
Elastic License 2.0

Copyright Discend and/or licensed to Discend under one
or more contributor license agreements. Licensed under the Elastic License;
you may not use this file except in compliance with the Elastic License.
"""
import asyncio
import os
import sys

import sentry_sdk
import websockets.server
from dotenv import load_dotenv

from gateway.database import connect
from gateway.on_connect import on_connection


async def start():
    load_dotenv()
    async with websockets.server.serve(
        on_connection, '0.0.0.0', 6000, ping_interval=32, ping_timeout=32
    ):
        sentry_sdk.init(dsn=os.environ['SENTRY_DSN'], traces_sample_rate=1.0)
        connect()
        print('DEBUG:Starting to serve on port 6000', file=sys.stderr)
        await asyncio.Future()


asyncio.run(start())
