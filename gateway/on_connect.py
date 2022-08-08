"""
Elastic License 2.0

Copyright Elasic and/or licensed to Elasic under one
or more contributor license agreements. Licensed under the Elastic License;
you may not use this file except in compliance with the Elastic License.
"""
# TODO: This module currently doesn't implement multiprocessing
# for such a thing, scaling without it would be impossible, meaning implement this ASAP.
import urllib.parse

from websockets.server import WebSocketServerProtocol

from gateway.v1 import v1Session

SETTING_NOT_FOUND = (KeyError, IndexError)


async def on_connection(ws: WebSocketServerProtocol, url: str):
    settings = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)

    try:
        version = settings['v'][0]
    except SETTING_NOT_FOUND:
        return await ws.close(4001, 'Invalid API version')
    else:
        if version not in ('1'):
            return await ws.close(4001, 'Invalid API version')

        version = int(version)

    try:
        compress = settings['compress'][0]
    except SETTING_NOT_FOUND:
        compress = 'false'
    else:
        if compress == 'true':
            compress = True
        elif compress == 'false':
            compress = False
        else:
            return await ws.close(4001, 'Invalid compress type')

    try:
        encoding = settings['encoding'][0]
    except:
        return await ws.close(4001, 'Invalid encoding type')
    else:
        if encoding not in ('json', 'msgpack'):
            return await ws.close(4001, 'Invalid encoding type')

    session = v1Session(version=version, encoding=encoding, compress=compress, ws=ws)

    # NOTE: This is a task which lasts until the connection drops.
    await session.run()
