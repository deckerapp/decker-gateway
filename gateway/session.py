"""
Elastic License 2.0

Copyright Elasic and/or licensed to Elasic under one
or more contributor license agreements. Licensed under the Elastic License;
you may not use this file except in compliance with the Elastic License.
"""
from __future__ import annotations

import hashlib
import os
from typing import TYPE_CHECKING, Any, Literal

from websockets.server import WebSocketServerProtocol

from gateway.database import Event
from gateway.registry import Registry

if TYPE_CHECKING:
    from gateway.v1.models import Intents


# NOTE: private functions, and version-specific functions are not listed here because
# this is only meant for the registry to read, and sometimes write.
class BaseSession:
    _compressor: Any | None
    _socket: WebSocketServerProtocol
    _sequence: int
    guild_ids: list[int]
    user_id: int
    session_id: str
    intents: int | Intents
    version: int
    encoding: str
    compress: bool
    _registry: Registry
    floodgates_open: bool

    def __init__(
        self,
        version: int,
        encoding: Literal['json', 'msgpack'],
        compress: bool,
        ws: WebSocketServerProtocol,
    ) -> None:
        pass

    async def send(self, data: dict | Event) -> None:
        pass

    async def receive(self) -> None:
        pass

    async def run(self) -> None:
        pass

    def _create_session_id(self) -> str:
        return hashlib.sha1(os.urandom(128)).hexdigest()

    def _equip(self, reg: Registry) -> None:
        self._registry = reg
        self.session_id = self._create_session_id()
        reg._sessions.append({'id': self.session_id, 'session': self})

    def _equip_guilds(self):
        for guild_id in self.guild_ids:
            if guild_id not in self._registry._managed_guild_ids:
                self._registry._managed_guild_ids.append(guild_id)
                self._registry._managed_guilds.append(
                    {'id': guild_id, 'sessions': [self]}
                )
