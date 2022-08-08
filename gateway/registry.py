"""
Elastic License 2.0

Copyright Elasic and/or licensed to Elasic under one
or more contributor license agreements. Licensed under the Elastic License;
you may not use this file except in compliance with the Elastic License.
"""

from __future__ import annotations

import asyncio
import itertools
from asyncio import Queue, sleep
from typing import TYPE_CHECKING, TypedDict

from aiokafka import AIOKafkaConsumer

from gateway.database import Event, decode, get_hosts

if TYPE_CHECKING:
    from .session import BaseSession


class ManagedGuild(TypedDict):
    id: int
    sessions: list[int]


class ManagedSession(TypedDict):
    id: int
    session: BaseSession | None


class Registry:
    def __init__(self) -> None:
        self._sessions: list[ManagedSession] = []
        # manages the list of guilds managed by this registry
        self._managed_guilds: list[ManagedGuild] = []
        self._managed_guild_ids: list[int] = []
        self._queues: dict[str, Queue[Event]] = {}
        asyncio.create_task(self._manage_kafka())

    async def append_queue(self, session_id: str, event: Event) -> None:
        if not self._queues.get(session_id):
            queue = Queue()
            self._queues[session_id] = queue
        else:
            queue = self._queues[session_id]

        await queue.put(event)

    # TODO: use ws.broadcast for this.
    async def handle_guild_id_event(self, event: Event) -> None:
        for guild in self._managed_guilds:
            if guild['id'] == event.guild_id:
                for session_id, session in itertools.product(
                    guild['sessions'], self._sessions
                ):
                    if session['id'] == session_id:
                        assert not isinstance(session['session'], str)
                        session2 = session['session']

                        if session2.floodgates_open:
                            await session2.send(event)
                        else:
                            await self.append_queue(
                                session_id=session2.session_id, event=event
                            )

    async def handle_user_id_event(self, event: Event) -> None:
        for session in self._sessions:
            if session['session'].user_id == event.user_id:
                if session['session'].floodgates_open:
                    await session['session'].send(event)
                else:
                    await self.append_queue(session_id=session['id'], event=event)
                break

    async def _manage_kafka(self) -> None:
        consumer = AIOKafkaConsumer(bootstrap_servers=get_hosts('KAFKA_HOSTS'))

        await consumer.start()
        consumer.subscribe(
            [
                'guilds',
                'channels',
                'direct_messages',
                'messages',
                'reactions',
                'roles',
                'users',
                'security',
                'presences',
                'members',
                'relationships',
            ]
        )

        async for msg in consumer:
            event = decode(msg)
            if event.guild_id is not None:
                await self.handle_guild_id_event(event=event)

            elif event.guild_ids is not None:
                for guild_id in event.guild_ids:
                    # TODO: There has to be a more memory efficient way to do this
                    event = Event(event.name, event.data, guild_id=guild_id)
                    await self.handle_guild_id_event(event)

            elif event.user_id is not None:
                await self.handle_user_id_event(event=event)

            elif event.user_ids is not None:
                for user_id in event.user_ids:
                    # TODO: Same with the handling of guild_ids
                    event = Event(event.name, event.data, user_id=user_id)
                    await self.handle_user_id_event(event=event)

    async def disconnect(self, session_id: str, reconnectable: bool):
        for session in self._sessions:
            if session['id'] == session_id:
                session['session'] = None
                break

        assert session

        if reconnectable:
            await sleep(60)

            for session in self._sessions:
                if session['id'] == session_id:
                    if session['session'] is None:
                        return
                    session = session
                    break

        self._sessions.pop(session)

        if self._queues.get(session_id):
            self._queues.pop(session_id)
