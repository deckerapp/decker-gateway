"""
Elastic License 2.0

Copyright Discend and/or licensed to Discend under one
or more contributor license agreements. Licensed under the Elastic License;
you may not use this file except in compliance with the Elastic License.
"""
import asyncio
import base64
import binascii
import traceback
import zlib
from asyncio import Task
from typing import Any, Coroutine, Literal, Sequence

import itsdangerous
import msgspec
import websockets.exceptions
from pydantic import ValidationError
from websockets.server import WebSocketServerProtocol

from gateway.database import (
    Activity,
    CategoryChannel,
    Channel,
    DMChannel,
    Event,
    Feature,
    GatewaySessionLimit,
    GroupDMChannel,
    Guild,
    Member,
    NotFound,
    Presence,
    ReadState,
    Recipient,
    Relationship,
    Role,
    Settings,
    TextChannel,
    User,
    WSMessage,
    encode,
    objectify,
)
from gateway.registry import Registry
from gateway.session import BaseSession

from .models import Identify, Intents


def yield_chunks(input_list: Sequence[Any], chunk_size: int):
    for idx in range(0, len(input_list), chunk_size):
        yield input_list[idx : idx + chunk_size]


OPCODES = {2: 'identify'}
REGISTRY: Registry | None = None


class v1Session(BaseSession):
    def __init__(
        self,
        version: int,
        encoding: Literal['json', 'msgpack'],
        compress: bool,
        ws: WebSocketServerProtocol,
    ) -> None:
        global REGISTRY
        self.version = version
        self.encoding = encoding
        self.compress = compress
        # self.interval = randint(40, 46) * 1000
        self._socket = ws
        self._sequence = 0
        self.identified: bool = False
        # TODO: let this be special for big bots
        self._rate_limit: int = 60
        self.loop = asyncio.get_running_loop()
        self._compressor = None
        self.disconnected: bool = False

        if compress:
            self._compressor = zlib.compressobj()

        if REGISTRY is None:
            REGISTRY = Registry()

        self._registry = REGISTRY

    async def send(
        self, data: dict | Event, op: int = 0, add_essentials: bool = True
    ) -> None:
        new_data = {'op': op}

        if add_essentials:
            self._sequence += 1
            new_data['t'] = data.name
            new_data['s'] = self._sequence

        new_data['d'] = data.data if isinstance(data, Event) else data

        if self.compress is True:
            await self._socket.send(
                yield_chunks(
                    input_size=encode(
                        data=new_data,
                        compressor=self._compressor,
                        encoding=self.encoding,
                    ),
                    chunk_size=1024,
                )
            )
        else:
            await self._socket.send(encode(new_data, None, encoding=self.encoding))

    async def close(self, code: int, reason: str, reconnectable: bool):
        await self._socket.close(code=code, reason=reason)
        await self._registry.disconnect(self.session_id, reconnectable)

    def _validate_token(self, token: str) -> Task | User:
        fragmented = token.split('.')
        user_id = fragmented[0]

        try:
            user_id = base64.b64decode(user_id.encode())
            user_id = int(user_id)
        except (ValueError, binascii.Error):
            return asyncio.create_task(self.close(4005, 'Invalid token', False))

        try:
            user: User = User.objects(User.id == user_id).only(['password']).get()
        except:
            return asyncio.create_task(self.close(4005, 'Invalid token', False))

        signer = itsdangerous.TimestampSigner(user.password)

        try:
            signer.unsign(token)

            return User.objects(User.id == user_id).get()
        except (itsdangerous.BadSignature):
            return asyncio.create_task(self.close(4005, 'Invalid token', False))

    async def validate_token(self, token: str) -> bool | User:
        result = await self.loop.run_in_executor(None, self._validate_token, token)

        if not isinstance(result, Task):
            return result

        await result
        return False

    def _decrease_connection_count(self) -> bool:
        # sourcery skip: simplify-numeric-comparison
        try:
            gwsl: GatewaySessionLimit = GatewaySessionLimit.objects(
                GatewaySessionLimit.user_id == self.user_id
            ).get()
        except NotFound:
            gwsl: GatewaySessionLimit = GatewaySessionLimit.create(user_id=self.user_id)

        if (gwsl.remaining - 1) < 0:
            return False

        gwsl.update(remaining=gwsl.remaining - 1)
        return True

    async def decrease_connection_count(self) -> bool:
        return await self.loop.run_in_executor(None, self._decrease_connection_count)

    def _get_joined_guild_ids(self) -> list[int]:
        member_guild_ids: list[Member] = (
            Member.objects(Member.user_id == self.user_id).only(['guild_id']).all()
        )

        return [obj.guild_id for obj in member_guild_ids]

    async def get_joined_guild_ids(self) -> list[int]:
        return await self.loop.run_in_executor(None, self._get_joined_guild_ids)

    def _get_guild_channels(self, guild_id: int) -> list[dict]:
        category_channels: list[CategoryChannel] = CategoryChannel.objects(
            CategoryChannel.guild_id == guild_id
        ).all()
        text_channels: list[TextChannel] = TextChannel.objects(
            TextChannel.guild_id == guild_id
        ).all()

        # TODO: permission overwrites
        dict_category_channels: list[dict] = [
            objectify(dict(channel)) for channel in category_channels
        ]
        dict_text_channels: list[dict] = [
            objectify(dict(channel)) for channel in text_channels
        ]

        return dict_category_channels + dict_text_channels

    def _get_guild_roles(self, guild_id: int) -> list[dict]:
        return [
            objectify(dict(role))
            for role in Role.objects(Role.guild_id == guild_id).all()
        ]

    def _get_guild_features(self, guild_id: int) -> list[str]:
        return [
            feature.value
            for feature in Feature.objects(Feature.guild_id == guild_id)
            .only(['value'])
            .all()
        ]

    def _get_joined_guilds(self, guild_ids: list[int]) -> list[Task]:
        tasks: list[Task] = []

        for guild_id in guild_ids:
            guild: dict = objectify(dict(Guild.objects(Guild.id == guild_id).get()))
            channels = self._get_guild_channels(guild_id=guild_id)
            roles = self._get_guild_roles(guild_id=guild_id)

            guild['channels'] = channels
            guild['roles'] = roles
            guild['features'] = self._get_guild_features(guild_id=guild_id)
            tasks.append(self.send(Event('GUILD_CREATE', guild)))

        return tasks

    async def send_guilds(self) -> None:
        tasks = await self.loop.run_in_executor(
            None, self._get_joined_guilds, self.guild_ids
        )
        # Spam the user all at once when guild processing is done
        await asyncio.gather(*tasks)

    async def empty_queue(self) -> None:
        queue = self._registry._queues.get(self.session_id)

        if queue is None:
            return
        elif queue.empty():
            return

        for _ in range(queue.qsize() - 1):
            event = await queue.get()
            await self.send(event)

    def _get_relationships(self) -> tuple[list[dict], list[dict]]:
        rels: list[Relationship] = Relationship.objects(
            Relationship.user_id == self.user_id
        ).all()

        ret: list[dict] = []
        ret_friend_presences: list[dict] = []

        for rel in rels:
            reld = dict(rel)
            reld.pop('user_id')
            target_id = reld.pop('target_id')
            reld['user'] = (
                User.objects(User.id == target_id).defer(['password', 'email']).get()
            )

            ret.append(reld)
            if rel.type == 0:
                try:
                    presence: Presence = Presence.objects(
                        Presence.user_id == target_id
                    ).get()
                except NotFound:
                    continue

                _activities: list[Activity] = Activity.objects(
                    Activity.user_id == target_id
                ).all()

                activities = [dict(activity) for activity in _activities]
                presd = dict(presence)
                presd['activities'] = activities
                ret_friend_presences.append(presd)

        return ret, ret_friend_presences

    def _get_client_status(self) -> str:
        dev = self.identify.properties.device

        if dev == 'Discend Mobile':
            return 'mobile'
        elif dev == 'Discend Web':
            return 'web'
        elif dev == 'Discend Desktop':
            return 'desktop'
        else:
            return 'unknown'

    def _set_presence(self) -> None:
        setting: Settings = (
            Settings.objects(Settings.user_id == self.user_id).only(['status']).get()
        )

        try:
            presence: Presence = Presence.objects(
                Presence.user_id == self.user_id
            ).get()
        except NotFound:
            Presence.create(
                user_id=self.user_id,
                status=setting.status or 'online',
                client_status=self._get_client_status(),
            )
        else:
            if presence.status == 'invisible' and setting.status != 'invisible':
                presence.update(status=setting.status)

    async def set_presence(self) -> None:
        await self.loop.run_in_executor(None, self._set_presence)

    def _delete_presence(self) -> None:
        setting: Settings = (
            Settings.objects(Settings.user_id == self.user_id).only(['status']).get()
        )

        if setting.status == 'invisible':
            return

        presence: Presence = Presence.objects(Presence.user_id == self.user_id).get()
        presence.update(status='invisible')

    async def delete_presence(self) -> None:
        await self.loop.run_in_executor(None, self._delete_presence)

    async def get_relationships(self) -> tuple[list[dict], list[dict]]:
        return await self.loop.run_in_executor(None, self._get_relationships)

    def _get_user_channels(self) -> tuple[list[dict], list[dict]]:
        ret_normal = []
        ret_group = []
        recp: list[Recipient] = Recipient.objects(
            Recipient.user_id == self.user_id
        ).all()

        for recipient in recp:
            channel: Channel = Channel.objects(Channel.id == recipient.channel_id).get()
            recipients = [
                objectify(dict(r))
                for r in Recipient.objects(Recipient.channel_id).all()
                if r.user_id != recipient.user_id
            ]

            if channel.type == 1:
                chn = (
                    dict(DMChannel.objects(DMChannel.channel_id == channel.id).get())
                    | channel
                )
                chn['recipients'] = recipients
                ret_normal.append(objectify(chn, True))
            elif channel.type == 2:
                chn = (
                    dict(
                        GroupDMChannel.objects(
                            GroupDMChannel.channel_id == channel.id
                        ).get()
                    )
                    | channel
                )
                chn['recipients'] = recipients
                ret_group.append(objectify(chn, True))

        return ret_normal, ret_group

    async def get_user_channels(self) -> tuple[list[dict], list[dict]]:
        return await self.loop.run_in_executor(None, self._get_user_channels)

    async def send_ready(self) -> None:
        _readstates = ReadState.objects(ReadState.user_id == self.user_id).all()
        readstates: dict[str, Any] = [dict(readstate) for readstate in _readstates]
        settings = (
            Settings.objects(Settings.user_id == self.user_id).defer(['mfa_code']).get()
        )

        relationships, friend_presences = await self.get_relationships()
        user_dms, group_dms = await self.get_user_channels()
        self._equip(reg=self._registry)
        await self.send(
            Event(
                'READY',
                {
                    'settings': dict(settings),
                    'read_states': readstates,
                    'relationships': relationships,
                    'friend_presences': friend_presences,
                    'guilds': self.guild_ids,
                    'direct_messages': {'single': user_dms, 'grouped': group_dms},
                },
            )
        )

    async def on_identify(self, data: dict[str, Any]):
        if self.identified:
            return await self.close(4007, 'You have already identified', True)

        try:
            # TODO: track analytics data stored here
            identify = Identify.validate(data)
        except ValidationError:
            await self._socket.close(4004, 'Invalid data sent')
            await self._registry.disconnect(self.session_id, reconnectable=False)
            return

        self.identify = identify

        self.intents = Intents(identify.intents)
        is_valid = await self.validate_token(identify.token)

        if not is_valid:
            # TODO: cleanups
            return

        self.token = identify.token
        self.user_id = is_valid.id
        decreased_connection_count = await self.decrease_connection_count()

        if not decreased_connection_count:
            return await self.close(
                4006, 'You have reached your connection limit for today', False
            )

        self.guild_ids = await self.get_joined_guild_ids()
        await self.send_ready()
        await self.send_guilds()
        await self.empty_queue()
        self.floodgates_open = True

    def _model_to_dict(self, model: Event):
        return {
            'name': model.name,
            'data': model.data,
            'guild_id': model.guild_id,
            'guild_ids': model.guild_ids,
            'user_id': model.user_id,
            'user_ids': model.user_ids,
        }

    async def hello(self):
        await self.send({'rate_limit': self._rate_limit}, 1, add_essentials=False)

    async def handle_events(self):
        async for msg in self._socket:
            try:
                data = msgspec.json.decode(msg.encode(), type=WSMessage)
            except msgspec.DecodeError:
                await self._socket.close(4002, 'Invalid json object')
                await self._registry.disconnect(self.session_id, reconnectable=False)
                break

            try:
                stringified_op = OPCODES[data.op]
            except KeyError:
                await self._socket.close(4003, 'Invalid OP code')
                await self._registry.disconnect(self.session_id, reconnectable=False)
                break

            handler: Coroutine = getattr(self, f'on_{stringified_op}')
            await handler(data.d)

    async def run(self) -> None:
        try:
            await self.hello()
            await self.handle_events()
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as exc:
            # TODO: hook this up with something like sentry
            traceback.print_exception(exc)
            await self.close(4000, 'Unknown exception occured, please reconnect.', True)
        finally:
            if self.identified:
                await self.delete_presence()
                await self._registry.disconnect(self.session_id, True)
