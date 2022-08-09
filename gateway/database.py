"""
Elastic License 2.0

Copyright Discend and/or licensed to Discend under one
or more contributor license agreements. Licensed under the Elastic License;
you may not use this file except in compliance with the Elastic License.
"""
import os
from typing import Any, Literal, TypeVar
from zlib import Z_SYNC_FLUSH

import msgspec
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine import columns, connection, models, query


def get_hosts(name: str):
    hs = os.getenv(name)

    return None if hs is None else hs.split(',')


auth_provider = PlainTextAuthProvider(
    os.getenv('SCYLLA_USER'), os.getenv('SCYLLA_PASSWORD')
)
T = TypeVar('T', dict[str, Any], list[Any])
NotFound = query.DoesNotExist


def connect():
    connection.setup(
        get_hosts('SCYLLA_HOSTS'),
        'derailed',
        auth_provider=auth_provider,
        connect_timeout=100,
        retry_connect=True,
    )


class Event(msgspec.Struct):
    name: str
    data: dict
    guild_id: int | None = None
    guild_ids: list[int] | None = None
    user_id: int | None = None
    user_ids: list[int] | None = None


class WSMessage(msgspec.Struct):
    op: int
    d: dict[str, Any]


def encode(
    data: dict[str, Any],
    compressor: Any | None,
    encoding: Literal['json', 'msgpack'],
) -> bytes:
    if encoding == 'json':
        datafied = msgspec.json.encode(data)
    elif encoding == 'msgpack':
        datafied = msgspec.msgpack.encode(data)

    if compressor is None:
        return datafied.decode()

    first_dataset = compressor.compress(datafied)
    second_dataset = compressor.flush(Z_SYNC_FLUSH)
    return first_dataset + second_dataset


def decode(data: bytes) -> Event:
    return msgspec.msgpack.decode(data, type=Event)


class Guild(models.Model):
    __table_name__ = 'guilds'
    id: int = columns.BigInt(primary_key=True)
    name: str = columns.Text()
    icon: str = columns.Text()
    splash: str = columns.Text()
    discovery_splash: str = columns.Text()
    owner_id: int = columns.BigInt()
    default_permissions: int = columns.BigInt()
    afk_channel_id: int = columns.BigInt()
    afk_timeout: int = columns.Integer()
    default_message_notification_level: int = columns.Integer()
    explicit_content_filter: int = columns.Integer()
    mfa_level: int = columns.Integer()
    system_channel_id: int = columns.BigInt()
    system_channel_flags: int = columns.Integer()
    rules_channel_id: int = columns.BigInt()
    max_presences: int = columns.Integer()
    max_members: int = columns.Integer()
    vanity_url_code: str = columns.Text()
    description: str = columns.Text()
    banner: str = columns.Text()
    preferred_locale: str = columns.Text()
    guild_updates_channel_id: int = columns.BigInt()
    nsfw_level: int = columns.Integer()
    verification_level: int = columns.Integer()


class Feature(models.Model):
    __table_name__ = 'features'
    guild_id: int = columns.BigInt(primary_key=True)
    value: str = columns.Text()


class Settings(models.Model):
    __table_name__ = 'settings'
    user_id: int = columns.BigInt(primary_key=True)
    locale: str = columns.Text()
    developer_mode: bool = columns.Boolean(default=False)
    theme: str = columns.Text()
    status: str = columns.Text()
    mfa_enabled: bool = columns.Boolean(default=False)
    mfa_code: str = columns.Text()
    friend_requests_off: bool = columns.Boolean(default=False)


class Channel(models.Model):
    __table_name__ = 'channels'
    id: int = columns.BigInt(primary_key=True)
    type: int = columns.Integer()
    name: str = columns.Text()


class Recipient(models.Model):
    __table_name__ = 'recipients'
    channel_id: int = columns.BigInt(primary_key=True)
    user_id: int = columns.BigInt(index=True)


class DMChannel(models.Model):
    __table_name__ = 'dm_channels'
    channel_id: int = columns.BigInt(primary_key=True)
    last_message_id: int = columns.BigInt()


class GroupDMChannel(DMChannel):
    __table_name__ = 'group_dm_channels'

    channel_id: int = columns.BigInt(primary_key=True)
    icon: str = columns.Text()
    owner_id: int = columns.BigInt()


class GuildChannel(models.Model):
    channel_id: int = columns.BigInt(primary_key=True)
    guild_id: int = columns.BigInt(index=True)
    position: int = columns.Integer()
    parent_id: int = columns.BigInt()
    nsfw: bool = columns.Boolean(default=False)


class CategoryChannel(GuildChannel):
    __table_name__ = 'category_channels'


class TextChannel(GuildChannel):
    __table_name__ = 'guild_text_channels'

    rate_limit_per_user: int = columns.Integer(default=0)
    topic: str = columns.Text()
    last_message_id: int = columns.BigInt()


class Relationship(models.Model):
    __table_name__ = 'relationships'
    # the user id who created the relationship
    user_id: int = columns.BigInt(primary_key=True)
    # the user id who friended/blocked/etc the relationship
    target_id: int = columns.BigInt(index=True)
    # the type of relationship
    type: int = columns.Integer()


class User(models.Model):
    __table_name__ = 'users'
    id: int = columns.BigInt(primary_key=True)
    email: str = columns.Text(index=True)
    password: str = columns.Text()
    username: str = columns.Text(index=True)
    discriminator: str = columns.Text()
    avatar: str = columns.Text()
    banner: str = columns.Text()
    flags: int = columns.Integer()
    bot: bool = columns.Boolean()
    verified: bool | None = columns.Boolean()


class MentionedUser(models.Model):
    __table_name__ = 'mentioned_users'
    message_id: int = columns.BigInt(primary_key=True)
    user_id: int = columns.BigInt(index=True)


class MentionedRole(models.Model):
    __table_name__ = 'mentioned_roles'
    message_id: int = columns.BigInt(primary_key=True)
    role_id: int = columns.BigInt(primary_key=True)


class ReadState(models.Model):
    __table_name__ = 'channel_readstates'
    user_id: int = columns.BigInt(primary_key=True)
    channel_id: int = columns.BigInt()
    last_read_message_id: int = columns.BigInt()
    mention_count: int = columns.Integer()


class GatewaySessionLimit(models.Model):
    __table_name__ = 'gateway_session_limit'
    __options__ = {'default_time_to_live': 43200}
    user_id: int = columns.BigInt(primary_key=True)
    total: int = columns.Integer(default=1000)
    remaining: int = columns.Integer(default=1000)
    max_concurrency: int = columns.Integer(default=16)


class Member(models.Model):
    __table_name__ = 'members'
    guild_id: int = columns.BigInt(primary_key=True)
    user_id: int = columns.BigInt(index=True)
    nick: str = columns.Text()
    avatar: str = columns.Text()
    joined_at: str = columns.DateTime()
    deaf: bool = columns.Boolean()
    mute: bool = columns.Boolean()
    pending: bool = columns.Boolean()
    communication_disabled_until: str = columns.DateTime()
    owner: bool = columns.Boolean()


class Role(models.Model):
    __table_name__ = 'roles'
    id: int = columns.BigInt(primary_key=False)
    guild_id: int = columns.BigInt(primary_key=True)
    name: str = columns.Text()
    color: int = columns.Integer()
    viewable: bool = columns.Boolean()
    icon: str = columns.Text()
    unicode_emoji: str = columns.Text()
    position: int = columns.Integer()
    permissions: int = columns.BigInt()
    mentionable: bool = columns.Boolean()


class Presence(models.Model):
    __table_name__ = 'presences'
    user_id: int = columns.BigInt(primary_key=True)
    status: str = columns.Text()
    client_status: str = columns.Text()


class Activity(models.Model):
    __table_name__ = 'activities'
    user_id: int = columns.BigInt(primary_key=True)
    type: int = columns.Integer()
    created_at: str = columns.DateTime()
    content: str = columns.Text()
    stream_url: str = columns.Text()
    emoji_id: int = columns.BigInt()


def objectify(data: T, remove_guild_id_field: bool = False) -> T:
    if isinstance(data, dict):
        for k, v in data.items():
            if (
                isinstance(v, int)
                and v > 2147483647
                or isinstance(v, int)
                and 'permissions' in k
            ):
                data[k] = str(v)
            elif isinstance(v, list):
                new_value = []

                for item in v:
                    if isinstance(item, (dict, list)):
                        new_value.append(objectify(item))
                    else:
                        new_value.append(item)

                data[k] = new_value
            elif k == 'guild_id' and remove_guild_id_field:
                data.pop(k)
            elif k == 'channel_id' and remove_guild_id_field:
                data.pop(k)

    elif isinstance(data, list):
        new_data = []

        for item in data:
            if isinstance(item, (dict, list)):
                new_data.append(objectify(item))
            else:
                new_data.append(item)

        data = new_data

    return data
