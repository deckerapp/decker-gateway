"""
Elastic License 2.0

Copyright Elasic and/or licensed to Elasic under one
or more contributor license agreements. Licensed under the Elastic License;
you may not use this file except in compliance with the Elastic License.
"""
import functools
from typing import Literal

from pydantic import BaseModel


def flagged(value: int, visible: int) -> bool:
    return bool(value & visible)


class ConnectionProperties(BaseModel):
    os: Literal['linux', 'darwin', 'windows']
    browser: str
    browser_user_agent: str | None
    client_build_number: int | None
    client_version: str | None
    device: str
    distro: str | None
    os_arch: str | None
    os_version: str | None
    referrer: str | None
    referring_domain: str | None
    release_channel: Literal['stable', 'canary', 'devel'] | None
    window_manager: str | None


class Identify(BaseModel):
    token: str
    intents: int
    properties: ConnectionProperties


class Intents:
    def __init__(self, intents: int) -> None:
        self.get = functools.partial(flagged, intents)
