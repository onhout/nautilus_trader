# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import asyncio
from collections.abc import Callable
from typing import Any

import msgspec

from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.nautilus_pyo3 import WebSocketClient
from nautilus_trader.core.nautilus_pyo3 import WebSocketClientError
from nautilus_trader.core.nautilus_pyo3 import WebSocketConfig
from nautilus_trader.live.cancellation import cancel_tasks_with_timeout


class AlpacaWebSocketClient:
    """
    Provides an Alpaca streaming WebSocket client.

    Two kinds of stream share the protocol (authenticate first, then listen/subscribe):

    - the account's ``trade_updates`` stream (``listen`` action, JSON);
    - market data streams (``subscribe`` action): stocks in JSON, options in MsgPack
      (``msgpack=True``).

    Authentication and subscriptions are re-sent after every reconnection.

    Parameters
    ----------
    clock : LiveClock
        The clock for the client.
    base_url : str
        The WebSocket URL.
    api_key : str
        The Alpaca API key id.
    api_secret : str
        The Alpaca API secret key.
    handler : Callable[[Any], None]
        Called with each decoded data message (dict for trading, dict per item for market data).
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    msgpack : bool, default False
        If the stream uses MsgPack frames (options market data).
    proxy_url : str, optional
        The proxy URL for the WebSocket connection.

    References
    ----------
    https://docs.alpaca.markets/docs/websocket-streaming
    https://docs.alpaca.markets/docs/real-time-option-data

    """

    def __init__(
        self,
        clock: LiveClock,
        base_url: str,
        api_key: str,
        api_secret: str,
        handler: Callable[[Any], None],
        loop: asyncio.AbstractEventLoop,
        msgpack: bool = False,
        proxy_url: str | None = None,
    ) -> None:
        self._clock = clock
        self._log = Logger(type(self).__name__)
        self._base_url = base_url
        self._key = api_key
        self._secret = api_secret
        self._handler = handler
        self._loop = loop
        self._msgpack = msgpack
        self._proxy_url = proxy_url
        self._client: WebSocketClient | None = None
        self._authenticated = asyncio.Event()
        self._listen_streams: list[str] = []
        self._quote_symbols: set[str] = set()
        self._tasks: set[asyncio.Task] = set()

    @property
    def url(self) -> str:
        return self._base_url

    @property
    def is_connected(self) -> bool:
        return self._client is not None and self._client.is_active()

    async def connect(self) -> None:
        headers = [("Content-Type", "application/msgpack")] if self._msgpack else []
        config = WebSocketConfig(url=self._base_url, headers=headers, heartbeat=30, proxy_url=self._proxy_url)
        self._authenticated.clear()
        self._client = await WebSocketClient.connect(
            loop_=self._loop,
            config=config,
            handler=self._handle_raw,
            post_reconnection=self._handle_reconnect,
        )
        self._log.info(f"Connected to {self._base_url}", LogColor.BLUE)
        await self._authenticate()
        try:
            await asyncio.wait_for(self._authenticated.wait(), timeout=15)
        except TimeoutError:
            self._log.error(f"Authentication to {self._base_url} timed out")

    async def disconnect(self) -> None:
        await cancel_tasks_with_timeout(self._tasks, self._log)
        if self._client is None or self._client.is_disconnecting() or self._client.is_closed():
            return
        try:
            await self._client.disconnect()
        except WebSocketClientError as e:
            self._log.warning(str(e))
        self._client = None
        self._log.info(f"Disconnected from {self._base_url}", LogColor.BLUE)

    # -- SUBSCRIPTIONS ----------------------------------------------------------------------------

    async def listen(self, streams: list[str]) -> None:
        """
        Listen to account streams (e.g. ``trade_updates``).
        """
        self._listen_streams = streams
        if self._authenticated.is_set():
            await self._send({"action": "listen", "data": {"streams": streams}})

    async def subscribe_quotes(self, symbols: list[str]) -> None:
        new = [s for s in symbols if s not in self._quote_symbols]
        self._quote_symbols.update(new)
        if new and self._authenticated.is_set():
            await self._send({"action": "subscribe", "quotes": new})

    async def unsubscribe_quotes(self, symbols: list[str]) -> None:
        gone = [s for s in symbols if s in self._quote_symbols]
        self._quote_symbols.difference_update(gone)
        if gone and self._authenticated.is_set():
            await self._send({"action": "unsubscribe", "quotes": gone})

    # -- INTERNALS --------------------------------------------------------------------------------

    async def _authenticate(self) -> None:
        await self._send({"action": "auth", "key": self._key, "secret": self._secret}, secret=True)

    async def _on_authenticated(self) -> None:
        self._authenticated.set()
        if self._listen_streams:
            await self._send({"action": "listen", "data": {"streams": self._listen_streams}})
        if self._quote_symbols:
            await self._send({"action": "subscribe", "quotes": sorted(self._quote_symbols)})

    def _handle_reconnect(self) -> None:
        self._log.warning(f"Reconnected to {self._base_url}: re-authenticating")
        self._authenticated.clear()
        self._spawn(self._authenticate())

    def _spawn(self, coro) -> None:
        task = self._loop.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _decode(self, raw: bytes) -> Any:
        if self._msgpack:
            return msgspec.msgpack.decode(raw)
        return msgspec.json.decode(raw)

    def _handle_raw(self, raw: bytes) -> None:
        try:
            msg = self._decode(raw)
        except Exception as e:
            self._log.error(f"Undecodable message from {self._base_url}: {e}")
            return
        items = msg if isinstance(msg, list) else [msg]
        for item in items:
            if not isinstance(item, dict):
                continue
            if self._is_control(item):
                continue
            try:
                self._handler(item)
            except Exception as e:
                self._log.exception(f"Error handling {item!r}", e)

    def _is_control(self, item: dict) -> bool:
        # Trading stream: {"stream": "authorization"|"listening", "data": {...}}
        stream = item.get("stream")
        if stream == "authorization":
            data = item.get("data") or {}
            if data.get("status") == "authorized":
                self._spawn(self._on_authenticated())
            else:
                self._log.error(f"Alpaca stream authorization failed: {data}")
            return True
        if stream == "listening":
            self._log.info(f"Listening to {(item.get('data') or {}).get('streams')}", LogColor.BLUE)
            return True
        # Market data streams: {"T": "success"|"error"|"subscription", ...}
        kind = item.get("T")
        if kind == "success":
            if item.get("msg") == "authenticated":
                self._spawn(self._on_authenticated())
            return True
        if kind == "error":
            self._log.error(f"Alpaca stream error {item.get('code')}: {item.get('msg')}")
            return True
        if kind == "subscription":
            self._log.debug(f"Subscriptions: {item}")
            return True
        return False

    async def _send(self, msg: dict[str, Any], secret: bool = False) -> None:
        if self._client is None:
            self._log.warning(f"Cannot send {msg.get('action')}: not connected")
            return
        if not secret:
            self._log.debug(f"SENDING: {msg}")
        try:
            if self._msgpack:
                await self._client.send(msgspec.msgpack.encode(msg))
            else:
                await self._client.send_text(msgspec.json.encode(msg))
        except WebSocketClientError as e:
            self._log.warning(str(e))
