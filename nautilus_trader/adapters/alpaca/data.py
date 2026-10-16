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
import datetime as dt
from typing import Any

import pandas as pd

from nautilus_trader.adapters.alpaca.common.constants import ALPACA_VENUE
from nautilus_trader.adapters.alpaca.common.enums import AlpacaDataFeed
from nautilus_trader.adapters.alpaca.common.schemas.market import AlpacaQuote
from nautilus_trader.adapters.alpaca.common.symbol import is_option_symbol
from nautilus_trader.adapters.alpaca.common.urls import get_ws_data_url
from nautilus_trader.adapters.alpaca.config import AlpacaDataClientConfig
from nautilus_trader.adapters.alpaca.http.client import AlpacaHttpClient
from nautilus_trader.adapters.alpaca.http.market import AlpacaMarketHttpAPI
from nautilus_trader.adapters.alpaca.providers import AlpacaInstrumentProvider
from nautilus_trader.adapters.alpaca.websocket.client import AlpacaWebSocketClient
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.data.messages import RequestBars
from nautilus_trader.data.messages import RequestInstrument
from nautilus_trader.data.messages import RequestInstruments
from nautilus_trader.data.messages import SubscribeInstrument
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


_TIMEFRAMES = {BarAggregation.MINUTE: "Min", BarAggregation.HOUR: "Hour", BarAggregation.DAY: "Day"}


class AlpacaDataClient(LiveMarketDataClient):
    """
    Provides a data client for Alpaca market data.

    Quotes stream over WebSocket: stocks from ``stock_feed`` (IEX on the free plan, JSON) and
    options from ``option_feed`` (indicative on the free plan, MsgPack). Each subscription starts
    with the latest quote from REST. Historical stock bars are served from REST.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    client : AlpacaHttpClient
        The Alpaca HTTP client.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : AlpacaInstrumentProvider
        The instrument provider.
    config : AlpacaDataClientConfig
        The configuration for the client.
    name : str, optional
        The custom client ID.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: AlpacaHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: AlpacaInstrumentProvider,
        config: AlpacaDataClientConfig,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or ALPACA_VENUE.value),
            venue=ALPACA_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=config,
        )
        self._config = config
        self._http_market = AlpacaMarketHttpAPI(client)
        self._ws_stocks = AlpacaWebSocketClient(
            clock=clock,
            base_url=get_ws_data_url(config.stock_feed),
            api_key=client.api_key,
            api_secret=client.api_secret,
            handler=self._handle_ws_item,
            loop=loop,
            proxy_url=config.proxy_url,
        )
        self._ws_options = AlpacaWebSocketClient(
            clock=clock,
            base_url=get_ws_data_url(config.option_feed),
            api_key=client.api_key,
            api_secret=client.api_secret,
            handler=self._handle_ws_item,
            loop=loop,
            msgpack=True,
            proxy_url=config.proxy_url,
        )
        self._quote_subs: set[InstrumentId] = set()

    @property
    def instrument_provider(self) -> AlpacaInstrumentProvider:
        return self._instrument_provider

    # -- CONNECTION -------------------------------------------------------------------------------

    async def _connect(self) -> None:
        await self._instrument_provider.initialize()
        for instrument in self._instrument_provider.list_all():
            self._handle_data(instrument)
        self._log.info(f"Market data: stocks {self._config.stock_feed.value}, options {self._config.option_feed.value}")

    async def _disconnect(self) -> None:
        await self._ws_stocks.disconnect()
        await self._ws_options.disconnect()

    async def _ensure_instrument(self, instrument_id: InstrumentId) -> Instrument | None:
        instrument = self._cache.instrument(instrument_id)
        if instrument is not None:
            return instrument
        await self._instrument_provider.load_async(instrument_id)
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.error(f"Instrument {instrument_id} not found")
            return None
        self._handle_data(instrument)
        return instrument

    # -- SUBSCRIPTIONS ----------------------------------------------------------------------------

    async def _subscribe_instrument(self, command: SubscribeInstrument) -> None:
        await self._ensure_instrument(command.instrument_id)

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        instrument = await self._ensure_instrument(command.instrument_id)
        if instrument is None:
            return
        self._quote_subs.add(command.instrument_id)
        symbol = command.instrument_id.symbol.value
        option = is_option_symbol(symbol)
        ws = self._ws_options if option else self._ws_stocks
        if not ws.is_connected:
            await ws.connect()
        await ws.subscribe_quotes([symbol])
        await self._emit_latest_quote(instrument, option)

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        self._quote_subs.discard(command.instrument_id)
        symbol = command.instrument_id.symbol.value
        ws = self._ws_options if is_option_symbol(symbol) else self._ws_stocks
        await ws.unsubscribe_quotes([symbol])

    async def _emit_latest_quote(self, instrument: Instrument, option: bool) -> None:
        symbol = instrument.id.symbol.value
        try:
            if option:
                quote = (await self._http_market.query_option_latest_quotes([symbol], self._config.option_feed)).get(symbol)
            else:
                snap = (await self._http_market.query_stock_snapshots([symbol], self._config.stock_feed)).get(symbol)
                quote = snap.latestQuote if snap else None
        except Exception as e:
            self._log.warning(f"Latest quote for {symbol} failed: {e}")
            return
        if quote is not None:
            tick = self._parse_quote(instrument, quote.bp, quote.bs, quote.ap, quote.as_, quote.t)
            if tick is not None:
                self._handle_data(tick)

    # -- STREAM -----------------------------------------------------------------------------------

    def _handle_ws_item(self, item: dict[str, Any]) -> None:
        if item.get("T") != "q":
            return
        instrument_id = InstrumentId.from_str(f"{item['S']}.{ALPACA_VENUE}")
        if instrument_id not in self._quote_subs:
            return
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            return
        tick = self._parse_quote(instrument, item.get("bp"), item.get("bs"), item.get("ap"), item.get("as"), item.get("t"))
        if tick is not None:
            self._handle_data(tick)

    def _parse_quote(self, instrument: Instrument, bid, bid_size, ask, ask_size, ts) -> QuoteTick | None:
        if bid is None or ask is None or ask <= 0 or bid < 0 or bid > ask:
            return None
        ts_init = self._clock.timestamp_ns()
        if isinstance(ts, dt.datetime):
            ts_event = int(ts.timestamp() * 1e9)
        else:
            ts_event = pd.Timestamp(ts).value if ts else ts_init
        return QuoteTick(
            instrument_id=instrument.id,
            bid_price=Price(float(bid), instrument.price_precision),
            ask_price=Price(float(ask), instrument.price_precision),
            bid_size=Quantity.from_int(max(1, int(bid_size or 0))),
            ask_size=Quantity.from_int(max(1, int(ask_size or 0))),
            ts_event=ts_event,
            ts_init=ts_init,
        )

    # -- REQUESTS ---------------------------------------------------------------------------------

    async def _request_instrument(self, request: RequestInstrument) -> None:
        instrument = await self._ensure_instrument(request.instrument_id)
        if instrument is not None:
            self._handle_instrument(instrument, request.id, request.start, request.end, request.params)

    async def _request_instruments(self, request: RequestInstruments) -> None:
        self._handle_instruments(
            self.venue,
            self._instrument_provider.list_all(),
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_bars(self, request: RequestBars) -> None:
        bar_type = request.bar_type
        unit = _TIMEFRAMES.get(bar_type.spec.aggregation)
        if unit is None or bar_type.spec.price_type != PriceType.LAST or not bar_type.is_externally_aggregated():
            self._log.error(f"Cannot request {bar_type}: only EXTERNAL LAST minute/hour/day bars")
            return
        instrument = await self._ensure_instrument(bar_type.instrument_id)
        if instrument is None:
            return
        now = pd.Timestamp.now(tz="UTC")
        end = min(request.end or now, now - pd.Timedelta(minutes=16))  # SIP history ends 15 minutes ago (free plan)
        start = request.start or end - pd.Timedelta(days=365)
        symbol = bar_type.instrument_id.symbol.value
        timeframe = f"{bar_type.spec.step}{unit}"
        if is_option_symbol(symbol):
            raw = (await self._http_market.query_option_bars([symbol], timeframe, start.isoformat(), end.isoformat())).get(symbol, [])
        else:
            raw = (await self._http_market.query_stock_bars(
                [symbol], timeframe, start.isoformat(), end.isoformat(), AlpacaDataFeed.SIP,
            )).get(symbol, [])
        step_ns = bar_type.spec.timedelta.value
        ts_init = self._clock.timestamp_ns()
        bars = [
            Bar(
                bar_type=bar_type,
                open=Price(b.o, instrument.price_precision),
                high=Price(b.h, instrument.price_precision),
                low=Price(b.l, instrument.price_precision),
                close=Price(b.c, instrument.price_precision),
                volume=Quantity(b.v, instrument.size_precision),
                ts_event=pd.Timestamp(b.t).value + step_ns,  # bars are stamped at their close
                ts_init=ts_init,
            )
            for b in raw
        ]
        if request.limit:
            bars = bars[-request.limit :]
        self._handle_bars(bar_type, bars, None, request.id, request.start, request.end, request.params)
