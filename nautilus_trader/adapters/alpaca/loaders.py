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

import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd

from nautilus_trader.adapters.alpaca.common.enums import AlpacaDataFeed
from nautilus_trader.adapters.alpaca.common.schemas.market import AlpacaBar
from nautilus_trader.adapters.alpaca.common.symbol import alpaca_symbol_to_instrument_id
from nautilus_trader.adapters.alpaca.http.client import AlpacaHttpClient
from nautilus_trader.adapters.alpaca.http.market import AlpacaMarketHttpAPI
from nautilus_trader.adapters.alpaca.providers import parse_equity
from nautilus_trader.adapters.alpaca.providers import parse_option_contract
from nautilus_trader.common.component import LiveClock
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.instruments import OptionContract
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


NEW_YORK = ZoneInfo("America/New_York")

_UNITS = {"Min": ("MINUTE", 60), "Hour": ("HOUR", 3600), "Day": ("DAY", 86400)}


def alpaca_bar_type(instrument: Instrument, step: int, unit: str) -> BarType:
    """
    Return the EXTERNAL LAST bar type for an Alpaca timeframe (``unit`` is 'Min', 'Hour' or 'Day').
    """
    return BarType.from_str(f"{instrument.id}-{step}-{_UNITS[unit][0]}-LAST-EXTERNAL")


def parse_alpaca_bars(
    instrument: Instrument,
    bars: list[AlpacaBar],
    step: int,
    unit: str,
    ts_init_ns: int | None = None,
) -> list[Bar]:
    """
    Convert Alpaca bars (stamped at their open) into Nautilus bars stamped at their close.

    Daily bars (Alpaca stamps them at midnight New York) close at 16:00 New York; intraday bars close
    ``step`` units after their open. Both ``ts_event`` and ``ts_init`` are the close, as Nautilus's
    bar execution and bar-to-quote wrangling expect.
    """
    bar_type = alpaca_bar_type(instrument, step, unit)
    out: list[Bar] = []
    for b in bars:
        opened = pd.Timestamp(b.t)
        if unit == "Day":
            session = opened.tz_convert(NEW_YORK).date()
            closed = pd.Timestamp(dt.datetime.combine(session, dt.time(16), NEW_YORK))
        else:
            closed = opened + pd.Timedelta(seconds=_UNITS[unit][1] * step)
        ts = closed.value
        high = max(b.h, b.o, b.c, b.l)
        low = min(b.l, b.o, b.c, b.h)
        out.append(
            Bar(
                bar_type=bar_type,
                open=Price(b.o, instrument.price_precision),
                high=Price(high, instrument.price_precision),
                low=Price(low, instrument.price_precision),
                close=Price(b.c, instrument.price_precision),
                volume=Quantity(b.v, instrument.size_precision),
                ts_event=ts,
                ts_init=ts_init_ns if ts_init_ns is not None else ts,
            ),
        )
    return out


class AlpacaDataLoader:
    """
    Loads Alpaca historical instruments and bars as Nautilus objects (for catalogs and backtests).

    Stock bars come from the SIP feed (the free plan serves them up to 15 minutes ago); option
    bars are available since February 2024. Expired option contracts are included when asked for.

    Parameters
    ----------
    client : AlpacaHttpClient
        The Alpaca HTTP client.
    clock : LiveClock
        The clock used for instrument init timestamps.

    """

    def __init__(self, client: AlpacaHttpClient, clock: LiveClock) -> None:
        self._http_market = AlpacaMarketHttpAPI(client)
        self._clock = clock

    async def load_equity(self, symbol: str) -> Equity | None:
        asset = await self._http_market.query_asset(symbol)
        if asset is None:
            return None
        reference = None
        snap = (await self._http_market.query_stock_snapshots([symbol], AlpacaDataFeed.IEX)).get(symbol)
        if snap is not None and snap.prevDailyBar is not None:
            reference = snap.prevDailyBar.c
        return parse_equity(asset, reference, self._clock.timestamp_ns())

    async def load_option_contracts(
        self,
        underlying: str,
        kind: str | None = None,
        expiration_gte: str | None = None,
        expiration_lte: str | None = None,
        strike_gte: float | None = None,
        strike_lte: float | None = None,
        include_expired: bool = True,
    ) -> list[OptionContract]:
        """
        Return the option contracts of an underlying in a window (expired ones too by default).
        """
        statuses = ["active", "inactive"] if include_expired else ["active"]
        found: dict[str, OptionContract] = {}
        now = self._clock.timestamp_ns()
        for status in statuses:
            contracts = await self._http_market.query_option_contracts(
                underlying,
                kind=kind,
                expiration_gte=expiration_gte,
                expiration_lte=expiration_lte,
                strike_gte=strike_gte,
                strike_lte=strike_lte,
                status=status,
            )
            for c in contracts:
                if c.underlying_symbol == underlying and c.symbol not in found:
                    found[c.symbol] = parse_option_contract(c, now)
        return list(found.values())

    async def load_stock_bars(
        self,
        instruments: list[Instrument],
        step: int,
        unit: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        feed: AlpacaDataFeed = AlpacaDataFeed.SIP,
    ) -> dict[str, list[Bar]]:
        """
        Return stock bars per symbol (split-adjusted), stamped at their close.
        """
        by_symbol = {i.id.symbol.value: i for i in instruments}
        raw = await self._http_market.query_stock_bars(
            list(by_symbol), f"{step}{unit}", start.isoformat(), end.isoformat(), feed,
        )
        return {s: parse_alpaca_bars(by_symbol[s], bars, step, unit) for s, bars in raw.items() if s in by_symbol}

    async def load_option_bars(
        self,
        instruments: list[Instrument],
        step: int,
        unit: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> dict[str, list[Bar]]:
        """
        Return option bars per OCC symbol, stamped at their close.
        """
        by_symbol = {i.id.symbol.value: i for i in instruments}
        raw = await self._http_market.query_option_bars(list(by_symbol), f"{step}{unit}", start.isoformat(), end.isoformat())
        return {s: parse_alpaca_bars(by_symbol[s], bars, step, unit) for s, bars in raw.items() if s in by_symbol}


__all__ = ["AlpacaDataLoader", "alpaca_bar_type", "alpaca_symbol_to_instrument_id", "parse_alpaca_bars"]
