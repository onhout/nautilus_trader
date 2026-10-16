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
from decimal import Decimal
from zoneinfo import ZoneInfo

import pandas as pd

from nautilus_trader.adapters.alpaca.common.constants import ALPACA_VENUE
from nautilus_trader.adapters.alpaca.common.enums import AlpacaDataFeed
from nautilus_trader.adapters.alpaca.common.schemas.market import AlpacaAsset
from nautilus_trader.adapters.alpaca.common.schemas.market import AlpacaOptionContract
from nautilus_trader.adapters.alpaca.common.symbol import alpaca_symbol_to_instrument_id
from nautilus_trader.adapters.alpaca.common.symbol import option_tick_table
from nautilus_trader.adapters.alpaca.common.symbol import parse_occ_symbol
from nautilus_trader.adapters.alpaca.http.client import AlpacaHttpClient
from nautilus_trader.adapters.alpaca.http.market import AlpacaMarketHttpAPI
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.enums import OptionKind
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.instruments import OptionContract
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


NEW_YORK = ZoneInfo("America/New_York")


class AlpacaInstrumentProvider(InstrumentProvider):
    """
    Provides Alpaca instruments on venue ALPACA.

    - US equities as whole-share ``Equity`` (``SPY.ALPACA``);
    - equity options as ``OptionContract`` with the OCC symbol (``MU261030C00250000.ALPACA``),
      ``info`` carrying Alpaca's contract id, the penny-pilot flag and its tick table (``min_ticks``).

    Parameters
    ----------
    client : AlpacaHttpClient
        The Alpaca HTTP client.
    clock : LiveClock
        The clock instance.
    config : InstrumentProviderConfig, optional
        The configuration for the provider.

    """

    def __init__(
        self,
        client: AlpacaHttpClient,
        clock: LiveClock,
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self._clock = clock
        self._http_market = AlpacaMarketHttpAPI(client)

    async def load_all_async(self, filters: dict | None = None) -> None:
        self._log.warning("Alpaca instruments load by id: configure `load_ids` instead of `load_all`")

    async def load_ids_async(self, instrument_ids: list[InstrumentId], filters: dict | None = None) -> None:
        for other in (i for i in instrument_ids if i.venue != ALPACA_VENUE):
            self._log.warning(f"Skipping {other}: not an {ALPACA_VENUE} instrument")
        ids = [i for i in instrument_ids if i.venue == ALPACA_VENUE]
        await asyncio.gather(*(self._load(i) for i in ids))

    async def load_async(self, instrument_id: InstrumentId, filters: dict | None = None) -> None:
        await self.load_ids_async([instrument_id], filters)

    async def _load(self, instrument_id: InstrumentId) -> None:
        symbol = instrument_id.symbol.value
        if parse_occ_symbol(symbol) is not None:
            contract = await self._http_market.query_option_contract(symbol)
            if contract is None:
                self._log.error(f"Option {symbol} not found on Alpaca")
                return
            self.add(parse_option_contract(contract, self._clock.timestamp_ns()))
            return
        asset = await self._http_market.query_asset(symbol)
        if asset is None:
            self._log.error(f"Symbol {symbol} not found on Alpaca")
            return
        reference = None
        try:
            snap = (await self._http_market.query_stock_snapshots([symbol], AlpacaDataFeed.IEX)).get(symbol)
            if snap is not None:
                reference = (snap.latestTrade.p if snap.latestTrade else None) or (
                    snap.prevDailyBar.c if snap.prevDailyBar else None
                )
        except Exception as e:
            self._log.warning(f"No reference price for {symbol} (tick size defaults to 0.01): {e}")
        if not asset.tradable or asset.status != "active":
            self._log.warning(f"{symbol} is not tradable on Alpaca (status={asset.status})")
        self.add(parse_equity(asset, reference, self._clock.timestamp_ns()))


def parse_equity(asset: AlpacaAsset, reference_price: float | None, ts_init: int) -> Equity:
    # Alpaca accepts 2 decimals at or above $1 and 4 below
    precision = 4 if reference_price is not None and 0 < reference_price < 1 else 2
    return Equity(
        instrument_id=alpaca_symbol_to_instrument_id(asset.symbol),
        raw_symbol=Symbol(asset.symbol),
        currency=USD,
        price_precision=precision,
        price_increment=Price.from_str(str(Decimal(1).scaleb(-precision))),
        lot_size=Quantity.from_int(1),
        ts_event=ts_init,
        ts_init=ts_init,
        maker_fee=Decimal(0),
        taker_fee=Decimal(0),
        info={
            "alpaca_id": asset.id,
            "name": asset.name,
            "exchange": asset.exchange,
            "status": asset.status,
            "tradable": asset.tradable,
            "shortable": asset.shortable,
            "fractionable": asset.fractionable,
            "options_enabled": asset.options_enabled,
        },
    )


def parse_option_contract(contract: AlpacaOptionContract, ts_init: int) -> OptionContract:
    root = contract.root_symbol or contract.underlying_symbol
    expiry = pd.Timestamp(
        dt.datetime.combine(dt.date.fromisoformat(contract.expiration_date), dt.time(16)),
        tz=NEW_YORK,
    )
    return OptionContract(
        instrument_id=InstrumentId(Symbol(contract.symbol), ALPACA_VENUE),
        raw_symbol=Symbol(contract.symbol),
        asset_class=AssetClass.EQUITY,
        currency=USD,
        price_precision=2,
        price_increment=Price.from_str("0.01"),
        multiplier=Quantity.from_int(int(Decimal(contract.multiplier or contract.size or "100"))),
        lot_size=Quantity.from_int(1),
        underlying=contract.underlying_symbol,
        option_kind=OptionKind.CALL if contract.type == "call" else OptionKind.PUT,
        strike_price=Price.from_str(f"{Decimal(contract.strike_price):.2f}"),
        activation_ns=0,
        expiration_ns=expiry.value,
        ts_event=ts_init,
        ts_init=ts_init,
        maker_fee=Decimal(0),
        taker_fee=Decimal(0),
        info={
            "alpaca_id": contract.id,
            "status": contract.status,
            "tradable": contract.tradable,
            "style": contract.style,
            "expiration_date": contract.expiration_date,
            "penny_pilot": contract.ppind,
            "min_ticks": option_tick_table(root, contract.ppind),
            "open_interest": contract.open_interest,
        },
    )
