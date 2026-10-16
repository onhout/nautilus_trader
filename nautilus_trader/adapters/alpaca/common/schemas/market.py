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

import msgspec


class AlpacaAsset(msgspec.Struct, frozen=True):
    """
    HTTP response from Alpaca `GET /v2/assets/{symbol}`.
    """

    id: str
    symbol: str
    status: str
    tradable: bool
    name: str | None = None
    exchange: str | None = None
    marginable: bool | None = None
    shortable: bool | None = None
    easy_to_borrow: bool | None = None
    fractionable: bool | None = None
    attributes: list[str] | None = None

    @property
    def options_enabled(self) -> bool:
        return "options_enabled" in (self.attributes or [])


class AlpacaOptionContract(msgspec.Struct, frozen=True):
    """
    HTTP response from Alpaca `GET /v2/options/contracts/{symbol}`.
    """

    id: str
    symbol: str
    status: str
    tradable: bool
    expiration_date: str
    underlying_symbol: str
    type: str
    strike_price: str
    name: str | None = None
    root_symbol: str | None = None
    underlying_asset_id: str | None = None
    style: str | None = None
    multiplier: str | None = None
    size: str | None = None
    open_interest: str | None = None
    open_interest_date: str | None = None
    close_price: str | None = None
    close_price_date: str | None = None
    ppind: bool | None = None


class AlpacaOptionContracts(msgspec.Struct, frozen=True):
    """
    HTTP response from Alpaca `GET /v2/options/contracts`.
    """

    option_contracts: list[AlpacaOptionContract] | None = None
    next_page_token: str | None = None


class AlpacaBar(msgspec.Struct, frozen=True):
    """
    A bar (``t`` is the bar's start time).
    """

    t: str
    o: float
    h: float
    l: float  # noqa: E741
    c: float
    v: float
    n: int | None = None
    vw: float | None = None


class AlpacaBarsResponse(msgspec.Struct, frozen=True):
    """
    HTTP response from Alpaca `GET /v2/stocks/bars` and `GET /v1beta1/options/bars`.
    """

    bars: dict[str, list[AlpacaBar]] | None = None
    next_page_token: str | None = None


class AlpacaQuote(msgspec.Struct, frozen=True):
    t: str
    bp: float = 0.0
    bs: float = 0.0
    ap: float = 0.0
    as_: float = msgspec.field(default=0.0, name="as")
    bx: str | None = None
    ax: str | None = None
    c: list[str] | str | None = None


class AlpacaTrade(msgspec.Struct, frozen=True):
    t: str
    p: float
    s: float = 0.0
    x: str | None = None


class AlpacaGreeks(msgspec.Struct, frozen=True):
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    rho: float | None = None


class AlpacaSnapshot(msgspec.Struct, frozen=True):
    """
    A stock or option snapshot (`GET /v2/stocks/snapshots`, `GET /v1beta1/options/snapshots/{underlying}`).
    """

    latestTrade: AlpacaTrade | None = None
    latestQuote: AlpacaQuote | None = None
    minuteBar: AlpacaBar | None = None
    dailyBar: AlpacaBar | None = None
    prevDailyBar: AlpacaBar | None = None
    greeks: AlpacaGreeks | None = None
    impliedVolatility: float | None = None


class AlpacaOptionSnapshotsResponse(msgspec.Struct, frozen=True):
    snapshots: dict[str, AlpacaSnapshot] | None = None
    next_page_token: str | None = None


class AlpacaLatestQuotesResponse(msgspec.Struct, frozen=True):
    quotes: dict[str, AlpacaQuote] | None = None
