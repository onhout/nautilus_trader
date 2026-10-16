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

from typing import Any

import msgspec

from nautilus_trader.adapters.alpaca.common.enums import AlpacaDataFeed
from nautilus_trader.adapters.alpaca.common.schemas.market import AlpacaAsset
from nautilus_trader.adapters.alpaca.common.schemas.market import AlpacaBar
from nautilus_trader.adapters.alpaca.common.schemas.market import AlpacaBarsResponse
from nautilus_trader.adapters.alpaca.common.schemas.market import AlpacaLatestQuotesResponse
from nautilus_trader.adapters.alpaca.common.schemas.market import AlpacaOptionContract
from nautilus_trader.adapters.alpaca.common.schemas.market import AlpacaOptionContracts
from nautilus_trader.adapters.alpaca.common.schemas.market import AlpacaOptionSnapshotsResponse
from nautilus_trader.adapters.alpaca.common.schemas.market import AlpacaQuote
from nautilus_trader.adapters.alpaca.common.schemas.market import AlpacaSnapshot
from nautilus_trader.adapters.alpaca.http.client import AlpacaHttpClient
from nautilus_trader.adapters.alpaca.http.endpoint import AlpacaHttpEndpoint
from nautilus_trader.adapters.alpaca.http.error import AlpacaClientError
from nautilus_trader.core.nautilus_pyo3 import HttpMethod


class AlpacaAssetsHttp(AlpacaHttpEndpoint):
    """
    `GET /v2/assets/{symbol}`
    """

    def __init__(self, client: AlpacaHttpClient) -> None:
        super().__init__(client, "/v2/assets")
        self._decoder = msgspec.json.Decoder(AlpacaAsset)

    async def get(self, symbol: str) -> AlpacaAsset | None:
        try:
            return self._decoder.decode(await self._method(HttpMethod.GET, url_path=f"{self.url_path}/{symbol}"))
        except AlpacaClientError as e:
            if e.status in (404, 422):
                return None
            raise


class AlpacaOptionContractsHttp(AlpacaHttpEndpoint):
    """
    `GET /v2/options/contracts`
    `GET /v2/options/contracts/{symbol_or_id}`
    """

    def __init__(self, client: AlpacaHttpClient) -> None:
        super().__init__(client, "/v2/options/contracts")
        self._decoder = msgspec.json.Decoder(AlpacaOptionContract)
        self._list_decoder = msgspec.json.Decoder(AlpacaOptionContracts)

    async def get(self, symbol: str) -> AlpacaOptionContract | None:
        try:
            return self._decoder.decode(await self._method(HttpMethod.GET, url_path=f"{self.url_path}/{symbol}"))
        except AlpacaClientError as e:
            if e.status in (404, 422):
                return None
            raise

    async def list(self, params: dict[str, Any]) -> list[AlpacaOptionContract]:
        out: list[AlpacaOptionContract] = []
        token: str | None = None
        for _ in range(50):
            page = self._list_decoder.decode(
                await self._method(HttpMethod.GET, params={**params, "limit": 10000, "page_token": token}),
            )
            out.extend(page.option_contracts or [])
            token = page.next_page_token
            if not token:
                break
        return out


class AlpacaBarsHttp(AlpacaHttpEndpoint):
    """
    `GET /v2/stocks/bars` and `GET /v1beta1/options/bars` (Market Data API).
    """

    def __init__(self, client: AlpacaHttpClient, url_path: str) -> None:
        super().__init__(client, url_path, data_api=True)
        self._decoder = msgspec.json.Decoder(AlpacaBarsResponse)

    async def get(self, params: dict[str, Any]) -> dict[str, list[AlpacaBar]]:
        out: dict[str, list[AlpacaBar]] = {}
        token: str | None = None
        for _ in range(500):
            page = self._decoder.decode(
                await self._method(HttpMethod.GET, params={**params, "limit": 10000, "sort": "asc", "page_token": token}),
            )
            for symbol, bars in (page.bars or {}).items():
                out.setdefault(symbol, []).extend(bars)
            token = page.next_page_token
            if not token:
                break
        return out


class AlpacaMarketHttpAPI:
    """
    Provides access to Alpaca assets, option contracts and market data.

    References
    ----------
    https://docs.alpaca.markets/reference/stockbars
    https://docs.alpaca.markets/reference/optionchain

    """

    def __init__(self, client: AlpacaHttpClient) -> None:
        self.client = client
        self._endpoint_assets = AlpacaAssetsHttp(client)
        self._endpoint_contracts = AlpacaOptionContractsHttp(client)
        self._endpoint_stock_bars = AlpacaBarsHttp(client, "/v2/stocks/bars")
        self._endpoint_option_bars = AlpacaBarsHttp(client, "/v1beta1/options/bars")
        self._snapshots_decoder = msgspec.json.Decoder(dict[str, AlpacaSnapshot])
        self._option_snapshots_decoder = msgspec.json.Decoder(AlpacaOptionSnapshotsResponse)
        self._latest_quotes_decoder = msgspec.json.Decoder(AlpacaLatestQuotesResponse)

    async def query_asset(self, symbol: str) -> AlpacaAsset | None:
        return await self._endpoint_assets.get(symbol)

    async def query_option_contract(self, symbol: str) -> AlpacaOptionContract | None:
        return await self._endpoint_contracts.get(symbol)

    async def query_option_contracts(
        self,
        underlying: str,
        kind: str | None = None,
        expiration_date: str | None = None,
        expiration_gte: str | None = None,
        expiration_lte: str | None = None,
        strike_gte: float | None = None,
        strike_lte: float | None = None,
        status: str | None = "active",
    ) -> list[AlpacaOptionContract]:
        """
        Return the contracts of one underlying (``status='inactive'`` for expired contracts).
        """
        return await self._endpoint_contracts.list({
            "underlying_symbols": underlying,
            "type": kind,
            "expiration_date": expiration_date,
            "expiration_date_gte": expiration_gte,
            "expiration_date_lte": expiration_lte,
            "strike_price_gte": strike_gte,
            "strike_price_lte": strike_lte,
            "status": status,
        })

    async def query_stock_bars(
        self,
        symbols: list[str],
        timeframe: str,
        start: str,
        end: str | None = None,
        feed: AlpacaDataFeed = AlpacaDataFeed.SIP,
        adjustment: str = "split",
    ) -> dict[str, list[AlpacaBar]]:
        """
        Return bars per symbol, oldest first. On the free plan SIP data must end 15 minutes ago.
        """
        out: dict[str, list[AlpacaBar]] = {}
        for i in range(0, len(symbols), 50):
            out.update(await self._endpoint_stock_bars.get({
                "symbols": ",".join(symbols[i : i + 50]),
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "feed": feed.value,
                "adjustment": adjustment,
            }))
        return out

    async def query_option_bars(
        self,
        symbols: list[str],
        timeframe: str,
        start: str,
        end: str | None = None,
    ) -> dict[str, list[AlpacaBar]]:
        """
        Return historical option bars (available since February 2024).
        """
        out: dict[str, list[AlpacaBar]] = {}
        for i in range(0, len(symbols), 100):
            out.update(await self._endpoint_option_bars.get({
                "symbols": ",".join(symbols[i : i + 100]), "timeframe": timeframe, "start": start, "end": end,
            }))
        return out

    async def query_stock_snapshots(
        self,
        symbols: list[str],
        feed: AlpacaDataFeed = AlpacaDataFeed.IEX,
    ) -> dict[str, AlpacaSnapshot]:
        out: dict[str, AlpacaSnapshot] = {}
        for i in range(0, len(symbols), 100):
            raw = await self.client.send_request(
                HttpMethod.GET,
                "/v2/stocks/snapshots",
                params={"symbols": ",".join(symbols[i : i + 100]), "feed": feed.value},
                ratelimiter_keys=["alpaca:/v2/stocks/snapshots"],
                data_api=True,
            )
            out.update(self._snapshots_decoder.decode(raw))
        return out

    async def query_option_chain(
        self,
        underlying: str,
        kind: str | None = None,
        expiration_gte: str | None = None,
        expiration_lte: str | None = None,
        strike_gte: float | None = None,
        strike_lte: float | None = None,
        feed: AlpacaDataFeed = AlpacaDataFeed.INDICATIVE,
    ) -> dict[str, AlpacaSnapshot]:
        """
        Return option snapshots (latest quote/trade, greeks, implied volatility) by OCC symbol.
        """
        out: dict[str, AlpacaSnapshot] = {}
        token: str | None = None
        for _ in range(50):
            raw = await self.client.send_request(
                HttpMethod.GET,
                f"/v1beta1/options/snapshots/{underlying}",
                params={
                    "type": kind,
                    "expiration_date_gte": expiration_gte,
                    "expiration_date_lte": expiration_lte,
                    "strike_price_gte": strike_gte,
                    "strike_price_lte": strike_lte,
                    "feed": feed.value,
                    "limit": 1000,
                    "page_token": token,
                },
                ratelimiter_keys=["alpaca:/v1beta1/options/snapshots"],
                data_api=True,
            )
            page = self._option_snapshots_decoder.decode(raw)
            out.update(page.snapshots or {})
            token = page.next_page_token
            if not token:
                break
        return out

    async def query_option_latest_quotes(
        self,
        symbols: list[str],
        feed: AlpacaDataFeed = AlpacaDataFeed.INDICATIVE,
    ) -> dict[str, AlpacaQuote]:
        out: dict[str, AlpacaQuote] = {}
        for i in range(0, len(symbols), 100):
            raw = await self.client.send_request(
                HttpMethod.GET,
                "/v1beta1/options/quotes/latest",
                params={"symbols": ",".join(symbols[i : i + 100]), "feed": feed.value},
                ratelimiter_keys=["alpaca:/v1beta1/options/quotes/latest"],
                data_api=True,
            )
            out.update(self._latest_quotes_decoder.decode(raw).quotes or {})
        return out
