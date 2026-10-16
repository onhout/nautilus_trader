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

from nautilus_trader.adapters.alpaca.common.schemas.account import AlpacaAccount
from nautilus_trader.adapters.alpaca.common.schemas.account import AlpacaClock
from nautilus_trader.adapters.alpaca.common.schemas.account import AlpacaFillActivity
from nautilus_trader.adapters.alpaca.common.schemas.account import AlpacaOrder
from nautilus_trader.adapters.alpaca.common.schemas.account import AlpacaPosition
from nautilus_trader.adapters.alpaca.http.client import AlpacaHttpClient
from nautilus_trader.adapters.alpaca.http.endpoint import AlpacaHttpEndpoint
from nautilus_trader.adapters.alpaca.http.error import AlpacaClientError
from nautilus_trader.core.nautilus_pyo3 import HttpMethod


class AlpacaAccountHttp(AlpacaHttpEndpoint):
    """
    Endpoint for the account.

    `GET /v2/account`

    References
    ----------
    https://docs.alpaca.markets/reference/getaccount-1

    """

    def __init__(self, client: AlpacaHttpClient) -> None:
        super().__init__(client, "/v2/account")
        self._decoder = msgspec.json.Decoder(AlpacaAccount)

    async def get(self) -> AlpacaAccount:
        return self._decoder.decode(await self._method(HttpMethod.GET))


class AlpacaClockHttp(AlpacaHttpEndpoint):
    """
    Endpoint for the market clock.

    `GET /v2/clock`

    """

    def __init__(self, client: AlpacaHttpClient) -> None:
        super().__init__(client, "/v2/clock")
        self._decoder = msgspec.json.Decoder(AlpacaClock)

    async def get(self) -> AlpacaClock:
        return self._decoder.decode(await self._method(HttpMethod.GET))


class AlpacaOrdersHttp(AlpacaHttpEndpoint):
    """
    Endpoint for orders.

    `POST /v2/orders`
    `GET /v2/orders`
    `GET /v2/orders/{order_id}`
    `GET /v2/orders:by_client_order_id`
    `PATCH /v2/orders/{order_id}`
    `DELETE /v2/orders/{order_id}`

    References
    ----------
    https://docs.alpaca.markets/reference/postorder

    """

    def __init__(self, client: AlpacaHttpClient) -> None:
        super().__init__(client, "/v2/orders")
        self._decoder = msgspec.json.Decoder(AlpacaOrder)
        self._list_decoder = msgspec.json.Decoder(list[AlpacaOrder])

    async def submit(self, body: dict[str, Any]) -> AlpacaOrder:
        return self._decoder.decode(await self._method(HttpMethod.POST, body=body))

    async def replace(self, order_id: str, body: dict[str, Any]) -> AlpacaOrder:
        raw = await self._method(HttpMethod.PATCH, body=body, url_path=f"{self.url_path}/{order_id}")
        return self._decoder.decode(raw)

    async def cancel(self, order_id: str) -> None:
        await self._method(HttpMethod.DELETE, url_path=f"{self.url_path}/{order_id}")

    async def get(self, order_id: str) -> AlpacaOrder | None:
        try:
            return self._decoder.decode(await self._method(HttpMethod.GET, url_path=f"{self.url_path}/{order_id}"))
        except AlpacaClientError as e:
            if e.status == 404:
                return None
            raise

    async def get_by_client_order_id(self, client_order_id: str) -> AlpacaOrder | None:
        try:
            raw = await self._method(
                HttpMethod.GET,
                params={"client_order_id": client_order_id},
                url_path="/v2/orders:by_client_order_id",
            )
            return self._decoder.decode(raw)
        except AlpacaClientError as e:
            if e.status in (404, 422):
                return None
            raise

    async def list(
        self,
        status: str = "all",
        after: str | None = None,
        until: str | None = None,
        symbols: list[str] | None = None,
        limit: int = 500,
    ) -> list[AlpacaOrder]:
        """
        Return orders oldest first, paging forward from `after`.
        """
        orders: list[AlpacaOrder] = []
        seen: set[str] = set()
        cursor = after
        for _ in range(50):
            page = self._list_decoder.decode(
                await self._method(
                    HttpMethod.GET,
                    params={
                        "status": status,
                        "after": cursor,
                        "until": until,
                        "limit": limit,
                        "direction": "asc",
                        "nested": "false",
                        "symbols": ",".join(symbols) if symbols else None,
                    },
                ),
            )
            fresh = [o for o in page if o.id not in seen]
            orders.extend(fresh)
            seen.update(o.id for o in fresh)
            if len(page) < limit or not fresh:
                break
            cursor = page[-1].submitted_at or page[-1].created_at
        return orders


class AlpacaPositionsHttp(AlpacaHttpEndpoint):
    """
    Endpoint for open positions.

    `GET /v2/positions`

    """

    def __init__(self, client: AlpacaHttpClient) -> None:
        super().__init__(client, "/v2/positions")
        self._decoder = msgspec.json.Decoder(list[AlpacaPosition])

    async def get(self) -> list[AlpacaPosition]:
        return self._decoder.decode(await self._method(HttpMethod.GET))


class AlpacaFillActivitiesHttp(AlpacaHttpEndpoint):
    """
    Endpoint for fill activities.

    `GET /v2/account/activities/FILL`

    """

    def __init__(self, client: AlpacaHttpClient) -> None:
        super().__init__(client, "/v2/account/activities/FILL")
        self._decoder = msgspec.json.Decoder(list[AlpacaFillActivity])

    async def get(self, after: str | None = None, page_size: int = 100) -> list[AlpacaFillActivity]:
        """
        Return fills oldest first after an ISO time (all pages).
        """
        out: list[AlpacaFillActivity] = []
        token: str | None = None
        for _ in range(50):
            page = self._decoder.decode(
                await self._method(
                    HttpMethod.GET,
                    params={"after": after, "direction": "asc", "page_size": page_size, "page_token": token},
                ),
            )
            out.extend(page)
            if len(page) < page_size:
                break
            token = page[-1].id
        return out


class AlpacaAccountHttpAPI:
    """
    Provides access to the Alpaca account, orders, positions and fills.
    """

    def __init__(self, client: AlpacaHttpClient) -> None:
        self.client = client
        self._endpoint_account = AlpacaAccountHttp(client)
        self._endpoint_clock = AlpacaClockHttp(client)
        self.orders = AlpacaOrdersHttp(client)
        self._endpoint_positions = AlpacaPositionsHttp(client)
        self._endpoint_fills = AlpacaFillActivitiesHttp(client)

    async def query_account(self) -> AlpacaAccount:
        return await self._endpoint_account.get()

    async def query_clock(self) -> AlpacaClock:
        return await self._endpoint_clock.get()

    async def query_positions(self) -> list[AlpacaPosition]:
        return await self._endpoint_positions.get()

    async def query_fills(self, after: str | None = None) -> list[AlpacaFillActivity]:
        return await self._endpoint_fills.get(after=after)
