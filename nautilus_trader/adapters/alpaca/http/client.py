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

import urllib.parse
from typing import Any

import msgspec

import nautilus_trader
from nautilus_trader.adapters.alpaca.common.constants import ALPACA_GLOBAL_RATE_KEY
from nautilus_trader.adapters.alpaca.http.error import AlpacaClientError
from nautilus_trader.adapters.alpaca.http.error import AlpacaServerError
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.nautilus_pyo3 import HttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpMethod
from nautilus_trader.core.nautilus_pyo3 import HttpResponse
from nautilus_trader.core.nautilus_pyo3 import Quota
from nautilus_trader.core.nautilus_pyo3 import mask_api_key


class AlpacaHttpClient:
    """
    Provides an Alpaca asynchronous HTTP client (Trading API and Market Data API share keys).

    Parameters
    ----------
    clock : LiveClock
        The clock for the client.
    api_key : str
        The Alpaca API key id.
    api_secret : str
        The Alpaca API secret key.
    base_url : str
        The Trading API base URL (paper or live).
    data_url : str
        The Market Data API base URL.
    ratelimiter_quotas : list[tuple[str, Quota]], optional
        The keyed rate limiter quotas for the client.
    ratelimiter_default_quota : Quota, optional
        The default rate limiter quota for the client.
    proxy_url : str, optional
        The proxy URL for HTTP requests.

    """

    def __init__(
        self,
        clock: LiveClock,
        api_key: str,
        api_secret: str,
        base_url: str,
        data_url: str,
        ratelimiter_quotas: list[tuple[str, Quota]] | None = None,
        ratelimiter_default_quota: Quota | None = None,
        proxy_url: str | None = None,
    ) -> None:
        self._clock = clock
        self._log = Logger(type(self).__name__)
        self._key = api_key
        self._secret = api_secret
        self._base_url = base_url
        self._data_url = data_url
        self._headers: dict[str, Any] = {
            "Content-Type": "application/json",
            "User-Agent": nautilus_trader.NAUTILUS_USER_AGENT,
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret,
        }
        self._client = HttpClient(
            keyed_quotas=ratelimiter_quotas or [],
            default_quota=ratelimiter_default_quota,
            proxy_url=proxy_url,
        )

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def data_url(self) -> str:
        return self._data_url

    @property
    def api_key(self) -> str:
        return self._key

    @property
    def api_secret(self) -> str:
        return self._secret

    @property
    def api_key_masked(self) -> str:
        return mask_api_key(self._key)

    async def send_request(
        self,
        http_method: HttpMethod,
        url_path: str,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        ratelimiter_keys: list[str] | None = None,
        data_api: bool = False,
    ) -> bytes:
        """
        Send a request and return the raw response body (raises ``AlpacaError`` on 4xx/5xx).
        """
        query = {k: v for k, v in (params or {}).items() if v is not None}
        if query:
            url_path += "?" + urllib.parse.urlencode(query)
        url = (self._data_url if data_api else self._base_url) + url_path
        self._log.debug(f"{http_method} {url_path} {body or ''}", LogColor.MAGENTA)

        response: HttpResponse = await self._client.request(
            http_method,
            url=url,
            headers=self._headers,
            body=msgspec.json.encode(body) if body is not None else None,
            keys=(ratelimiter_keys or []) + [ALPACA_GLOBAL_RATE_KEY],
        )

        if response.status >= 400:
            try:
                message = msgspec.json.decode(response.body) if response.body else None
            except msgspec.DecodeError:
                message = response.body.decode(errors="replace")
            error_cls = AlpacaServerError if response.status >= 500 else AlpacaClientError
            raise error_cls(status=response.status, message=message, headers=response.headers)

        return response.body
