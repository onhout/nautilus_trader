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

from nautilus_trader.adapters.alpaca.http.client import AlpacaHttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpMethod


class AlpacaHttpEndpoint:
    """
    Base functionality of endpoints connecting to the Alpaca REST APIs.

    Warnings
    --------
    This class should not be used directly, but through a concrete subclass.

    """

    def __init__(self, client: AlpacaHttpClient, url_path: str, data_api: bool = False) -> None:
        self.client = client
        self.url_path = url_path
        self.data_api = data_api
        self._ratelimiter_key = f"alpaca:{url_path}"

    async def _method(
        self,
        method_type: HttpMethod,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        url_path: str | None = None,
    ) -> bytes:
        return await self.client.send_request(
            http_method=method_type,
            url_path=url_path or self.url_path,
            params=params,
            body=body,
            ratelimiter_keys=[self._ratelimiter_key],
            data_api=self.data_api,
        )
