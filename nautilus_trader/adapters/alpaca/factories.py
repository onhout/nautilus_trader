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
from functools import lru_cache

from nautilus_trader.adapters.alpaca.common.constants import ALPACA_GLOBAL_RATE_KEY
from nautilus_trader.adapters.alpaca.common.constants import ALPACA_REQUESTS_PER_MINUTE
from nautilus_trader.adapters.alpaca.common.credentials import get_api_key
from nautilus_trader.adapters.alpaca.common.credentials import get_api_secret
from nautilus_trader.adapters.alpaca.common.enums import AlpacaEnvironment
from nautilus_trader.adapters.alpaca.common.urls import get_data_base_url
from nautilus_trader.adapters.alpaca.common.urls import get_http_base_url
from nautilus_trader.adapters.alpaca.config import AlpacaDataClientConfig
from nautilus_trader.adapters.alpaca.config import AlpacaExecClientConfig
from nautilus_trader.adapters.alpaca.data import AlpacaDataClient
from nautilus_trader.adapters.alpaca.execution import AlpacaExecutionClient
from nautilus_trader.adapters.alpaca.http.client import AlpacaHttpClient
from nautilus_trader.adapters.alpaca.providers import AlpacaInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.nautilus_pyo3 import Quota
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory


@lru_cache(1)
def get_cached_alpaca_http_client(
    clock: LiveClock,
    environment: AlpacaEnvironment = AlpacaEnvironment.PAPER,
    api_key: str | None = None,
    api_secret: str | None = None,
    proxy_url: str | None = None,
) -> AlpacaHttpClient:
    """
    Cache and return an Alpaca HTTP client for the given environment and keys.

    If a cached client with matching parameters already exists, the cached client will be returned.

    Parameters
    ----------
    clock : LiveClock
        The clock for the client.
    environment : AlpacaEnvironment, default PAPER
        The Alpaca account.
    api_key : str, optional
        The API key id (sourced from the environment if ``None``).
    api_secret : str, optional
        The API secret key (sourced from the environment if ``None``).
    proxy_url : str, optional
        The proxy URL for HTTP requests.

    Returns
    -------
    AlpacaHttpClient

    """
    quota = Quota.rate_per_minute(ALPACA_REQUESTS_PER_MINUTE)
    return AlpacaHttpClient(
        clock=clock,
        api_key=api_key or get_api_key(environment),
        api_secret=api_secret or get_api_secret(environment),
        base_url=get_http_base_url(environment),
        data_url=get_data_base_url(),
        ratelimiter_quotas=[(ALPACA_GLOBAL_RATE_KEY, quota)],
        ratelimiter_default_quota=quota,
        proxy_url=proxy_url,
    )


@lru_cache(1)
def get_cached_alpaca_instrument_provider(
    client: AlpacaHttpClient,
    clock: LiveClock,
    config: InstrumentProviderConfig,
) -> AlpacaInstrumentProvider:
    """
    Cache and return an Alpaca instrument provider.

    If a cached provider already exists, then that provider will be returned.

    Parameters
    ----------
    client : AlpacaHttpClient
        The client for the instrument provider.
    clock : LiveClock
        The clock for the instrument provider.
    config : InstrumentProviderConfig
        The configuration for the instrument provider.

    Returns
    -------
    AlpacaInstrumentProvider

    """
    return AlpacaInstrumentProvider(client=client, clock=clock, config=config)


class AlpacaLiveDataClientFactory(LiveDataClientFactory):
    """
    Provides an Alpaca live data client factory.
    """

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: AlpacaDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> AlpacaDataClient:
        """
        Create a new Alpaca data client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The custom client ID.
        config : AlpacaDataClientConfig
            The client configuration.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Returns
        -------
        AlpacaDataClient

        """
        client = get_cached_alpaca_http_client(
            clock=clock,
            environment=config.environment,
            api_key=config.api_key,
            api_secret=config.api_secret,
            proxy_url=config.proxy_url,
        )
        provider = get_cached_alpaca_instrument_provider(
            client=client,
            clock=clock,
            config=config.instrument_provider,
        )
        return AlpacaDataClient(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )


class AlpacaLiveExecClientFactory(LiveExecClientFactory):
    """
    Provides an Alpaca live execution client factory.
    """

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: AlpacaExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> AlpacaExecutionClient:
        """
        Create a new Alpaca execution client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The custom client ID.
        config : AlpacaExecClientConfig
            The configuration for the client.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Returns
        -------
        AlpacaExecutionClient

        """
        client = get_cached_alpaca_http_client(
            clock=clock,
            environment=config.environment,
            api_key=config.api_key,
            api_secret=config.api_secret,
            proxy_url=config.proxy_url,
        )
        provider = get_cached_alpaca_instrument_provider(
            client=client,
            clock=clock,
            config=config.instrument_provider,
        )
        return AlpacaExecutionClient(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )
