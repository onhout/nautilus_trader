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

from nautilus_trader.adapters.alpaca.common.enums import AlpacaDataFeed
from nautilus_trader.adapters.alpaca.common.enums import AlpacaEnvironment
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.config import PositiveInt


class AlpacaInstrumentProviderConfig(InstrumentProviderConfig, frozen=True):
    """
    Configuration for ``AlpacaInstrumentProvider`` instances.

    Alpaca has thousands of stocks and millions of option contracts: instruments are loaded by
    id (``load_ids``) or on demand when a strategy subscribes, never with ``load_all``.
    """


class AlpacaDataClientConfig(LiveDataClientConfig, frozen=True):
    """
    Configuration for ``AlpacaDataClient`` instances.

    Parameters
    ----------
    api_key : str, optional
        The Alpaca API key id. If ``None`` then sourced from the environment
        (`ALPACA_PAPER_API_KEY` for paper, `ALPACA_API_KEY` for live).
    api_secret : str, optional
        The Alpaca API secret key. If ``None`` then sourced from the environment
        (`ALPACA_PAPER_API_SECRET` / `ALPACA_API_SECRET`).
    environment : AlpacaEnvironment, default PAPER
        Which account's keys to use (market data is the same for both).
    stock_feed : AlpacaDataFeed, default IEX
        Stock quotes feed: IEX (free) or SIP (Algo Trader Plus).
    option_feed : AlpacaDataFeed, default INDICATIVE
        Option quotes feed: INDICATIVE (free) or OPRA (Algo Trader Plus).
    proxy_url : str, optional
        The proxy URL for HTTP and WebSocket requests.

    """

    api_key: str | None = None
    api_secret: str | None = None
    environment: AlpacaEnvironment = AlpacaEnvironment.PAPER
    stock_feed: AlpacaDataFeed = AlpacaDataFeed.IEX
    option_feed: AlpacaDataFeed = AlpacaDataFeed.INDICATIVE
    proxy_url: str | None = None


class AlpacaExecClientConfig(LiveExecClientConfig, frozen=True):
    """
    Configuration for ``AlpacaExecutionClient`` instances.

    Parameters
    ----------
    api_key : str, optional
        The Alpaca API key id. If ``None`` then sourced from the environment.
    api_secret : str, optional
        The Alpaca API secret key. If ``None`` then sourced from the environment.
    environment : AlpacaEnvironment, default PAPER
        The account: PAPER (simulated money) or LIVE (real money).
    live_trading : bool, default False
        Must be True for orders to be sent to the LIVE account.
    max_retries : PositiveInt, default 3
        The maximum number of times a retryable request is retried.
    proxy_url : str, optional
        The proxy URL for HTTP and WebSocket requests.

    """

    api_key: str | None = None
    api_secret: str | None = None
    environment: AlpacaEnvironment = AlpacaEnvironment.PAPER
    live_trading: bool = False
    max_retries: PositiveInt = 3
    proxy_url: str | None = None
