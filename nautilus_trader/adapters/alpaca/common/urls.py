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


def get_http_base_url(environment: AlpacaEnvironment) -> str:
    """Trading API (orders, account, positions, assets, option contracts)."""
    if environment == AlpacaEnvironment.LIVE:
        return "https://api.alpaca.markets"
    return "https://paper-api.alpaca.markets"


def get_data_base_url() -> str:
    """Market Data API (bars, quotes, snapshots); the same for paper and live."""
    return "https://data.alpaca.markets"


def get_ws_trading_url(environment: AlpacaEnvironment) -> str:
    """The ``trade_updates`` stream (order events) of the account."""
    if environment == AlpacaEnvironment.LIVE:
        return "wss://api.alpaca.markets/stream"
    return "wss://paper-api.alpaca.markets/stream"


def get_ws_data_url(feed: AlpacaDataFeed) -> str:
    """Market data stream: stocks (``iex``/``sip``, JSON) or options (``indicative``/``opra``, MsgPack)."""
    if feed in (AlpacaDataFeed.INDICATIVE, AlpacaDataFeed.OPRA):
        return f"wss://stream.data.alpaca.markets/v1beta1/{feed.value}"
    return f"wss://stream.data.alpaca.markets/v2/{feed.value}"
