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

from nautilus_trader.adapters.alpaca.common.enums import AlpacaEnvironment
from nautilus_trader.adapters.env import get_env_key


def get_api_key(environment: AlpacaEnvironment) -> str:
    """
    Get the Alpaca API key id from environment variables.

    ``ALPACA_PAPER_API_KEY`` for the paper account, ``ALPACA_API_KEY`` for live.
    """
    if environment == AlpacaEnvironment.PAPER:
        return get_env_key("ALPACA_PAPER_API_KEY")
    return get_env_key("ALPACA_API_KEY")


def get_api_secret(environment: AlpacaEnvironment) -> str:
    """
    Get the Alpaca API secret key from environment variables.

    ``ALPACA_PAPER_API_SECRET`` for the paper account, ``ALPACA_API_SECRET`` for live.
    """
    if environment == AlpacaEnvironment.PAPER:
        return get_env_key("ALPACA_PAPER_API_SECRET")
    return get_env_key("ALPACA_API_SECRET")
