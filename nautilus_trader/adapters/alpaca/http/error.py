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

class AlpacaError(Exception):
    """
    The base class for all Alpaca specific errors.
    """

    def __init__(self, status: int, message, headers) -> None:
        super().__init__(f"Alpaca {status}: {message}")
        self.status = status
        self.message = message
        self.headers = headers

    @property
    def code(self) -> int | None:
        return self.message.get("code") if isinstance(self.message, dict) else None

    @property
    def text(self) -> str:
        if isinstance(self.message, dict):
            return str(self.message.get("message") or self.message)
        return str(self.message)


class AlpacaServerError(AlpacaError):
    """
    Represents an Alpaca specific 500 series HTTP error.
    """


class AlpacaClientError(AlpacaError):
    """
    Represents an Alpaca specific 400 series HTTP error.
    """


def should_retry(error: BaseException) -> bool:
    """
    Return whether a failed request is worth retrying (rate limits, server errors, transport).
    """
    if isinstance(error, AlpacaServerError):
        return True
    if isinstance(error, AlpacaClientError):
        return error.status == 429
    return not isinstance(error, AlpacaError)
